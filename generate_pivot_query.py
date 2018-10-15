#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals

import os
import io
import re
import sys
import itertools
import collections

import psycopg2

from create_table_from_select import get_connection_dict_from_airflow


# Maps to a tuple of (agg_function, default). We currently don't handle
# averages, because weighting would get more complicated.
#
# We use an `OrderedDict` because the keys are prefix-matched and we want to
# ensure they're checked in the order specified
aggfunction_mappings = collections.OrderedDict([
    ('first',       ('MIN', 'NULL')),
    ('min',         ('MIN', 'NULL')),
    ('least',       ('MIN', 'NULL')),
    ('most_recent', ('MAX', 'NULL')),
    ('max',         ('MAX', 'NULL')),
    ('most',        ('MAX', 'NULL')),
    ('last',        ('MAX', 'NULL')),
    ('longest',     ('MAX', 'NULL')),
    ('latest',      ('MAX', 'NULL')),
    ('num',         ('SUM', 0)),
    ('sum',         ('SUM', 0)),
    ('total',       ('SUM', 0)),
    ('count',       ('SUM', 0)),
    ('list',        ('LISTAGG', 'NULL')),
    ('and',         ('BOOL_AND', 'true')),
    ('bool_and',    ('BOOL_AND', 'true')),
    ('or',          ('BOOL_OR', 'NULL')),
    ('bool_or',     ('BOOL_OR', 'NULL'))
])

table_columns_query = """
SELECT
    "column"
FROM
    pg_table_def
WHERE
    "tablename" = '{table_name}'
    AND "schemaname" = '{schema_name}'
"""

distinct_values_query = """
SELECT
    DISTINCT {columns}
FROM
    "{schema}"."{table}"
"""


class PrefixNotFoundError(KeyError):
    pass


def get_aggregate_mapping(name):
    if name in aggfunction_mappings:
        return aggfunction_mappings[name]
    for prefix, value in aggfunction_mappings.items():
        prefix_ = '{}_'.format(prefix)
        if name.startswith(prefix_):
            return value
    raise PrefixNotFoundError(name)


def get_columns(cursor, schema, table):
    """Return a list of columns in the specified table."""
    query = table_columns_query.format(
        table_name=table,
        schema_name=schema
    )

    cursor.execute(query)
    rows = list(cursor)

    if not rows:
        raise Exception('Table {}.{} empty or not found'.format(
            schema, table
        ))

    return [row[0] for row in rows]


def get_distinct_values(cursor, schema, table, columns):
    columns = ', '.join('"{}"'.format(col) for col in columns)
    query = distinct_values_query.format(
        schema=schema,
        table=table,
        columns=columns
    )

    cursor.execute(query)
    return list(cursor)


def _generate_pivot_query(cursor, source_schema=None, source_table=None,
                          base_columns=None, pivot_columns=None,
                          exclude_columns=None, aggfunction_mappings=None,
                          exclude_aggregates=None,
                          **kwargs):
    if not source_schema:
        raise Exception('No source_schema provided')
    if not source_table:
        raise Exception('No source_table provided')
    if not base_columns:
        raise Exception('No base_columns provided')
    if not pivot_columns:
        raise Exception('No pivot_columns provided')

    out = io.StringIO()

    exclude_columns = exclude_columns or []
    exclude_aggregates = exclude_aggregates or []

    table_columns = get_columns(cursor, source_schema, source_table)

    inhibit_pivot = set(itertools.chain(base_columns, pivot_columns,
                                        exclude_columns))
    columns_to_pivot = [c for c in table_columns if c not in inhibit_pivot]

    # Get distinct values for the base columns
    distinct_values = get_distinct_values(cursor, source_schema, source_table,
                                          pivot_columns)

    out.write('SELECT\n')
    for i, col in enumerate(base_columns):
        out.write('    ')
        if i > 0:
            out.write(', ')
        out.write(col + '\n')
    for col in columns_to_pivot:
        pivoted = pivot_column(
            col, pivot_columns, distinct_values,
            exclude_aggregates=exclude_aggregates,
            aggfunction_mappings=aggfunction_mappings
        )
        for col_ in pivoted:
            out.write('    , {}\n'.format(col_))
    out.write('FROM\n    {}.{}\n'.format(source_schema, source_table))
    out.write('GROUP BY\n')
    for i, col in enumerate(base_columns):
        out.write('    ')
        if i > 0:
            out.write(', ')
        out.write(col + '\n')

    return out.getvalue()


def generate_pivot_query(connection_dict, **kwargs):
    with psycopg2.connect(**connection_dict) as connection:
        cursor = connection.cursor()
        return _generate_pivot_query(cursor, **kwargs)


def pivot_column(column, pivot_columns, distinct_values,
                 aggfunction_mappings=None, exclude_aggregates=None,
                 **kwargs):
    aggfunction_mappings = aggfunction_mappings or {}
    exclude_aggregates = exclude_aggregates or []

    if column in aggfunction_mappings:
        # If the aggregate and default were supplied explicitly, use that.
        agginfo = aggfunction_mappings[column]
        if isinstance(aggfunction_mappings, str):
            # If the value is a tuple, it's the aggregate function and "else
            # value" we can use directly. If it's a string, it's a shorthand we
            # use to perform a second lookup in aggfunction_mappings
            agginfo = aggfunction_mappings[agginfo]
        sql_agg_function, default = get_aggregate_mapping(agginfo)
    else:
        # Otherwise, try to guess reasonable ones based on the prefix of the
        # column name
        try:
            sql_agg_function, default = get_aggregate_mapping(column)
        except PrefixNotFoundError:
            raise Exception('column: {}, aggfunction_mappings: {}'.format(
                column, aggfunction_mappings
            ))

    # Make subcondition strings and names
    tests = []
    names = []
    for row in distinct_values:
        test = []
        name = []
        for col, val in zip(pivot_columns, row):
            if val is None:
                test.append('{} is NULL'.format(col))
                name.append('{}_null'.format(col))
            else:
                name_suffix = re.sub('[^0-9A-Za-z_]+', '_', str(val))
                if isinstance(val, (int, float)):
                    val_str = str(val)
                else:
                    val_str = "'{}'".format(val)
                test.append('{} = {}'.format(col, val_str))
                name.append('{}_{}'.format(col, name_suffix))
        tests.append(' AND '.join(test))
        names.append('_'.join(name))

    result = []
    for test, suffix in zip(tests, names):
        result.append('{sql_agg_function}(CASE WHEN {test} THEN {column} ELSE {default} END) AS {column}_{suffix}'.format(
            sql_agg_function=sql_agg_function,
            test=test,
            column=column,
            default=default,
            suffix=suffix
        ))
    if column not in exclude_aggregates:
        # Add the overall aggregate case (not specific to any any pivoting
        # columns)
        result.append('{sql_agg_function}({column}) AS {column}'.format(
            sql_agg_function=sql_agg_function,
            column=column
        ))

    return result


def generate_pivot_query_to_file(connection_dict, destination, **kwargs):
    query = generate_pivot_query(connection_dict, **kwargs)
    with open(destination, 'w') as outfile:
        outfile.write(query)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Generate a pivot query')
    parser.add_argument('source_schema')
    parser.add_argument('source_table')
    parser.add_argument('--airflow-postgres-conn-id')
    parser.add_argument('--host')
    parser.add_argument('--dbname')
    parser.add_argument('--port')
    parser.add_argument('--user')
    parser.add_argument('--password')
    parser.add_argument('--sql-directory')
    parser.add_argument('--base-columns', nargs='+')
    parser.add_argument('--pivot-columns', nargs='+')
    parser.add_argument('--exclude-columns', nargs='*')
    parser.add_argument('--exclude-aggregates', nargs='*')
    parser.add_argument('--aggfunction-mappings', nargs='*')

    args = parser.parse_args()

    if args.airflow_postgres_conn_id:
        connection_dict = get_connection_dict_from_airflow(
            args.airflow_postgres_conn_id
        )
    else:
        connection_dict = {}

    for key in 'host', 'dbname', 'port', 'user', 'password':
        connection_dict[key] = getattr(args, key)

    # Convert the aggregate function mappings from a list of key=val items into
    # a dictionary
    aggfunctions = [item.split('=') for item in args.aggfunction_mappings]
    aggfunctions = collections.OrderedDict(aggfunctions)

    query = generate_pivot_query(
        connection_dict,
        source_schema=args.source_schema,
        source_table=args.source_table,
        base_columns=args.base_columns,
        pivot_columns=args.pivot_columns,
        exclude_columns=args.exclude_columns,
        exclude_aggregates=args.exclude_aggregates,
        aggfunction_mappings=aggfunctions
    )

    print('{}'.format(query))
