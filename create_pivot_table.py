#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections

import psycopg2

from generate_pivot_query import _generate_pivot_query
from create_table_from_select import create_table_from_select


def _create_pivot_table(cursor, *args, **kwargs):
    schema_name = kwargs['source_schema']
    table_name = kwargs['table_name']
    pivot_query = _generate_pivot_query(cursor, *args, **kwargs)
    table_keys = {} # TODO: load these from a file if available
    create_table_from_select(
        cursor,
        pivot_query,
        schema_name,
        table_name,
        **table_keys
    )



def create_pivot_table(connection_dict, *args, **kwargs):
    with psycopg2.connect(**connection_dict) as connection:
        cursor = connection.cursor()
        _create_pivot_table(cursor, *args, **kwargs)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create a pivot table')
    parser.add_argument('source_schema')
    parser.add_argument('source_table')
    parser.add_argument('table_name')
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

    query = create_pivot_table(
        connection_dict,
        source_schema=args.source_schema,
        source_table=args.source_table,
        table_name=args.table_name,
        base_columns=args.base_columns,
        pivot_columns=args.pivot_columns,
        exclude_columns=args.exclude_columns,
        exclude_aggregates=args.exclude_aggregates,
        aggfunction_mappings=aggfunctions
    )

    print('{}'.format(query))
