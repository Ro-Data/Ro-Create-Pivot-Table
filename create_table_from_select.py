#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals

import os
import sys
import io

import psycopg2


def read_file(filename):
    with open(filename, 'r') as infile:
        return infile.read()


def read_yaml_file(filename):
    import yaml
    with open(filename, 'r') as infile:
        return yaml.load(infile)


def get_connection_dict_from_airflow(conn_id):
    """Look up `conn_id` and return a dictionary usable with `psycopg2.connect`.
    """
    from airflow.hooks.postgres_hook import PostgresHook
    conn = PostgresHook.get_connection(conn_id)
    return {
        'host': conn.host,
        'dbname': conn.schema,
        'port': conn.port,
        'user': conn.login,
        'password': conn.password
    }


def execute(cursor, query, *args, **kwargs):
    try:
        cursor.execute(query, *args, **kwargs)
    except psycopg2.Error:
        raise Exception('Bad query: {}'.format(query))


def get_table_info(cursor, schema, table):
    """Return a list of columns and types for a database table."""
    query = """
    SELECT "column", "type" FROM pg_table_def
    WHERE "schemaname" = %s AND "tablename" = %s
    """
    execute(cursor, query, (schema, table))
    return list(cursor)


def generate_table_definition(schema_and_table, column_info,
                              primary_key=None, foreign_keys=None,
                              diststyle=None, distkey=None, sortkey=None):
    """Return a CREATE TABLE statement as a string."""
    if not column_info:
        raise Exception('No columns specified for {}'.format(schema_and_table))

    out = io.StringIO()

    out.write('CREATE TABLE {} (\n'.format(schema_and_table))

    columns_count = len(column_info)
    for i, (column, type_) in enumerate(column_info):
        out.write('    "{}" {}'.format(column, type_))
        if (i < columns_count - 1) or primary_key or foreign_keys:
            out.write(',')
        out.write('\n')

    if primary_key:
        out.write('    PRIMARY KEY({})'.format(primary_key))
        if foreign_keys:
            out.write(',')
        out.write('\n')

    foreign_keys = foreign_keys or []
    foreign_keys_count = len(foreign_keys)
    for i, (key, reftable, refcolumn) in enumerate(foreign_keys):
        out.write('    FOREIGN KEY({}) REFERENCES {}({})'.format(
            key, reftable, refcolumn
        ))
        if i < foreign_keys_count - 1:
            out.write(',')
        out.write('\n')

    out.write(')\n')

    if diststyle:
        out.write('DISTSTYLE {}\n'.format(diststyle))

    if distkey:
        out.write('DISTKEY({})\n'.format(distkey))

    if sortkey:
        if isinstance(sortkey, str):
            out.write('SORTKEY({})\n'.format(sortkey))
        elif len(sortkey) == 1:
            out.write('SORTKEY({})\n'.format(sortkey[0]))
        else:
            out.write('COMPOUND SORTKEY({})\n'.format(', '.join(sortkey)))

    return out.getvalue()


def create_table_from_select(cursor, source_query, schema, table,
                             primary_key=None, foreign_keys=None,
                             diststyle=None, distkey=None, sortkey=None):
    """(Re-)create a table based on a SQL query.

    Requires that the source query has "FROM" capitalized in the final SELECT
    statement. An INTO is inserted directly before this last FROM when creating
    an intermediate temporary table.
    """
    final_table_full_name = '{}.{}'.format(schema, table)
    temp_table = 'temp_{}'.format(table)
    temp_table_full_name = '{}.{}'.format(schema, temp_table)

    drop_table_template = "DROP TABLE IF EXISTS {};"
    drop_temp_table = drop_table_template.format(temp_table_full_name)
    drop_final_table = drop_table_template.format(final_table_full_name)

    # Assume "INTO [table]" can be inserted immediately before the final FROM
    last_FROM_start_index = source_query.rfind('FROM')

    select_into_temp_table = (
        source_query[:last_FROM_start_index]
        + '\nINTO {}\n'.format(temp_table_full_name)
        + source_query[last_FROM_start_index:]
        + '\nLIMIT 10'
    )

    insert_into_final_table = (
        'INSERT INTO {}'.format(final_table_full_name)
        + '\n'
        + source_query
    )

    execute(cursor, drop_temp_table)
    execute(cursor, select_into_temp_table)

    table_info = get_table_info(cursor, schema, temp_table)
    create_final_table = generate_table_definition(
        final_table_full_name, table_info,
        primary_key=primary_key, foreign_keys=foreign_keys,
        diststyle=diststyle, distkey=distkey, sortkey=sortkey
    )

    execute(cursor, drop_final_table)
    execute(cursor, create_final_table)
    execute(cursor, insert_into_final_table)


def run(connection_dict, schema, table, sql_directory=None):
    sql_directory = sql_directory or os.path.abspath('sql')
    sql_filename = os.path.join(sql_directory, '{}.sql'.format(table))
    yaml_directory = os.path.join(sql_directory, 'table_designs')
    yaml_filename = os.path.join(yaml_directory, '{}.yml'.format(table))

    query = read_file(sql_filename)

    if os.path.exists(yaml_filename):
        table_keys = read_yaml_file(yaml_filename)
    else:
        table_keys = {}

    # TODO - Add enforcement of key names, value types
    with psycopg2.connect(**connection_dict) as connection:
        cursor = connection.cursor()
        create_table_from_select(cursor, query, schema, table, **table_keys)
        connection.commit()


def run_from_airflow(*_args, **kwargs):
    schema = kwargs['schema_name']
    table = kwargs['table_name']
    conn_id = kwargs['postgres_conn_id']
    sql_directory = kwargs.get('sql_directory')
    connection_dict = get_connection_dict_from_airflow(conn_id)
    run(connection_dict, schema, table, sql_directory=sql_directory)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Create a database table from a SELECT statement'
    )

    parser.add_argument('schema',
                        help='The schema in which to create the table')
    parser.add_argument('table', help='The name of the table to create')

    parser.add_argument('--airflow-postgres-conn-id')
    parser.add_argument('--host')
    parser.add_argument('--dbname')
    parser.add_argument('--port')
    parser.add_argument('--user')
    parser.add_argument('--password')
    parser.add_argument('--sql-directory')

    args = parser.parse_args()

    if args.airflow_postgres_conn_id:
        connection_dict = get_connection_dict_from_airflow(
            args.airflow_postgres_conn_id
        )
    else:
        connection_dict = {}

    for key in 'host', 'dbname', 'port', 'user', 'password':
        connection_dict[key] = getattr(args, key)

    run(connection_dict, args.schema, args.table,
        sql_directory=args.sql_directory)
