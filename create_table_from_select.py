#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals

import os

import psycopg2


def read_file(filename):
    with open(filename, 'r') as infile:
        return infile.read()


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


def create_table_from_select(cursor, query, schema, table):
    """(Re-)create a table based on a SQL query."""
    stmt = 'CREATE OR REPLACE TABLE {}.{} AS\n{};'.format(
        schema, table, query
    )
    cursor.execute(stmt)


def run(connection_dict, schema, table, sql_directory=None):
    sql_directory = sql_directory or os.path.abspath('sql')
    sql_filename = os.path.join(sql_directory, '{}.sql'.format(table))
    query = read_file(sql_filename)
    with psycopg2.connect(**connection_dict) as connection:
        cursor = connection.cursor()
        create_table_from_select(cursor, query, schema, table)
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
