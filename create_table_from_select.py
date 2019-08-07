#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals

import os


def read_file(filename):
    with open(filename, 'r') as infile:
        return infile.read()


def get_snowflake_connection_dict_from_airflow(conn_id):
    """Return a dictionary usable with `snowflake.connector.connect`.
    """
    from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
    conn = SnowflakeHook.get_connection(conn_id)
    account = conn.extra_dejson.get('account', None)
    warehouse = conn.extra_dejson.get('warehouse', None)
    database = conn.extra_dejson.get('database', None)
    return {
        'user': conn.login,
        'password': conn.password or '',
        'schema': conn.schema or '',
        'database': database or '',
        'account': account or '',
        'warehouse': warehouse or ''
    }


def get_redshift_connection_dict_from_airflow(conn_id):
    """Return a dictionary usable with `psycopg2.connect`.
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


def _run(connection, schema, table, sql_directory=None):
    sql_directory = sql_directory or os.path.abspath('sql')
    sql_filename = os.path.join(sql_directory, '{}.sql'.format(table))
    query = read_file(sql_filename)
    cursor = connection.cursor()
    create_table_from_select(cursor, query, schema, table)
    connection.commit()


def run(dbtype, connection_dict, schema, table, sql_directory=None):
    if dbtype == 'snowflake':
        import snowflake.connector
        connector = snowflake.connector.connect
    else:
        import psycopg2
        connector = psycopg2.connect
    with connector(**connection_dict) as connection:
        _run(connection, schema, table, sql_directory=sql_directory)


def run_from_airflow(*_args, **kwargs):
    schema = kwargs['schema_name']
    table = kwargs['table_name']
    sql_directory = kwargs.get('sql_directory')
    if 'snowflake_conn_id' in kwargs:
        conn_id = kwargs['snowflake_conn_id']
        connection_dict = get_snowflake_connection_dict_from_airflow(conn_id)
        run('snowflake', connection_dict, schema, table,
            sql_directory=sql_directory)
    else:
        conn_id = kwargs['postgres_conn_id']
        connection_dict = get_redshift_connection_dict_from_airflow(conn_id)
        run('redshift', connection_dict, schema, table,
            sql_directory=sql_directory)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Create a database table from a SELECT statement'
    )

    parser.add_argument('schema',
                        help='The schema in which to create the table')
    parser.add_argument('table', help='The name of the table to create')

    parser.add_argument('--dbtype')
    parser.add_argument('--host')
    parser.add_argument('--dbname')
    parser.add_argument('--database')
    parser.add_argument('--account')
    parser.add_argument('--warehouse')
    parser.add_argument('--port')
    parser.add_argument('--user')
    parser.add_argument('--password')
    parser.add_argument('--sql-directory')

    args = parser.parse_args()

    if args.dbtype == 'snowflake':
        connection_dict = {
            'user': args.user,
            'password': args.password or '',
            'schema': args.schema or '',
            'database': args.database or '',
            'account': args.account or '',
            'warehouse': args.warehouse or ''
        }
    else:
        connection_dict = {
            'host': args.host,
            'dbname': args.schema,
            'port': args.port,
            'user': args.login,
            'password': args.password
        }

    run(args.dbtype, connection_dict, args.schema, args.table,
        sql_directory=args.sql_directory)
