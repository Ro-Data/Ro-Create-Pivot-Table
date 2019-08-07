#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections

import generate_pivot_query as generate
import create_table_from_select as create


def _create_pivot_table(cursor, *args, **kwargs):
    schema_name = kwargs['source_schema']
    table_name = kwargs['table_name']
    pivot_query = generate._generate_pivot_query(cursor, *args, **kwargs)
    create.create_table_from_select(
        cursor,
        pivot_query,
        schema_name,
        table_name
    )


def create_pivot_table(dbtype, connection_dict, *args, **kwargs):
    if dbtype == 'snowflake':
        import snowflake.connector
        connector = snowflake.connector.connect
    else:
        import psycopg2
        connector = psycopg2.connect
    with connector(**connection_dict) as connection:
        cursor = connection.cursor()
        _create_pivot_table(cursor, *args, **kwargs)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create a pivot table')
    parser.add_argument('source_schema')
    parser.add_argument('source_table')
    parser.add_argument('table_name')
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
    parser.add_argument('--base-columns', nargs='+')
    parser.add_argument('--pivot-columns', nargs='+')
    parser.add_argument('--exclude-columns', nargs='*')
    parser.add_argument('--exclude-aggregates', nargs='*')
    parser.add_argument('--aggfunction-mappings', nargs='*')

    args = parser.parse_args()

    if args.dbtype == 'snowflake':
        connection_dict = {
            'user': args.user,
            'password': args.password or '',
            'schema': args.source_schema or '',
            'database': args.database or '',
            'account': args.account or '',
            'warehouse': args.warehouse or ''
        }
    else:
        connection_dict = {
            'host': args.host,
            'dbname': args.source_schema,
            'port': args.port,
            'user': args.login,
            'password': args.password
        }

    # Convert the aggregate function mappings from a list of key=val items into
    # a dictionary
    aggfunctions = [item.split('=') for item in args.aggfunction_mappings]
    aggfunctions = collections.OrderedDict(aggfunctions)

    query = create_pivot_table(
        args.dbtype,
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
