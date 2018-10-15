#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals

import copy

from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults

from generate_pivot_query import generate_pivot_query
from create_table_from_select import create_table_from_select


def create_pivot_table(**kwargs):
    pivot_query = generate_pivot_query(**kwargs)
    connection_dict = get_connection_dict()
    schema_name = kwargs['schema_name']
    table_name = kwargs['table_name']
    table_keys = {} # TODO: load these from a file if available
    create_table_from_select(
        connection_dict,
        pivot_query,
        schema_name,
        table_name,
        **table_keys
    )


class CreatePivotTableOperator(PythonOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        # Required arguments
        source_schema = kwargs.pop('source_schema')
        source_table = kwargs.pop('source_table')
        schema_name = kwargs.pop('schema_name')
        table_name = kwargs.pop('table_name')
        base_columns = kwargs.pop('base_columns')
        pivot_columns = kwargs.pop('pivot_columns')

        # Optional arguments
        exclude_columns = kwargs.pop('exclude_columns', [])
        aggfunction_mappings = kwargs.pop('aggfunction_mappings', {})
        exclude_aggregates = kwargs.pop('exclude_aggregates', [])

        # Apply defaults for `task_id` and `provide_context`
        if 'task_id' not in kwargs:
            kwargs['task_id'] = 'create_{}_{}_pivot_task'.format(
                schema_name, table_name
            )
        if 'provide_context' not in kwargs:
            kwargs['provide_context'] = True

        # Set our hard-coded `python_callable`
        kwargs['python_callable'] = create_pivot_table

        # Stick the arguments to `generate_pivot_query` into `op_kwargs`
        op_kwargs = kwargs['op_kwargs'] = kwargs.get('op_kwargs', {})
        op_kwargs['source_schema'] = source_schema
        op_kwargs['source_table'] = source_table
        op_kwargs['schema_name'] = schema_name
        op_kwargs['table_name'] = table_name
        op_kwargs['base_columns'] = base_columns
        op_kwargs['pivot_columns'] = pivot_columns
        op_kwargs['exclude_columns'] = exclude_columns
        op_kwargs['aggfunction_mappings'] = aggfunction_mappings
        op_kwargs['exclude_aggregates'] = exclude_aggregates

        # Initialize the instance with our special callable
        super(CreatePivotTableOperator, self).__init__(*args, **kwargs)

