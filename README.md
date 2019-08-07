<!-- -*- mode: gfm -*- -->

# Create Pivot Table

This project provides an easy way to create a pivot table. It features pivot
customization options similar to Excel and includes an [Airflow][] operator.

[Airflow]: https://airflow.apache.org/

The `generate_pivot_query.py` and `create_pivot_table.py` scripts can be used
directly, or you can use `create_pivot_table_operator.CreatePivotTableOperator`
with Airflow.

For convenience, this project includes `create_table_from_select.py` directly.
`create_table_from_select.py` is also [available separately][ctfs]; see [its
README][ctfs-readme] for more information.

[ctfs]: https://github.com/Ro-Data/Ro-Create-Table-From-Select
[ctfs-readme]: https://github.com/Ro-Data/Ro-Create-Table-From-Select/blob/master/README.md

The only databases currently supported are [Snowflake][snowflakke] and Amazon
Redshift.

[snowflake]: https://www.snowflake.com

## Requirements

- [Snowflake Connector for Python][snowflake-connector] is required for
  Snowflake support
- [`psycopg2`][psycopg2] is required for Redshift support
- [`airflow`][airflow] is required to use the (optional)
  `CreatePivotTableOperator` or load database connection information from
  airflow

[snowflake-connector]: https://docs.snowflake.net/manuals/user-guide/python-connector.html
[psycopg2]: http://initd.org/psycopg/
[airflow]: https://airflow.apache.org/

## Usage

Suppose you have a database table, `myschema.orders`, that looks something like
this:

| order_id | customer_id | category | amount   |
| -------- | ----------- | ---------| -------- |
| 100      | 1           | gizmos   | 100.00   |
| 101      | 1           | gadgets  | 200.00   |
| 102      | 2           | gadgets  | 220.00   |
| 103      | 2           | gizmos   | 85.00    |

And you want to pivot it by `category` with `customer_id` as the base
column so that you'll end up with a result like:

| customer_id | amount_category_gizmos | amount_category_gadgets | amount |
| ----------- | ---------------------- | ----------------------- | ------ |
| 1           | 100.00                 | 200.00                  | 300.00 |
| 2           | 85.00                  | 220.00                  | 305.00 |

You can use the scripts in this repository in three main ways to achieve this.

### Generate pivot query text

You can use `generate_pivot_query.py` to generate the pivot query and write it
to standard output:

```sh
python generate_pivot_query.py                  \
    --dbtype snowflake --database mydb          \
    --host myhost.url --port 5432               \
    --user me --password myp4ssw0rd             \
    --base-columns customer_id                  \
    --pivot_columns category                    \
    --exclude-columns order_id                  \
    --aggfunction-mappings amount=sum           \
    myschema orders
```

The resulting query will resemble:

``` sql
SELECT
    customer_id,
    SUM(CASE
            WHEN category = 'gizmos' THEN amount
            ELSE 0
    END) AS amount_category_gizmos,
    SUM(CASE
            WHEN category = 'gadgets' THEN amount
            ELSE 0
    END) AS amount_category_gadgets,
    SUM(amount) AS amount
FROM
    myschema.orders
```

### Create the pivot table

Or, you can use `create_pivot_table.py` to go a step further and create the
pivot table under e.g. `orders_summary`:

```sh
python create_pivot_table.py                    \
    --dbtype snowflake --database mydb          \
    --host myhost.url --port 5432               \
    --user me --password myp4ssw0rd             \
    --base-columns customer_id                  \
    --pivot_columns category                    \
    --exclude-columns order_id                  \
    --aggfunction-mappings amount=sum           \
    myschema orders orders_summary
```

### Airflow

To do the same thing using an Airflow operator, you could define your DAG such
as:

```python
from airflow import DAG

from create_pivot_table_operator import CreatePivotTableOperator

default_args = {
    # ...
    'snowflake_conn_id': 'my-snowflake-conn-id'
}

dag = DAG(
    # ...
    default_args=default_args
)

my_orders_summary_op = CreatePivotTableOperator(
    source_schema='myschema',
    source_table='orders',
    table_name='orders_summary',
    base_columns=['customer_id'],
    pivot_columns=['category'],
    aggfunction_mappings={'amount': 'sum'},
    dag=dag
)
```

### Supported options

You can specify an arbitrary number of columns for both `base_columns` and
`pivot_columns`.

Columns listed in `exclude_columns` won't be included in the resulting query or
table at all. If a column is instead listed in `exclude_aggregates`, the pivoted
versions will still be created, but not the overall aggregate. In the example
above, this means that `amount_category_gizmos` and `amount_category_gadgets`
would be created but not `amount`.

The `aggfunction_mappings` option specifies how the aggregation should work. In
the example above, we specified `sum`, which meant that the query used `SUM`
with a default of `0`. The currently supported options are:

| Name    | Aggregate  | Default |
| ------- | ---------- | ------- |
| max     | `MAX`      | `NULL`  |
| min     | `MIN`      | `NULL`  |
| sum     | `SUM`      | `0`     |
| list    | `LISTAGG`  | `NULL`  |
| and     | `MIN`      | `true`  |
| or      | `MAX`      | `NULL`  |

If a column isn't explicitly specified in `aggfunction_mapping`, the script will
attempt to guess a reasonable choice based on the column's name. For example,
column's beginning with `first_` will be aggregated based on "min", and column's
beginning with `latest_` will be aggregate based on "max".
