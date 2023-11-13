# Demo on DBT Core + Snowflake

The database used is made available on Snowflake (Snowsight), after registering an account.

Dataset documentation: https://docs.snowflake.com/en/user-guide/sample-data-tpch

## Setup
* Clone the repository
* Create a [Trial Account](https://signup.snowflake.com/) in Snowflake
* [Setup](https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup) the connection with the data platform
  with the method you prefer
* `dbt deps` to install packages specified in `packages.yml`

## Usage

Try running the following commands:
- `dbt run`
- `dbt run -m kpi_volume_orders_groupBy_where --vars '{groupBy: [cust_region_name], filters: [{field: orderstatus, value: F}]}'`
- `dbt test`
- `dbt build`
- `dbt build -m <model_name>`
- `dbt docs generate`