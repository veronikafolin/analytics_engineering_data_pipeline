### Demo on DBT Core

Dataset documentation: https://docs.snowflake.com/en/user-guide/sample-data-tpch

Try running the following commands:
- `dbt deps` to install packages specified in `packages.yml`
- `dbt run`
- `dbt test`
- `dbt build`
- `dbt build --select <model_name>`
- `dbt docs generate`