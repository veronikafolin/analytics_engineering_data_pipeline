with

metadata_tables as (
    select
        LOWER(table_catalog) as table_catalog,
        LOWER(table_schema) as table_schema,
        LOWER(table_name) as table_name,
        row_count
    from {{ source('metadata', 'tables') }}
    where table_catalog = 'ANALYTICS' and table_schema = 'DBT_CORE_REUSABLE_DEMO' and table_type = 'BASE TABLE'
)

--metadata_views as (
--    select
--        table_catalog,
--        table_schema,
--        table_name,
--        table_catalog || '.' || table_schema || '.' || table_name AS table_ref,
--        COALESCE(null, 0) as row_count
--    from {{ source('metadata', 'views') }}
--    where table_catalog = 'ANALYTICS' and table_schema = 'DBT_CORE_REUSABLE_DEMO'
--),
--
--final as (
--    select * from metadata_tables
--    union
--    select * from metadata_views
--)

select * from metadata_tables