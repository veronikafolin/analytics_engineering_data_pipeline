with

metadata_tables as (
    select
        table_catalog,
        table_schema,
        table_name,
--        table_catalog || '.' || table_schema || '.' || table_name AS table_ref,
        row_count
    from {{ source('metadata', 'tables') }}
    where deleted is null and table_catalog = 'ANALYTICS' and table_schema = 'DBT_CORE_REUSABLE_DEMO'
)

--metadata_views as (
--    select
--        table_catalog,
--        table_schema,
--        table_name,
--        table_catalog || '.' || table_schema || '.' || table_name AS table_ref,
--        COALESCE(null, 0) as row_count
--    from {{ source('metadata', 'views') }}
--    where deleted is null and table_catalog = 'ANALYTICS' and table_schema = 'DBT_CORE_REUSABLE_DEMO'
--),
--
--final as (
--    select * from metadata_tables
--    union
--    select * from metadata_views
--)

select * from metadata_tables