with

previous_state_of_metadata as (
    select *
    from {{this}}
),

new_state_of_metadata as (
    select *
    from {{ source('metadata', 'tables') }}
    where table_catalog = 'ANALYTICS' and table_schema = 'ANALYTICS_ENGINEERING_DATA_PIPELINE' and table_type = 'BASE TABLE'
),

metadata_tables as (
    select
        LOWER(new.table_catalog) as table_catalog,
        LOWER(new.table_schema) as table_schema,
        LOWER(new.table_name) as table_name,
        LOWER(new.table_catalog) || '.' || LOWER(new.table_schema) || '.' || LOWER(new.table_name)  AS TABLE_REF,
        COALESCE(new.row_count, 0) as row_count,
        IFF(
            (COALESCE(new.row_count, 0) - COALESCE(old.row_count, 0)) = 0,
            COALESCE(new.row_count, 0),
            (COALESCE(new.row_count, 0) - COALESCE(old.row_count, 0))
        ) as row_count_delta
    from new_state_of_metadata as new left join previous_state_of_metadata as old on (TABLE_REF = old.TABLE_REF)
)

select * from metadata_tables