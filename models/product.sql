{{
    config(
        alias=env_var('DESTINATION_TABLE', var('destination_table', ''))
    )
}}

select * from {{ source('airbyte', env_var('SOURCE_TABLE', var('source_table', ''))) }}
