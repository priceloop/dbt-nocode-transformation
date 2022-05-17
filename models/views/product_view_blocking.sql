{{ config(alias=env_var('DESTINATION_TABLE', var('destination_table', '')) + "_view_blocking") }}

with blocking as (
    select * from {{ ref('product') }}
)

select * from blocking