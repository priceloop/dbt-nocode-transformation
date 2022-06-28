{{ config(alias=env_var('DESTINATION_TABLE', var('destination_table', '')) + "_view") }}

with view as (
    select * from {{ ref('product') }}
)

select * from view