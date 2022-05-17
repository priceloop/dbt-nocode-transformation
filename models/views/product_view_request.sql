{{ config(alias=env_var('DESTINATION_TABLE', var('destination_table', '')) + "_view_request") }}

with request as (
    select * from {{ ref('product') }}
)

select * from request