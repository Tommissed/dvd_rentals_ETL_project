
{{ config(
    materialized = 'table'         
) }}

select
    {{ dbt_utils.generate_surrogate_key(['address_id', 'dbt_valid_from']) }} as address_sk,
    address_id as address_nk,   -- natural key
    address,
    address2,
    district,
    city_id,
    postal_code,
    phone,
    dbt_valid_from as effective_start_date,
    dbt_valid_to as effective_end_date,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref('address_history') }}   