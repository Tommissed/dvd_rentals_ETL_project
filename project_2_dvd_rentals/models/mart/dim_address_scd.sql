
{{ config(
    materialized = 'table'         
) }}

select
    {{ dbt_utils.generate_surrogate_key(['address_id', 'dbt_valid_from']) }} as address_sk,
    address_id as address_nk,   -- natural key
    address,
    address2,
    district,
    city.city,
    postal_code,
    country.country,
    phone,
    cast(dbt_valid_from as date) as effective_start_date,
    cast(dbt_valid_to as date) as effective_end_date,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref('address_history') }}   
left join {{ref('city')}} on city.city_id=address_history.city_id
left join {{ref('country')}} on country.country_id=city.country_id