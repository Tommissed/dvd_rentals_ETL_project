{{
    config(
        materialized="incremental",
        unique_key=["country_id"],
        incremental_strategy="delete+insert"
    )
}}

select
    country_id,
    country,
    last_update
from {{ source('dvd_rental', 'country') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}
