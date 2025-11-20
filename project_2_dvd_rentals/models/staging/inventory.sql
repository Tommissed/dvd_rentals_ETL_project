{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["inventory_id"]
        )
}}


select
    inventory_id,
    film_id,
    store_id
    last_update
from {{source ('dvd_rental','inventory')}}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}
