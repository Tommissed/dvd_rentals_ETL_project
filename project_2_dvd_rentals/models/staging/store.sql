{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["store_id"]
        )
}}


select
    address_id,
    last_update,
    manager_staff_id,
    store_id
from {{source ('dvd_rental','store')}}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}
