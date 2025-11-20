{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["staff_id"]
        )
}}


select
    active,
    address_id,
    email,
    first_name,
    last_name,
    last_update,
    password,
    picture,
    staff_id,
    store_id,
    username
from {{source ('dvd_rental','staff')}}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}
