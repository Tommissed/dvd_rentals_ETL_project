{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["rental_id"]
        )
}}


select
    customer_id,
    inventory_id,
    last_update,
    rental_date,
    rental_id,
    return_date,
    staff_id
from {{source ('dvd_rental','rental')}}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}
