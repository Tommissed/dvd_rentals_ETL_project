{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["payment_id"]
        )
}}


select
    amount,
    customer_id,
    payment_date,
    payment_id,
    rental_id,
    staff_id
from {{source ('dvd_rental','payment')}}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}
