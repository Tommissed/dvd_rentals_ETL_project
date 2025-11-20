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
    where payment_date > (select max(payment_date) from {{ this }} )
{% endif %}
