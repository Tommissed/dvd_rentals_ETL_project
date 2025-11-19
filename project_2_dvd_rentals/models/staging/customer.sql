{{
    config(
        materialized="incremental",
        unique_key=["customer_id"],
        incremental_strategy="delete+insert"
    )
}}

select
    customer_id,
    first_name,
    last_name,
    create_date,
    active,
    address_id,
    email,
    store_id,
    last_update
from {{ source('dvd_rental', 'customer') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}
