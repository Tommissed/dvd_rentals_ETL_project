{{
    config(
        materialized="incremental",
        unique_key=["category_id"],
        incremental_strategy="delete+insert"
    )
}}

select
    category_id,
    name,
    last_update
from {{ source('dvd_rental', 'category') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}