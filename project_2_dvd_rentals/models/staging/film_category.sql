{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert"
        )
}}


select
    category_id,
    film_id,
    last_update
from {{source ('dvd_rental','film_category')}}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}
