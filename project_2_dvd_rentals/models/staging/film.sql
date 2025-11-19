{{
    config(
        materialized="incremental",
        unique_key=["film_id"],
        incremental_strategy="delete+insert"
    )
}}

select
    film_id,
    title,
    description,
    release_year,
    replacement_cost,
    length,
    rating,
    rental_rate,
    rental_duration,
    special_features,
    language_id,
    fulltext,
    last_update
from {{ source('dvd_rental', 'film') }}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}
