{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["language_id"]
        )
}}


select
    language_id,
    last_update,
    name
from {{source ('dvd_rental','language')}}

{% if is_incremental() %}
    where last_update > (select max(last_update) from {{ this }} )
{% endif %}
