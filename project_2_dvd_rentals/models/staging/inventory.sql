{{
    config(
        materialized="incremental",
        unique_key=["inventory_id"]
        )
}}


select
    inventory_id,
    film_id,
    store_id
    last_update
from {{source ('dvd_rental','inventory')}}

