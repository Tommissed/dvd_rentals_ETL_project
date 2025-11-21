{{
  config(
    materialized='table'
  )
}}

--dim table for rental info
--customer -> address -> city -> country
--bring into one dim table to maintain a star schema

select
    c.customer_id,
    c.store_id,
    concat(c.first_name, ' ', c.last_name) as full_name,
    c.first_name,
    c.last_name,
    c.email,
    c.activebool,
    a.address,
    a.district,
    a.postal_code,
    ci.city,
    co.country
from {{ ref('customer') }} as c
left join {{ ref('address')}} as a
    on a.address_id=c.address_id
left join {{ ref('city')}} as ci 
    on ci.city_id=a.city_id
left join {{ ref('country')}} as co 
    on co.country_id=ci.country_id