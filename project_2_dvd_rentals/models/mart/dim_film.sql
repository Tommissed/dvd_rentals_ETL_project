{{
  config(
    materialized='table'
  )
}}

--dim table for rental info
--inventory -> film -> film_actor -> actor / language / film_category -> category
--bring into one dim table to maintain a star schema
-- to fix: one row should correspond to one film
-- currently because of multiple actors per film,
--  one film will have MANY rows, for as many actors there are.

select
    i.inventory_id,
    i.store_id,
    film.title,
    film.description,
    film.release_year,
    l.name as language,
    film.rental_duration,
    film.rental_rate,
    film.length,
    film.replacement_cost,
    film.rating,
    film.special_features,
    film.fulltext,
    a.first_name,
    a.last_name,
    concat(a.first_name, ' ', a.last_name) as actor_full_name,
    c.name as category

from {{ ref('inventory') }} as i
left join {{ ref('film')}} as film
    on i.film_id=film.film_id
left join {{ ref('language')}} as l
    on film.language_id=l.language_id
left join {{ ref('film_actor')}} as fa
    on film.film_id=fa.film_id
left join {{ ref('actor')}} as a
    on fa.actor_id=a.actor_id
left join {{ ref('film_category')}} as fc
    on film.category_id=fc.category_id
left join {{ ref('category')}} as c
    on fc.category_id=c.category_id