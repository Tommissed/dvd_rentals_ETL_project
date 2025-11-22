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
-- need to make an actor list so that for one movie,
--  there will only be one row corresponding to its actors

--returns a list of all actors in a movie
with actor_list as (select 
    f.film_id,
    LISTAGG(concat(a.first_name, ' ', a.last_name), ', ') as actors
from {{ ref('film')}} as f
left join {{ ref('film_actor')}} as fa
    on f.film_id=fa.film_id
left join {{ ref('actor')}} as a
    on fa.actor_id=a.actor_id
group by f.film_id)

select
    f.film_id,
    f.title,
    al.actors,
    f.description,
    f.release_year,
    l.name as language,
    f.rental_duration,
    f.rental_rate,
    f.length,
    f.replacement_cost,
    f.rating,
    f.special_features,
    c.name as category
from {{ ref('film')}} as f
left join {{ ref('language')}} as l
    on f.language_id=l.language_id
left join {{ ref('film_category')}} as fc
    on f.film_id=fc.film_id
left join {{ ref('category')}} as c
    on fc.category_id=c.category_id
left join actor_list as al
    on f.film_id=al.film_id