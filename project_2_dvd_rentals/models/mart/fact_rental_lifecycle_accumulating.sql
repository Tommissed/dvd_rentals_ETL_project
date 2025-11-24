-- one row per rental event
-- what should each row include?
-- what customer had the rental_event
-- what film was rented
-- when the film was rented
-- what staff member processed the rental
-- what store they rented from

-- rentals per customer (fact -> dim_customer)
-- rentals per country (fact ->dim_customer -> country)
-- rentals per staff (fact -> dim_staff)
-- rentals per film (fact -> dim_film)
-- rentals per category (fact -> dim_fiml -> category)
-- accumulating data -> the table will track the rental date and return date milestones

select
    r.customer_id,
    r.rental_id,
    s.staff_id,
    s.store_id,

    f.film_id,
    f.title as film_name,

    --number of days youre allowed to rent the film
    f.rental_duration as max_rental_duration,
    --number of days the film was rented
    DATEDIFF(day, r.rental_date, r.return_date) as length_of_rental,

    r.rental_date,
    r.return_date,
    CASE 
        when r.return_date is not null
            and DATEDIFF(day, r.rental_date, r.return_date) > f.rental_duration
        then true
        else false
    END as returned_late_bool
    
from {{ref('rental')}} as r
left join {{ref('inventory')}} as i
    on r.inventory_id=i.inventory_id
left join {{ref('film')}} as f
    on i.film_id=f.film_id
left join {{ref('staff')}} as s
    on r.staff_id=s.staff_id