select 
f.film_id,
p.payment_date,
p.customer_id,
sum(p.amount) as film_revenue,
count(r.rental_id) as rental_count 
from {{ref('payment')}} p
inner join {{ref('rental')}} r on r.rental_id=p.rental_id
inner join {{ref('inventory')}} i on i.inventory_id=r.inventory_id
inner join {{ref('film')}} f on f.film_id=i.film_id
group by f.film_id,p.payment_date,p.customer_id
order by p.payment_date,f.film_id
