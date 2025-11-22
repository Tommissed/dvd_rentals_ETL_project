select 
p.customer_id,
p.payment_date,
sum(p.amount) as film_revenue
from {{ref('payment')}} p
group by p.customer_id, p.payment_date
order by p.payment_date,p.customer_id
