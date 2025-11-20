select
s.staff_id,
s.first_name,
s.last_name,
s.email,
s.active,
a.city
from {{ref ('staff')}} as s
left join {{ref('store')}} on store.store_id=s.store_id
left join {{ref('dim_address_scd')}} as a on a.address_nk=store.address_id
 and store.last_update>=a.effective_start_date
 and store.last_update<coalesce(a.effective_end_date,{{dbt.current_timestamp()}})