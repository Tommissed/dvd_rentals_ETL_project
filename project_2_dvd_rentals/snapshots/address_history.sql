{% snapshot address_history %}

{{ 
  config(
    target_schema = 'snapshots', 
    unique_key    = 'address_id',
    strategy      = 'check',
    check_cols    = ['address', 'address2', 'district', 'city_id', 'postal_code', 'phone']
  ) 
}}

select
  address_id,
  address,
  address2,
  district,
  city_id,
  postal_code,
  phone,
  last_update
from {{ source('dvd_rental', 'address') }}

{% endsnapshot %}