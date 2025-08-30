{{ config(materialized='view') }}
select
  geolocation_zip_code_prefix,
  geolocation_lat as lat,
  geolocation_lng as lng,
  geolocation_city as city,
  geolocation_state as state
from {{ source('olist_raw','geolocation') }}
