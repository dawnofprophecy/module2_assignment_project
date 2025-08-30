-- SELECT
--   order_id,
--   order_item_id,
--   product_id,
--   seller_id,
--   CAST(price AS NUMERIC)         AS price,
--   CAST(freight_value AS NUMERIC) AS freight_value
-- FROM `{{ target.project }}.olist_raw.order_items`;

{{ config(materialized='view') }}
select
  order_id,
  order_item_id,
  product_id,
  seller_id,
  cast(shipping_limit_date as timestamp) as shipping_limit_ts,
  cast(price as numeric)                 as price,
  cast(freight_value as numeric)         as freight_value
from {{ source('olist_raw','order_items') }}
