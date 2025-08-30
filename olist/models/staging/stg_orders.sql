-- SELECT
--   order_id,
--   customer_id,
--   order_status,
--   CAST(order_purchase_timestamp AS TIMESTAMP)      AS order_purchase_ts,
--   CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_customer_ts,
--   CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_ts
-- FROM `{{ target.project }}.olist_raw.orders`;

{{ config(materialized='view') }}
select
  order_id,
  customer_id,
  order_status,
  cast(order_purchase_timestamp as timestamp)        as order_purchase_ts,
  cast(order_approved_at as timestamp)               as order_approved_ts,
  cast(order_delivered_carrier_date as timestamp)    as delivered_carrier_ts,
  cast(order_delivered_customer_date as timestamp)   as delivered_customer_ts,
  cast(order_estimated_delivery_date as timestamp)   as estimated_delivery_ts
from {{ source('olist_raw','orders') }}
