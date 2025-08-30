{{ config(materialized='table') }}

WITH
oi AS (
  SELECT
    order_id, order_item_id, product_id, seller_id,
    shipping_limit_ts, price, freight_value
  FROM {{ ref('stg_order_items') }}
),
o AS (
  SELECT
    order_id, customer_id, order_status,
    order_purchase_ts, delivered_customer_ts, estimated_delivery_ts
  FROM {{ ref('stg_orders') }}
),
joined AS (
  SELECT
    oi.*,
    o.customer_id,
    o.order_status,
    o.order_purchase_ts,
    o.delivered_customer_ts,
    o.estimated_delivery_ts
  FROM oi
  LEFT JOIN o USING(order_id)
)
SELECT
  -- deterministic surrogate keys from dims
  TO_HEX(MD5(CAST(customer_id AS STRING))) AS customer_key,
  TO_HEX(MD5(CAST(seller_id   AS STRING))) AS seller_key,
  TO_HEX(MD5(CAST(product_id  AS STRING))) AS product_key,

  -- role-playing date keys
  CAST(FORMAT_DATE('%Y%m%d', DATE(order_purchase_ts))     AS INT64) AS order_date_key,
  CAST(FORMAT_DATE('%Y%m%d', DATE(estimated_delivery_ts)) AS INT64) AS estimated_delivery_date_key,
  CAST(FORMAT_DATE('%Y%m%d', DATE(delivered_customer_ts)) AS INT64) AS delivered_date_key,

  -- fact grain (order-item) + measures
  order_id,
  order_item_id,
  order_status,
  price,
  freight_value,
  price + freight_value                                   AS total_sale_amount,
  TIMESTAMP_DIFF(delivered_customer_ts, order_purchase_ts, DAY) AS delivery_lead_days,
  CASE
    WHEN delivered_customer_ts IS NULL OR estimated_delivery_ts IS NULL THEN NULL
    WHEN delivered_customer_ts >  estimated_delivery_ts THEN TRUE
    ELSE FALSE
  END AS delivered_late_flag
FROM joined
