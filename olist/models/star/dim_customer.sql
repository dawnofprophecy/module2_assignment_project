{{ config(materialized='table') }}

-- Deterministic surrogate key: same input -> same key
WITH base AS (
  SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
  FROM {{ ref('stg_customers') }}
)
SELECT
  TO_HEX(MD5(CAST(customer_id AS STRING))) AS customer_key,
  *
FROM base
