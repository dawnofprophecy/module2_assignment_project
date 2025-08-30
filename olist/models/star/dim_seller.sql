{{ config(materialized='table') }}

WITH base AS (
  SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
  FROM {{ ref('stg_sellers') }}
)
SELECT
  TO_HEX(MD5(CAST(seller_id AS STRING))) AS seller_key,
  *
FROM base
