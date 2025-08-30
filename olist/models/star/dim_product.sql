{{ config(materialized='table') }}

WITH base AS (
  SELECT
    p.product_id,
    COALESCE(ct.category_en, p.product_category_name) AS product_category_name_en,
    p.product_category_name                          AS product_category_name_pt,
    p.weight_g, p.length_cm, p.height_cm, p.width_cm
  FROM {{ ref('stg_products') }} p
  LEFT JOIN {{ ref('stg_category_translation') }} ct
    ON p.product_category_name = ct.category_pt
)
SELECT
  TO_HEX(MD5(CAST(product_id AS STRING))) AS product_key,
  *
FROM base
