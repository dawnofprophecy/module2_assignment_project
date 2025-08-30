-- SELECT
--   product_id,
--   product_category_name,
--   CAST(product_weight_g AS FLOAT64) AS weight_g,
--   CAST(product_length_cm AS FLOAT64) AS length_cm,
--   CAST(product_height_cm AS FLOAT64) AS height_cm,
--   CAST(product_width_cm  AS FLOAT64) AS width_cm
-- FROM `{{ target.project }}.olist_raw.products`;

{{ config(materialized='view') }}
select
  product_id,
  product_category_name,
  cast(product_weight_g as float64) as weight_g,
  cast(product_length_cm as float64) as length_cm,
  cast(product_height_cm as float64) as height_cm,
  cast(product_width_cm  as float64) as width_cm
from {{ source('olist_raw','products') }}
