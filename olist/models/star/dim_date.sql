{{ config(materialized='table') }}

WITH calendar AS (
  SELECT day AS calendar_date
  FROM UNNEST(GENERATE_DATE_ARRAY('2016-01-01','2018-12-31', INTERVAL 1 DAY)) AS day
)
SELECT
  CAST(FORMAT_DATE('%Y%m%d', calendar_date) AS INT64) AS date_key,
  calendar_date                               AS date,
  EXTRACT(DAY FROM calendar_date)             AS day,
  EXTRACT(WEEK FROM calendar_date)            AS week,
  EXTRACT(MONTH FROM calendar_date)           AS month,
  EXTRACT(QUARTER FROM calendar_date)         AS quarter,
  EXTRACT(YEAR FROM calendar_date)            AS year,
  CASE WHEN EXTRACT(DAYOFWEEK FROM calendar_date) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM calendar
