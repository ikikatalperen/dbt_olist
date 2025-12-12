{{ config(materialized = 'table') }}

WITH base AS (
  SELECT *
  FROM {{ref("int_items_joined")}}
)

SELECT
product_id,
product_category_name_english AS category_name,

COUNT(*) as sales_qty

SUM(price + freight_value) AS gross_revenue,

AVG(price) AS avg_price

COUNT(DISTINCT order_id) AS nb_orders

COUNT(DISTINCT seller_id) AS nb_sellers

COUNT(DISTINCT seller_state) AS nb_seller_state

FROM base

