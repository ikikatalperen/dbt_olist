{{ config(materialized = 'table') }}

WITH base AS (

  SELECT *
  FROM {{ref("int_items_joined")}}

),

product_level AS ( 

    SELECT
product_id,
product_category_name AS category_name,

COUNT(*) as product_total_qty,

ROUND(SUM(price + freight_value),2) AS product_total_revenue,

ROUND(AVG(price),2) AS product_avg_price,

COUNT(DISTINCT order_id) AS product_nb_orders,

COUNT(DISTINCT seller_id) AS nb_sellers,

COUNT(DISTINCT seller_state) AS nb_seller_state

FROM base
GROUP BY product_id, category_name
),

product_category_metrics AS (

    SELECT
   
    product_id,
    category_name,
    product_total_qty,
    product_total_revenue,
    product_avg_price,
    product_nb_orders,
    nb_sellers,
    nb_seller_state,

    SUM(product_total_qty) OVER (
        PARTITION BY category_name
    ) AS category_total_qty, -- o kategoride satılan toplam ürün sayısı

    ROUND(SUM(product_total_revenue) OVER (
        PARTITION BY category_name
    ),2) AS category_total_revenue, -- o kategorinin toplam cirosu

    COUNT(DISTINCT product_id) OVER (
    PARTITION BY category_name
) AS category_nb_products,  -- o kategoride satılan ürün çeşitliliği

    ROUND(SAFE_DIVIDE(
    SUM(product_total_revenue) OVER (PARTITION BY category_name),
    SUM(product_total_qty) OVER (PARTITION BY category_name)
),2) AS category_avg_price    -- kategorinin ortalama satış fiyatı

FROM product_level

)

SELECT *
FROM product_category_metrics
