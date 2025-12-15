{{ config(materialized = 'table') }}

WITH base AS (
  SELECT
    seller_id,
    seller_city,
    seller_state,
    order_id,
    product_id,
    price,
    freight_value,
    is_seller_delayed,
    shipping_limit_date,
    order_delivered_carrier_date
  FROM {{ ref('int_items_joined') }}
),

base_with_delay AS (
  SELECT
    *,
    -- sadece gecikenlerde gecikme gün sayısı (gecikmeyenler NULL)
    CASE
      WHEN shipping_limit_date IS NULL OR order_delivered_carrier_date IS NULL THEN NULL
      WHEN order_delivered_carrier_date > shipping_limit_date
        THEN DATE_DIFF(DATE(order_delivered_carrier_date), DATE(shipping_limit_date), DAY)
      ELSE NULL
    END AS seller_delay_days_late
  FROM base
),

seller_level AS (
  SELECT
    seller_id,
    seller_city,
    seller_state,

    -- Volumes
    COUNT(*) AS seller_total_items_sold,
    COUNT(DISTINCT order_id) AS seller_nb_orders,
    COUNT(DISTINCT product_id) AS seller_nb_products,

    -- Financials
    ROUND(SUM(price), 2) AS seller_total_product_revenue,
    ROUND(SUM(freight_value), 2) AS seller_total_freight_revenue,
    ROUND(SUM(price + freight_value), 2) AS seller_total_revenue,

    ROUND(AVG(price), 2) AS seller_avg_item_price,
    ROUND(SAFE_DIVIDE(SUM(price + freight_value), COUNT(DISTINCT order_id)), 2) AS seller_avg_order_value,

    -- Delay KPIs
    SUM(CAST(is_seller_delayed AS INT64)) AS seller_nb_delayed_shipments,
    ROUND(AVG(CAST(is_seller_delayed AS FLOAT64)), 4) AS seller_delay_ratio,

    -- ✅ sadece gecikenlerin ortalama gecikme gün sayısı
    COALESCE(
  ROUND(AVG(seller_delay_days_late), 2),
  0
) AS avg_seller_delay_days

  FROM base_with_delay
  GROUP BY seller_id, seller_city, seller_state
)

SELECT *
FROM seller_level