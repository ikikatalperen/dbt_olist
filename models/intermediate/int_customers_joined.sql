{{ config(
    materialized = 'view'
) }}

WITH base AS (
    SELECT
        order_id,
        customer_id,
        customer_unique_id,
        order_purchase_date,
        total_order_value,
        customer_state
        
    FROM {{ ref('int_orders_joined') }}
),

with_lag AS (
    SELECT
        customer_unique_id,
        order_id,
        order_purchase_date,
        total_order_value,
        
        LAG(order_purchase_date) OVER (
            PARTITION BY customer_unique_id
            ORDER BY order_purchase_date
        ) AS prev_order_ts
    FROM base
),

with_gap AS (
    SELECT
        customer_unique_id,
        order_id,
        order_purchase_date,
        total_order_value,
       

        DATE_DIFF(
            DATE(order_purchase_date),
            DATE(prev_order_ts),
            DAY
        ) AS order_gap_days
    FROM with_lag
),

customer_metrics AS (
    SELECT
        customer_unique_id,
        

        -- Sipariş sayısı
        COUNT(DISTINCT order_id) AS nb_orders,

        -- CLV: toplam sipariş değeri
        ROUND(SUM(total_order_value),2) AS clv,

        -- Ortalama sipariş değeri
        ROUND(AVG(total_order_value),2) AS avg_order_value,

        -- İlk ve son sipariş tarihleri
        MIN(order_purchase_date) AS first_order_date,
        MAX(order_purchase_date) AS last_order_date,

        
        DATE_DIFF(
            DATE(MAX(order_purchase_date)),
            DATE(MIN(order_purchase_date)),
            DAY
        ) AS customer_lifetime_days, -- Müşteri yaşam süresi (gün cinsinden)
    
        ROUND(AVG(order_gap_days),2) AS avg_days_between_orders,   -- Siparişler arasında geçen ortalama süre 

        CASE WHEN COUNT(DISTINCT order_id) = 1 THEN 1 ELSE 0 END AS one_time_customer,  -- Tek seferlik sipariş veren müşteri
    
       
        DATE_DIFF(
            DATE '2019-01-01',
            DATE(MAX(order_purchase_date)),
            DAY
        ) AS recency_days


    FROM with_gap
    GROUP BY customer_unique_id
)

SELECT c.*,
b.customer_state
FROM customer_metrics AS c
LEFT JOIN base AS b ON b.customer_unique_id = c.customer_unique_id 