{{ config(
    materialized = 'view'
) }}

WITH orders AS (
    SELECT 
        order_id,
        customer_id, 
        order_status,
        order_purchase_timestamp,       -- sipariş tarihi
        order_approved_at,              -- sipariş onay tarihi
        order_delivered_carrier_date,   -- kargoya veriliş tarihi
        order_delivered_customer_date,  -- müşteriye teslim tarihi
        order_estimated_delivery_date,  -- tahmini teslim tarihi
        DATE(order_purchase_timestamp) as order_purchase_date,
        DATE(order_approved_at) as order_approved_day,
        DATE(order_delivered_carrier_date) as order_delivered_carrier_date_date,
        DATE(order_delivered_customer_date) as order_delivered_customer_date_date,
        DATE(order_estimated_delivery_date) as order_estimated_delivery_date_date,
        FORMAT_TIMESTAMP('%Y-%m', order_purchase_timestamp) AS order_purchase_month

    FROM {{ ref('stg_raw__orders_dataset') }}
),

customers AS (
    SELECT * 
    FROM {{ ref('stg_raw__customers_dataset') }}
),

payment_aggs AS (
    SELECT
        order_id,
        ROUND(SUM(payment_value),2) AS total_order_value,        
        COUNT(payment_sequential) AS payment_seq_count  
    FROM {{ ref('stg_raw__order_payments_dataset') }}
    GROUP BY order_id
),

review_aggs AS (
    SELECT
        order_id,
        AVG(review_score) AS avg_review_score          
    FROM {{ ref('stg_raw__order_reviews_dataset') }}
    GROUP BY order_id
),

shipping_limit AS (
    SELECT
        order_id,
        DATE(MIN(shipping_limit_date)) AS shipping_limit_date
    FROM {{ref("stg_raw__order_items_dataset")}}
    GROUP BY order_id
)

SELECT
    ord.order_id,
    ord.customer_id,
    cust.customer_unique_id, 
    ord.order_purchase_date,
    ord.order_purchase_month,
    ord.order_approved_day,
    ord.order_delivered_carrier_date_date,
    shp.shipping_limit_date,
    ord.order_delivered_customer_date_date,
    ord.order_estimated_delivery_date_date,
    cust.customer_city,
    cust.customer_state,
    COALESCE(pay.total_order_value, 0) AS total_order_value, -- müşterinin ödediği toplam tutar
      rev.avg_review_score, -- ortalama review score u
    TIMESTAMP_DIFF(
        ord.order_delivered_customer_date, -- müşteriye teslim tarihi
        ord.order_purchase_timestamp, -- sipariş tarihi
        DAY
    ) AS delivery_days, --teslimat süresi
    CASE 
        WHEN ord.order_delivered_customer_date > ord.order_estimated_delivery_date THEN 1
        ELSE 0
    END AS is_delayed, -- tahmini süreye göre gecikme durumu
    ord.order_status,

    CASE
        WHEN ord.order_delivered_carrier_date_date > shp.shipping_limit_date THEN 1 
        ELSE 0
    END AS is_seller_delays,
                              --satıcının kargoya son teslim gününe göre gecikme 
    
    
    CASE WHEN ord.order_status = 'canceled' THEN 1 ELSE 0 END AS is_cancelled,

    CASE
        WHEN RANK() OVER (PARTITION BY customer_unique_id ORDER BY ord.order_purchase_timestamp) = 1
                THEN 1 ELSE 0
        END AS is_new_customer

FROM orders AS ord
LEFT JOIN customers   AS cust ON ord.customer_id = cust.customer_id
LEFT JOIN payment_aggs AS pay ON ord.order_id   = pay.order_id
LEFT JOIN review_aggs  AS rev ON ord.order_id   = rev.order_id
LEFT JOIN shipping_limit AS shp ON ord.order_id = shp.order_id