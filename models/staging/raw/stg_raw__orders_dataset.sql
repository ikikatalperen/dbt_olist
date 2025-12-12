{{ config(
    materialized = 'view'
) }}

SELECT
    -- Kimlik kolonları
    order_id,
    customer_id,

    -- Sipariş durumu: boş string → NULL, hepsini küçük harfe çek
    NULLIF(LOWER(order_status), '') AS order_status,

    -- Zaman damgaları 
    order_purchase_timestamp,       
    order_approved_at,              
    order_delivered_carrier_date,   
    order_delivered_customer_date,  
    order_estimated_delivery_date, 

    -- Sık kullanılan date-only kolonlar
    DATE(order_purchase_timestamp)      AS order_purchase_date,
    DATE(order_delivered_customer_date) AS delivered_customer_date,
    DATE(order_estimated_delivery_date) AS estimated_delivery_date

FROM {{ source('raw', 'orders_dataset') }}