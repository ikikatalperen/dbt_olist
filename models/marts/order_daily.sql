
WITH base AS (
    SELECT
        order_purchase_date,
        order_id,
        customer_unique_id,
        total_order_value,
        delivery_days,
        is_cancelled,
        is_delayed,
        is_seller_delays,
        avg_review_score,
        is_new_customer,
        order_status
    FROM {{ ref('int_orders_joined') }}
)

SELECT
    order_purchase_date,

    COUNT(order_id) AS nb_orders,

    COUNT(DISTINCT customer_unique_id) AS nb_customers,

    COUNT(DISTINCT CASE 
    WHEN is_new_customer = 1 THEN customer_unique_id 
    END) AS nb_new_customers,

    COUNT(DISTINCT customer_unique_id) - COUNT(DISTINCT CASE 
        WHEN is_new_customer = 1 THEN customer_unique_id 
    END) AS nb_returning_customers,
    
    SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) AS nb_delivered_orders,
    
    SUM(is_cancelled) AS nb_canceled_orders, -- iptal sayısı 
    ROUND(AVG(is_cancelled), 4) AS cancel_ratio, -- iptal oranı

    ROUND(AVG(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END), 4) AS delivered_ratio,

    
    ROUND(SUM(total_order_value), 2) AS total_order_value, -- toplam gelir
    ROUND(AVG(total_order_value), 2) AS avg_order_value, -- ortalama sepet

   
    ROUND(AVG(delivery_days), 2) AS avg_delivery_days,  -- ortalama teslimat süresi

    ROUND(AVG(is_delayed), 4) AS delay_ratio, --müşteri teslimatı gecikme oranı
    ROUND(AVG(is_seller_delays), 4) AS seller_delay_ratio,   --satıcı kargolama gecikme oranı

    -- Review
    ROUND(AVG(avg_review_score), 2) AS avg_review_score

FROM base
GROUP BY order_purchase_date
ORDER BY order_purchase_date