{{ config(
    materialized = 'view'
) }}

WITH items AS (
    SELECT * 
    FROM {{ ref('stg_raw__order_items_dataset') }}
),

orders AS (
    SELECT 
        order_id,
        order_approved_at,   
        order_delivered_carrier_date 
    FROM {{ ref('stg_raw__orders_dataset') }}
),

products AS (
    SELECT * 
    FROM {{ ref('stg_raw__products_dataset') }}
),

translations AS (
    SELECT * 
    FROM {{ ref('stg_raw__product_category_name_translation') }}
),

sellers AS (
    SELECT * 
    FROM {{ ref('stg_raw__sellers_dataset') }}
),

final AS (
    SELECT
        items.order_id,
        items.order_item_id,
        items.product_id,
        items.seller_id,
        items.shipping_limit_date,           -- satıcının son teslim tarihi 
        orders.order_delivered_carrier_date, -- Satıcının kargoya verdiği tarih      
        items.price,
        items.freight_value,
        COALESCE(
            translations.product_category_name_english,
            translations.product_category_name_pt,
            products.product_category_name,
            'Unknown'
        ) AS product_category_name,
        products.product_name_length,
        products.product_description_length,
        products.product_photos_qty,
        products.product_length_cm * products.product_height_cm * products.product_width_cm / 1000 AS product_volume_dm3,        
        sellers.seller_city,
        sellers.seller_state,
        CASE 
            WHEN orders.order_delivered_carrier_date > items.shipping_limit_date THEN 1
            ELSE 0 
        END AS is_seller_delayed

    FROM items
    LEFT JOIN orders       ON items.order_id   = orders.order_id
    LEFT JOIN products     ON items.product_id = products.product_id
    LEFT JOIN translations ON products.product_category_name = translations.product_category_name_pt
    LEFT JOIN sellers      ON items.seller_id  = sellers.seller_id
)

SELECT *
FROM final