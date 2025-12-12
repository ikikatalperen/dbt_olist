{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        customer_unique_id,
        nb_orders,
        clv,
        avg_order_value,
        avg_days_between_orders,
        customer_lifetime_days,
        recency_days
    FROM {{ ref('int_customers_joined') }}
),

order_frequency AS (
    SELECT *,
        CASE 
            WHEN nb_orders = 1 THEN 1
            WHEN nb_orders BETWEEN 2 AND 3 THEN 2
            WHEN nb_orders BETWEEN 4 AND 6 THEN 3
            WHEN nb_orders BETWEEN 7 AND 10 THEN 4
            ELSE 5
        END AS order_frequency_score
    FROM base
),

aov_scored AS (
    SELECT *,
        CASE
            WHEN avg_order_value < 80 THEN 1
            WHEN avg_order_value < 160 THEN 2
            WHEN avg_order_value < 300 THEN 3
            WHEN avg_order_value < 600 THEN 4
            ELSE 5
        END AS avg_order_value_score
    FROM order_frequency
),

clv_scored AS (
    SELECT *,
        CASE
            WHEN clv < 80 THEN 1
            WHEN clv < 170 THEN 2
            WHEN clv < 500 THEN 3
            WHEN clv < 1200 THEN 4
            ELSE 5
        END AS clv_score
    FROM aov_scored
),

interval_scored AS (
    SELECT *,
        CASE
            WHEN nb_orders = 1 OR avg_days_between_orders IS NULL THEN 1
            WHEN avg_days_between_orders < 20 THEN 5
            WHEN avg_days_between_orders < 40 THEN 4
            WHEN avg_days_between_orders < 80 THEN 3
            WHEN avg_days_between_orders < 150 THEN 2
            ELSE 1
        END AS order_interval_score
    FROM clv_scored
),

recency_scored AS (
    SELECT *,
        CASE
            WHEN recency_days <= 30 THEN 5
            WHEN recency_days <= 90 THEN 4
            WHEN recency_days <= 180 THEN 3
            WHEN recency_days <= 365 THEN 2
            ELSE 1
        END AS recency_score
    FROM interval_scored
),


customer_total_scored AS (
    SELECT *,
    ((order_frequency_score 
     + avg_order_value_score 
     + clv_score 
     + order_interval_score 
     + recency_score)/5) AS customer_total_score,
     FROM recency_scored
)


SELECT
    customer_unique_id,
    nb_orders,
    clv,
    avg_order_value,
    avg_days_between_orders,
    customer_lifetime_days,
    recency_days,

    order_frequency_score,
    avg_order_value_score,
    clv_score,
    order_interval_score,
    recency_score,
    customer_total_score,

     CASE
    WHEN customer_total_score >= 4.2 THEN 'VIP'
    WHEN customer_total_score >= 3.5 THEN 'Loyal'
    WHEN customer_total_score >= 2.8 THEN 'High Potential'
    WHEN customer_total_score >= 2.0 THEN 'Promising'
    WHEN customer_total_score >= 1.4 THEN 'At Risk'
    ELSE 'Churned'
END AS customer_segment

FROM customer_total_scored