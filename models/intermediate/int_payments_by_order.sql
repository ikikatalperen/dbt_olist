{{ config( materialized = 'view' ) }}

WITH payments AS (
    SELECT
        order_id,
        payment_type,
        payment_installments,
        payment_value
    FROM {{ ref('stg_raw__order_payments_dataset') }}
),

agg AS (
    SELECT
        order_id,
        SUM(payment_value) AS total_order_value,
        SUM(CASE WHEN payment_type = 'voucher' THEN payment_value ELSE 0 END) AS coupon_value,
        SUM(CASE WHEN payment_type <> 'voucher' THEN payment_value ELSE 0 END) AS non_coupon_value,
        SUM(CASE WHEN payment_type = 'credit_card' THEN payment_value ELSE 0 END) AS credit_card_value,
        SUM(CASE WHEN payment_type = 'boleto'      THEN payment_value ELSE 0 END) AS boleto_value,
        SUM(CASE WHEN payment_type = 'debit_card'  THEN payment_value ELSE 0 END) AS debit_card_value,
        SUM(CASE WHEN payment_type = 'not_defined' THEN payment_value ELSE 0 END) AS not_defined_value,
        COUNT(*) AS nb_payments,
        MAX(payment_installments) AS max_installments,
        MAX(CASE WHEN payment_type = 'credit_card' THEN payment_installments END) AS credit_card_installments
    FROM payments
    GROUP BY order_id
)

SELECT *
FROM agg