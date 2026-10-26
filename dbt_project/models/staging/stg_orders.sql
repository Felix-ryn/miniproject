{{ config(materialized='view') }}

SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(product_category AS VARCHAR) AS product_category,
    CAST(payment_method AS VARCHAR) AS payment_method,
    CAST(order_ts AS TIMESTAMPTZ) AS order_ts,
    CAST(quantity AS INTEGER) AS quantity,
    CAST(unit_price AS DECIMAL(12, 2)) AS unit_price,
    CAST(discount_pct AS DECIMAL(5, 4)) AS discount_pct,
    CAST(status AS VARCHAR) AS status,

    CAST(quantity AS DECIMAL(12, 2))
        * CAST(unit_price AS DECIMAL(12, 2)) AS gross_amount,

    CAST(quantity AS DECIMAL(12, 2))
        * CAST(unit_price AS DECIMAL(12, 2))
        * (1 - CAST(discount_pct AS DECIMAL(5, 4))) AS net_amount

FROM read_parquet(
    'C:/Users/USER/Downloads/data_processing_miniproject_lengkap/data_processing_miniproject/data/valid/orders.parquet'
)