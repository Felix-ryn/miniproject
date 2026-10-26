

SELECT
    CAST(order_ts AS DATE) AS sales_date,

    COUNT(*) AS total_orders,

    SUM(quantity) AS total_quantity,

    SUM(
        CASE
            WHEN status IN ('paid', 'shipped')
            THEN gross_amount
            ELSE 0
        END
    ) AS gross_sales,

    SUM(
        CASE
            WHEN status IN ('paid', 'shipped')
            THEN net_amount
            ELSE 0
        END
    ) AS net_sales

FROM "warehouse"."main"."fct_orders"

GROUP BY CAST(order_ts AS DATE)