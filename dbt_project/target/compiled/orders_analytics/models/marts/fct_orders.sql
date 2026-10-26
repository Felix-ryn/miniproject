

SELECT
    o.order_id,
    o.customer_id,
    o.product_category,
    o.order_ts,
    o.payment_method,
    o.status,
    o.quantity,
    o.unit_price,
    o.discount_pct,
    o.gross_amount,
    o.net_amount

FROM "warehouse"."main"."stg_orders" AS o

INNER JOIN "warehouse"."main"."dim_customer" AS c
    ON o.customer_id = c.customer_id

INNER JOIN "warehouse"."main"."dim_product_category" AS p
    ON o.product_category = p.product_category