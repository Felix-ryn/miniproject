

SELECT DISTINCT
    customer_id
FROM "warehouse"."main"."stg_orders"
WHERE customer_id IS NOT NULL