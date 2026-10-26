

SELECT DISTINCT
    product_category
FROM "warehouse"."main"."stg_orders"
WHERE product_category IS NOT NULL