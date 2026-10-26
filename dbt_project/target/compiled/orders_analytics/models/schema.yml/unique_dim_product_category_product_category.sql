
    
    

select
    product_category as unique_field,
    count(*) as n_records

from "warehouse"."main"."dim_product_category"
where product_category is not null
group by product_category
having count(*) > 1


