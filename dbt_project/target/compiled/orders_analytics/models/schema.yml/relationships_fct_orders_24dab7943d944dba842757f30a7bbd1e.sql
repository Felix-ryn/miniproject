
    
    

with child as (
    select product_category as from_field
    from "warehouse"."main"."fct_orders"
    where product_category is not null
),

parent as (
    select product_category as to_field
    from "warehouse"."main"."dim_product_category"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


