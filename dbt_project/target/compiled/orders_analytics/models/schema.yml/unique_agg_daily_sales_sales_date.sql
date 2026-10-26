
    
    

select
    sales_date as unique_field,
    count(*) as n_records

from "warehouse"."main"."agg_daily_sales"
where sales_date is not null
group by sales_date
having count(*) > 1


