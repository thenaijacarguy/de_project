
    
    

select
    item_id as unique_field,
    count(*) as n_records

from "warehouse"."staging"."stg_order_items"
where item_id is not null
group by item_id
having count(*) > 1


