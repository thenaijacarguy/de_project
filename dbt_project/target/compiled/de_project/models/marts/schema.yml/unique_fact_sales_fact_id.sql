
    
    

select
    fact_id as unique_field,
    count(*) as n_records

from "warehouse"."marts"."fact_sales"
where fact_id is not null
group by fact_id
having count(*) > 1


