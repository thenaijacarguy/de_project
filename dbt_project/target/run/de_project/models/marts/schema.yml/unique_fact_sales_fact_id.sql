select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    fact_id as unique_field,
    count(*) as n_records

from "warehouse"."marts"."fact_sales"
where fact_id is not null
group by fact_id
having count(*) > 1



      
    ) dbt_internal_test