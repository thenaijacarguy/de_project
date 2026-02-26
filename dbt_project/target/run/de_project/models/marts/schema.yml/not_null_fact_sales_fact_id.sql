select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select fact_id
from "warehouse"."marts"."fact_sales"
where fact_id is null



      
    ) dbt_internal_test