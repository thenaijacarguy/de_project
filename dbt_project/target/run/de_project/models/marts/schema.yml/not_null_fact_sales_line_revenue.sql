select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select line_revenue
from "warehouse"."marts"."fact_sales"
where line_revenue is null



      
    ) dbt_internal_test