
    
    



select close_price
from `stock-pipeline-dbt-cloud`.`stock_data`.`stg_stock_prices`
where close_price is null


