SELECT 
    *
FROM
    `stock-pipeline-dbt-cloud`.`stock_data`.`stg_stock_prices`
WHERE 
    volume < 1