SELECT 
    *
FROM
    {{ ref("stg_stock_prices") }}
WHERE 
    volume < 1