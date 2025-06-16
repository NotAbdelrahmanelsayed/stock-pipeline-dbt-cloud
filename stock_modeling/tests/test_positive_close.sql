SELECT 
    *
FROM
    {{ ref("stg_stock_prices") }}
WHERE 
    close_price <= 0