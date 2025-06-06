SELECT 
    *
FROM 
    {{ source('stock_data', 'raw_stock_prices') }}

LIMIT 10;