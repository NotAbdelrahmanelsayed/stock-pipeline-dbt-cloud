SELECT 
    Date AS trade_date,
    Ticker AS ticker,
    Open AS open_price,
    High AS high_price,
    low AS low_price,
    Close AS close_price,
    Volume AS volume
FROM 
    {{ source('stock_data', 'raw_stock_prices') }}
WHERE
    Ticker IS NOT NULL 
    AND Volume > 0 
    AND Close > 0
ORDER BY 
    trade_date