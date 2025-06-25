WITH base AS (
    SELECT 
        trade_date,
        ticker,
        close_price,
        LAG(close_price) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
        ) AS prev_day_close_price
    FROM 
        `stock-pipeline-dbt-cloud`.`stock_data`.`stg_stock_prices`
    
),

returns AS (
    SELECT 
    trade_date,
    ticker,
    close_price,
    SAFE_DIVIDE(close_price - prev_day_close_price, prev_day_close_price) AS daily_return,
FROM
    base
)

SELECT 
    *,
    AVG(daily_return) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_return_7d,
    STDDEV(daily_return) OVER(
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS rolling_volatility_30d

FROM 
    returns