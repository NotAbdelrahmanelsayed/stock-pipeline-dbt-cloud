

WITH base AS (
    SELECT 
        trade_date,
        ticker,
        open_price,
        close_price,
        high_price,
        low_price,
        volume
    FROM 
        `stock-pipeline-dbt-cloud`.`stock_data`.`stg_stock_prices`
    
)
SELECT 
    *,
    AVG(close_price) OVER (
        PARTITION BY ticker 
        ORDER BY trade_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS close_price_ma_7d,

    AVG(close_price) OVER (
        PARTITION BY ticker 
        ORDER BY trade_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS close_price_ma_30d,

    close_price - LAG(close_price) OVER (
        PARTITION BY ticker 
        ORDER BY trade_date
        ) AS close_price_daily_diff,

    SUM(volume) OVER (
        PARTITION BY ticker 
        ORDER BY trade_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS volume_rolling_sum_7d,
    
    SUM(volume) OVER (
        PARTITION BY ticker 
        ORDER BY trade_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS volume_rolling_sum_30d
FROM 
    base



WHERE trade_date >= (
    SELECT 
        DATE_SUB(MAX(_dbt_max_partition), INTERVAL 1 MONTH)
    FROM 
        `stock-pipeline-dbt-cloud`.`stock_data`.`fact_stock_prices`
)

