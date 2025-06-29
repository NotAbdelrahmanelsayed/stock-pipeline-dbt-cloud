{{
    config(
        materialized='incremental',
        unique_key=["trade_date", "ticker"],
        partition_by={ 'field': 'trade_date', 'data_type': 'date'},
        incremental_strategy='insert_overwrite',
    )
}}

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
        {{ ref("stg_stock_prices") }}
    ORDER BY 
        trade_date
    
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

{% if is_incremental() %}

WHERE trade_date >= (
    SELECT 
        DATE_SUB(MAX(_dbt_max_partition), INTERVAL 1 MONTH)
    FROM 
        {{this}}
)

{% endif %}
