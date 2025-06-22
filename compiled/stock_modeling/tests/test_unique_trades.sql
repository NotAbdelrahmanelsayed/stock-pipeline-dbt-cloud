-- Test to ensure uniqueness of trades per trade_date and ticker.
-- Returns zero rows if unique, or problematic duplicates otherwise.

SELECT 
    trade_date,
    ticker,
    COUNT(*) AS duplicate_count
FROM
    `stock-pipeline-dbt-cloud`.`stock_data`.`stg_stock_prices`
GROUP BY 
    trade_date,
    ticker
HAVING
    COUNT(*) > 1
ORDER BY
    duplicate_count DESC, trade_date, ticker