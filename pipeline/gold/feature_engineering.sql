-- @bruin.asset
-- name: feature_engineering_gold
-- type: athena
-- depends:
--   - process_dukascopy
--   - process_forexfactory

/* Gold Layer Transformation: 
  Joins Macroeconomic News Events with 4H Price Action Candles.
  This CTAS (Create Table As Select) statement generates the final dataset 
  for the Machine Learning Model and Streamlit Dashboard.
*/

CREATE TABLE IF NOT EXISTS forex_lakehouse_db.gold_ml_features
WITH (
  format = 'PARQUET',
  external_location = 's3://forex-datalake-bucket/gold/ml_features/',
  partitioned_by = ARRAY['year']
) AS
SELECT 
    n.event_id,
    n.datetime_utc AS news_release_time,
    n.currency AS news_currency,
    n.impact,
    n.event_name,
    n.actual,
    n.forecast,
    n.surprise_factor,
    p.pair AS traded_pair,
    p.timestamp AS candle_open_time,
    p.open,
    p.high,
    p.low,
    p.close,
    p.candle_volatility,
    p.year
FROM forex_lakehouse_db.forex_calendar n
JOIN forex_lakehouse_db.price_action p
  -- Time Join: The news event must fall within the 4H candle's open and close
  ON n.datetime_utc >= p.timestamp 
  AND n.datetime_utc < p.timestamp + interval '4' hour
  -- Currency Join: Ensure the news currency affects the traded pair (e.g., USD news affects EURUSD)
  AND p.pair LIKE '%' || n.currency || '%'
WHERE n.impact IN ('High Impact Expected', 'Medium Impact Expected')
  -- Ensure we only train on data where actual numbers were reported
  AND n.actual IS NOT NULL;