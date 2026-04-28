/* @bruin
name: feature_engineering_gold
type: athena.sql
depends:
  - create_athena_tables
@bruin */

CREATE OR REPLACE VIEW forex_lakehouse_db.gold_ml_features AS
SELECT 
    n.datetime_utc AS event_time,
    n.title AS event_name,
    n.country,
    n.currency,
    n.impact,
    n.actual,
    n.forecast,
    n.surprise_factor,
    p.pair,
    p.open AS candle_open,
    p.close AS candle_close,
    (p.high - p.low) AS candle_volatility,
    (p.close - p.open) AS price_change_label
FROM forex_lakehouse_db.forex_calendar n
JOIN forex_lakehouse_db.price_action p
  ON (n.currency = SUBSTRING(p.pair, 1, 3) OR n.currency = SUBSTRING(p.pair, 4, 3))
  -- Match news to the specific hourly candle it fell inside of
  AND date_trunc('hour', n.datetime_utc) = p.timestamp
WHERE 
    (lower(n.impact) LIKE '%high%' OR lower(n.impact) LIKE '%medium%')
    AND n.actual IS NOT NULL;