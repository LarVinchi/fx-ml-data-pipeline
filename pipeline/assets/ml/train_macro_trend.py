"""@bruin
name: train_macro_trend
type: python
depends:
  - feature_engineering_gold
@bruin"""

import os
import time
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import yaml

# --- Dependency Handling ---
try:
    import joblib
    import xgboost as xgb
    import awswrangler as wr
except ModuleNotFoundError:
    import subprocess
    import sys
    logger.info("📦 Required ML libraries missing. Initializing auto-install...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "joblib", "xgboost", "awswrangler", "scikit-learn"])
    import joblib
    import xgboost as xgb
    import awswrangler as wr

load_dotenv()

def load_config() -> dict:
    """Loads pipeline configuration for metadata and S3 paths."""
    config_path = os.path.join(os.path.dirname(__file__), "../../../config/pipeline_config.yaml")
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

def get_query() -> str:
    """
    Returns the SQL query aligned with the Gold View Schema.
    """
    return """
        SELECT 
            pair,
            impact, 
            surprise_factor, 
            event_time, 
            candle_open, 
            candle_close,
            price_change_label
        FROM forex_lakehouse_db.gold_ml_features
        ORDER BY event_time ASC
    """

def process_macro_features(df: pd.DataFrame) -> pd.DataFrame:
    """Transforms high-frequency data into Monthly Macro Snapshots."""
    df['event_time'] = pd.to_datetime(df['event_time'])
    df['year_month'] = df['event_time'].dt.to_period('M')

    # Mapping lowercase impacts from silver layer to numeric weights
    impact_mapping = {'high': 2, 'medium': 1, 'low': 0}
    df['impact_encoded'] = df['impact'].str.lower().map(impact_mapping).fillna(0)

    # Monthly Resampling
    monthly = df.groupby(['pair', 'year_month']).agg(
        total_surprise=('surprise_factor', 'sum'),
        avg_surprise=('surprise_factor', 'mean'),
        high_impact_count=('impact_encoded', lambda x: (x == 2).sum()),
        first_open=('candle_open', 'first'),
        last_close=('candle_close', 'last'),
        total_move=('price_change_label', 'sum')
    ).reset_index()

    # Feature: Current Month Price Trend
    monthly['this_month_trend'] = monthly['last_close'] - monthly['first_open']

    # Target: Prediction for the NEXT month's trend
    monthly['target_next_month_trend'] = monthly.groupby('pair')['this_month_trend'].shift(-1)
    
    return monthly

def main():
    logger.info("🤖 Initializing Automated Macro Training Asset...")
    bucket = os.getenv("S3_BUCKET_NAME")
    target_year = os.getenv("TARGET_YEAR")
    
    if not target_year:
        logger.error("TARGET_YEAR environment variable not set.")
        return

    # Wait for Athena metadata propagation
    time.sleep(5)

    # Idempotency check
    s3_forecast_path = f"s3://{bucket}/gold/predictions/year={target_year}/live_forecast.parquet"
    if wr.s3.does_object_exist(s3_forecast_path):
        logger.info(f"⏭️ AI Training for {target_year} already exists. Skipping.")
        return

    # Data Ingestion
    try:
        df = wr.athena.read_sql_query(sql=get_query(), database="forex_lakehouse_db")
    except Exception as e:
        logger.error(f"Athena connection failed: {e}")
        return

    if df.empty:
        logger.warning("No data found in Gold View. Verify that TARGET_YEAR matches your S3 data.")
        return

    # ML Pipeline
    monthly_df = process_macro_features(df)
    train_set = monthly_df.dropna(subset=['target_next_month_trend'])
    live_set = monthly_df[monthly_df['target_next_month_trend'].isna()]

    if train_set.empty:
        logger.warning("Insufficient history for training. Need at least 2 months of data.")
        return

    features = ['total_surprise', 'avg_surprise', 'high_impact_count', 'this_month_trend']
    X = train_set[features]
    y = train_set['target_next_month_trend']

    logger.info(f"Training XGBoost on {len(train_set)} macro-months...")
    model = xgb.XGBRegressor(n_estimators=100, learning_rate=0.05, max_depth=4)
    model.fit(X, y)

    # Export
    os.makedirs("models", exist_ok=True)
    joblib.dump(model, f"models/xgboost_macro_{target_year}.pkl")

    if not live_set.empty:
        prediction = model.predict(live_set[features])
        forecast_df = pd.DataFrame({
            'pair': live_set['pair'],
            'forecast_generated_at': [datetime.utcnow()] * len(live_set),
            'current_month': live_set['year_month'].astype(str),
            'predicted_next_month_trend': prediction
        })
        wr.s3.to_parquet(df=forecast_df, path=s3_forecast_path, index=False)
        logger.success(f"✅ Prediction Registry Updated: {s3_forecast_path}")

if __name__ == "__main__":
    main()