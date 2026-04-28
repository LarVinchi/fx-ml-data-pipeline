"""@bruin
name: ingest_dukascopy
type: python
@bruin"""

import os
import io
import sys
import yaml
import subprocess
import pandas as pd
import boto3
import datetime
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from loguru import logger

# --- THE PIVOT: USING YFINANCE ---
def ensure_package(pkg_name):
    try:
        __import__(pkg_name)
    except ImportError:
        logger.info(f"📦 Installing {pkg_name}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg_name])

ensure_package("yfinance")
import yfinance as yf

load_dotenv()

def load_config() -> dict:
    config_path = os.path.join(os.path.dirname(__file__), "../../../config/pipeline_config.yaml")
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

def fetch_price_data(pair: str, year: int) -> pd.DataFrame:
    """Fetches hourly data using Yahoo Finance instead of the broken duka library."""
    # Map EURUSD to EURUSD=X for Yahoo Finance
    yf_pair = f"{pair}=X"
    
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    
    logger.info(f"📥 Fetching {pair} for {year} via yfinance...")
    
    try:
        ticker = yf.Ticker(yf_pair)
        # 1h interval is restricted to the last 730 days.
        df = ticker.history(start=start_date, end=end_date, interval="1h")
        
        if df.empty:
            return pd.DataFrame()
        
        # Standardize schema to match what the pipeline expects
        df = df.reset_index()
        # Convert timezone-aware Datetime to naive UTC
        df['timestamp'] = pd.to_datetime(df['Datetime'], utc=True).dt.tz_localize(None)
        
        df = df.rename(columns={
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        })
        
        return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        
    except Exception as e:
        logger.error(f"yfinance execution error for {pair} {year}: {e}")
        return pd.DataFrame()

def get_target_years(start_str: str, end_str: str) -> list:
    target_year = os.getenv("TARGET_YEAR")
    if target_year:
        return [int(target_year)]
    start_year = pd.to_datetime(start_str).year
    end_year = pd.to_datetime(end_str).year
    return list(range(start_year, end_year + 1))

def main():
    logger.info("🚀 Starting Bronze Price Ingestion (Powered by yfinance)...")
    
    config = load_config()
    bucket = os.getenv("S3_BUCKET_NAME")
    s3_client = boto3.client('s3')
    
    pairs = config.get("pairs", ["EURUSD", "GBPUSD"])
    timeframe = config.get("timeframe", "h1")
    bronze_prefix = config.get("bronze_prefix", "bronze/")
    
    target_years = get_target_years(
        config.get("start_date", "2023-01-01"),
        config.get("end_date", "2026-12-31")
    )

    for pair in pairs:
        for year in target_years:
            s3_key = f"{bronze_prefix}dukascopy/pair={pair}/year={year}/raw_{timeframe}.csv"
            
            # Idempotency check
            try:
                obj = s3_client.head_object(Bucket=bucket, Key=s3_key)
                if obj["ContentLength"] > 5000:
                    logger.info(f"⏭️ Skipping {pair} {year} (Valid file exists)")
                    continue
            except ClientError:
                pass 
            
            try:
                raw_df = fetch_price_data(pair, year)
                
                if raw_df is None or raw_df.empty:
                    logger.warning(f"⚠️ No data available for {pair} in {year} (Likely outside 730-day limit).")
                    continue
                
                csv_buffer = io.StringIO()
                raw_df.to_csv(csv_buffer, index=False)
                
                s3_client.put_object(
                    Bucket=bucket, 
                    Key=s3_key, 
                    Body=csv_buffer.getvalue()
                )
                logger.success(f"✅ Successfully uploaded {pair} ({year}) to Bronze.")
                
            except Exception as e:
                logger.error(f"❌ Critical failure for {pair} ({year}): {e}")

if __name__ == "__main__":
    main()