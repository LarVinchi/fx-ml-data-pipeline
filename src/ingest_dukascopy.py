"""
Dukascopy Native API Ingestion & Medallion Transformation Module.

This module orchestrates the extraction of historical Forex price data using the 
`dukascopy-python` library. It implements a multi-stage data processing pipeline:
1. Extraction: Fetches 1-Hour BID-side OHLC data directly into memory.
2. Transformation: Resamples 1-Hour candles into 4-Hour candles using Pandas.
3. Feature Engineering: Calculates technical indicators (e.g., Pip Volatility).
4. Loading: Implements idempotent writes to AWS S3, partitioning data by currency 
   pair and year for both Bronze (Raw CSV) and Silver (Optimized Parquet) layers.

This architecture supports 'Zero-ETL' querying via Amazon Athena and provides 
an immutable record of raw API responses.
"""

import os
import io
import pandas as pd
import boto3
import yaml
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger
import dukascopy_python
from dukascopy_python.instruments import (
    INSTRUMENT_FX_MAJORS_EUR_USD, 
    INSTRUMENT_FX_MAJORS_GBP_USD
)

# Load AWS Credentials
load_dotenv()

# Logger Configuration
os.makedirs("logs", exist_ok=True)
logger.add(
    "logs/ingest_dukascopy_{time:YYYY-MM-DD}.log", 
    rotation="10 MB", 
    retention="30 days", 
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)

# Instrument Mapping
INSTRUMENT_MAP = {
    "EURUSD": INSTRUMENT_FX_MAJORS_EUR_USD,
    "GBPUSD": INSTRUMENT_FX_MAJORS_GBP_USD
}

def load_config() -> dict:
    """
    Load the central pipeline configuration.

    Returns:
        dict: Parsed YAML configuration containing assets and S3 parameters.
    """
    with open("config/pipeline_config.yaml", "r") as file:
        return yaml.safe_load(file)

def fetch_and_process(pair: str, start_str: str, end_str: str, config: dict) -> None:
    """
    Execute the E-T-L cycle for a specific currency pair.

    Fetches 1H data, aggregates to 4H, calculates volatility, and 
    synchronizes the S3 Bronze and Silver layers.

    Args:
        pair (str): Target currency pair (e.g., 'EURUSD').
        start_str (str): Start date in YYYY-MM-DD format.
        end_str (str): End date in YYYY-MM-DD format.
        config (dict): Configuration dictionary for S3 and asset metadata.
    """
    logger.info(f"🚀 Starting ingestion for {pair} ({start_str} to {end_str})")
    
    # 1. Fetch from API
    start_dt = datetime.strptime(start_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_str, "%Y-%m-%d")
    
    try:
        df = dukascopy_python.fetch(
            instrument=INSTRUMENT_MAP[pair],
            interval=dukascopy_python.INTERVAL_HOUR_1,
            offer_side=dukascopy_python.OFFER_SIDE_BID,
            start=start_dt, 
            end=end_dt
        )
    except Exception as e:
        logger.error(f"API Fetch failed for {pair}: {e}")
        return

    if df is None or df.empty:
        logger.warning(f"No data returned for {pair}")
        return

    # 2. Resampling Engine (1H -> 4H)
    df.columns = [str(col).lower() for col in df.columns]
    agg_dict = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'}
    
    # Identify volume column dynamically (bidvolume vs volume)
    vol_col = next((c for c in df.columns if 'volume' in c), None)
    if vol_col:
        agg_dict[vol_col] = 'sum'
    
    df_4h = df.resample('4H').agg(agg_dict).dropna()
    df_4h.index.name = 'datetime_utc'
    df_4h = df_4h.reset_index()
    
    # Feature Engineering & Partitioning
    df_4h['volatility_pips'] = (df_4h['high'] - df_4h['low']) * 10000
    df_4h['year'] = df_4h['datetime_utc'].dt.year

    # 3. Medallion Upload to S3
    s3 = boto3.client('s3', region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1"))
    bucket = config["aws"]["s3_bucket"]

    for year in df_4h['year'].unique():
        year_df = df_4h[df_4h['year'] == year].drop(columns=['year'])
        
        # --- BRONZE LAYER (CSV) ---
        bronze_key = f"bronze/dukascopy/pair={pair}/year={year}/raw_4h.csv"
        csv_buf = io.StringIO()
        year_df.to_csv(csv_buf, index=False)
        s3.put_object(Bucket=bucket, Key=bronze_key, Body=csv_buf.getvalue())
        
        # --- SILVER LAYER (Parquet) ---
        silver_key = f"silver/price_action/pair={pair}/year={year}/data_4h.parquet"
        pq_buf = io.BytesIO()
        year_df.to_parquet(pq_buf, engine='pyarrow', index=False)
        s3.put_object(Bucket=bucket, Key=silver_key, Body=pq_buf.getvalue())
        
        logger.success(f"✅ {pair} {year}: Bronze & Silver layers synchronized.")

def main():
    """Entry point for the Dukascopy ingestion pipeline."""
    config = load_config()
    for currency in config["assets"]["currencies"]:
        fetch_and_process(
            currency, 
            config["data_range"]["start_date"], 
            config["data_range"]["end_date"], 
            config
        )

if __name__ == "__main__":
    main()