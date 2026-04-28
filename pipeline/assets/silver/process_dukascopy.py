"""@bruin
name: process_dukascopy
type: python
depends:
  - ingest_dukascopy
@bruin"""

import os
import io
import yaml
import pandas as pd
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from loguru import logger

load_dotenv()

def load_config() -> dict:
    """Loads centralized pipeline configuration from the config directory."""
    config_path = os.path.join(os.path.dirname(__file__), "../../../config/pipeline_config.yaml")
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

def main():
    logger.info("⚙️ Processing Price Action (Silver Layer)...")
    config = load_config()
    bucket = os.getenv("S3_BUCKET_NAME")
    s3_client = boto3.client('s3')
    
    # Get configuration settings
    pairs = config['dukascopy']['currencies']
    timeframe = config['dukascopy'].get('timeframe', 'h1')
    target_year = os.getenv("TARGET_YEAR")

    if not target_year:
        logger.error("❌ TARGET_YEAR environment variable not set. Skipping execution.")
        return

    for pair in pairs:
        # Hive-style partitioning: essential for Athena's MSCK REPAIR to work
        bronze_key = f"bronze/dukascopy/pair={pair}/year={target_year}/raw_{timeframe}.csv"
        silver_key = f"silver/price_action/pair={pair}/year={target_year}/data.parquet"

        # Idempotency: Skip if Silver Parquet already exists
        try:
            s3_client.head_object(Bucket=bucket, Key=silver_key)
            logger.info(f"⏭️ Silver data for {pair} ({target_year}) already exists. Skipping.")
            continue
        except:
            pass

        try:
            logger.info(f"🔄 Transforming {pair} for {target_year}...")
            
            # 1. Fetch Bronze Data
            response = s3_client.get_object(Bucket=bucket, Key=bronze_key)
            df = pd.read_csv(io.BytesIO(response['Body'].read()))

            if df.empty:
                logger.warning(f"⚠️ {pair} ({target_year}) CSV is empty.")
                continue

            # 2. Standardize Columns (Handles case sensitivity and different headers)
            df.columns = [col.lower().strip() for col in df.columns]
            
            # Ensure 'timestamp' exists; if named 'local time' or 'date', rename it
            if 'local time' in df.columns:
                df.rename(columns={'local time': 'timestamp'}, inplace=True)
            elif 'date' in df.columns:
                df.rename(columns={'date': 'timestamp'}, inplace=True)

            # 3. Data Cleaning & Feature Engineering
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            
            # Calculate volatility (High - Low)
            if 'high' in df.columns and 'low' in df.columns:
                df['candle_volatility'] = df['high'] - df['low']
            else:
                logger.error(f"❌ Missing OHLC columns in {bronze_key}")
                continue

            # 4. Final Formatting
            # Only keep columns we want in the Lakehouse
            cols_to_keep = ['timestamp', 'open', 'high', 'low', 'close', 'candle_volatility']
            df = df[cols_to_keep]

            # 5. Export to Silver Parquet
            buffer = io.BytesIO()
            df.to_parquet(buffer, engine='pyarrow', index=False, compression='snappy')
            s3_client.put_object(Bucket=bucket, Key=silver_key, Body=buffer.getvalue())
            
            logger.success(f"✅ Created Silver Price Action: {pair} ({target_year})")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.debug(f"Bronze file not found: {bronze_key} (Normal if data doesn't exist yet).")
            else:
                logger.error(f"S3 Error: {e}")
        except Exception as e:
            logger.error(f"❌ Transformation failed for {pair} ({target_year}): {e}")

if __name__ == "__main__":
    main()