"""
Dynamic Dukascopy API Ingestion Module (Medallion Bronze Layer).

This module programmatically fetches historical tick/OHLC data from 
Dukascopy using the open-source `duka` API wrapper. It standardizes 
the resulting dataset, calculates initial Machine Learning targets 
(like pip volatility), and dynamically partitions the data by year 
to align with an S3 Hive-partitioned Lakehouse architecture.

Functions:
    load_config: Loads the central YAML configuration.
    fetch_dukascopy_data: Executes the duka API call and retrieves the generated CSV.
    clean_partition_and_upload: Formats the data, slices it by year, and streams to S3.
    main: Orchestrates the pipeline across multiple currencies.
"""

import os
import io
import glob
import subprocess
import pandas as pd
import boto3
import yaml
from dotenv import load_dotenv
from loguru import logger

# Load AWS Credentials from .env file
load_dotenv()

# ==========================================
# Professional Logger Configuration
# ==========================================
os.makedirs("logs", exist_ok=True)
logger.add(
    "logs/ingest_dukascopy_{time:YYYY-MM-DD}.log", 
    rotation="10 MB",     # Create a new file if it gets larger than 10MB
    retention="30 days",  # Automatically delete logs older than a month
    level="INFO",         # Only log INFO, SUCCESS, WARNING, and ERROR
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)

def load_config() -> dict:
    """
    Load the pipeline configuration from the central YAML file.

    Returns:
        dict: The parsed configuration containing date ranges, 
              currencies, and AWS settings.
    """
    with open("config/pipeline_config.yaml", "r") as file:
        return yaml.safe_load(file)

def fetch_dukascopy_data(pair: str, start: str, end: str, timeframe: str) -> str:
    logger.info(f"Initiating API request to Dukascopy for {pair} ({start} to {end})")
    
    output_dir = "src/data/raw"
    os.makedirs(output_dir, exist_ok=True)
    
    # Clean output directory before running
    for f in glob.glob(os.path.join(output_dir, "*.csv")):
        os.remove(f)
    
    # ADDED: "--header" flag to ensure Pandas always has columns to parse
    command = [
        "duka", pair, 
        "-s", start, 
        "-e", end, 
        "-c", timeframe, 
        "--header", 
        "-f", output_dir
    ]
    
    try:
        # We add timeout=600 (10 mins) to ensure it doesn't hang indefinitely
        subprocess.run(command, check=True, capture_output=True, text=True, timeout=600)
        
        downloaded_files = glob.glob(os.path.join(output_dir, "*.csv"))
        if not downloaded_files:
            raise FileNotFoundError(f"Duka ran successfully but no CSV was found in {output_dir}")
            
        return downloaded_files[0]
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Dukascopy API Error: {e.stderr}")
        raise

def clean_partition_and_upload(file_path: str, pair: str, config: dict) -> None:
    logger.info(f"Cleaning and partitioning data for {pair}...")
    
    try:
        # If the file is 0 bytes, this will safely catch it
        df = pd.read_csv(file_path)
    except pd.errors.EmptyDataError:
        logger.error(f"The downloaded file for {pair} is completely empty. Dukascopy returned no data.")
        os.remove(file_path)
        return

    # If the file only has headers but no rows, skip it
    if df.empty:
        logger.warning(f"The downloaded file for {pair} has headers but no data rows. Skipping.")
        os.remove(file_path)
        return

    df.columns = [col.strip().lower() for col in df.columns]
    
    time_col = next((col for col in df.columns if 'time' in col), None)
    if time_col:
        df['datetime_utc'] = pd.to_datetime(df[time_col], utc=True)
        df = df.drop(columns=[time_col])
    else:
        logger.error(f"Failed to find time column for {pair}. Aborting partition.")
        return

    if 'high' in df.columns and 'low' in df.columns:
        df['volatility_pips'] = (df['high'] - df['low']) * 10000

    df['year'] = df['datetime_utc'].dt.year

    bucket = config["aws"]["s3_bucket"]
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
    region = os.getenv("AWS_DEFAULT_REGION", "eu-north-1").strip()

    s3_client = boto3.client(
        's3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region
    )
    
    for year in df['year'].unique():
        year_df = df[df['year'] == year].drop(columns=['year'])
        s3_key = f"bronze/dukascopy/pair={pair}/year={year}/raw_4h.csv"
        
        csv_buffer = io.StringIO()
        year_df.to_csv(csv_buffer, index=False)

        s3_client.put_object(Bucket=bucket, Key=s3_key, Body=csv_buffer.getvalue())
        logger.success(f"Successfully uploaded partition: s3://{bucket}/{s3_key}")
    
    if os.path.exists(file_path):
        os.remove(file_path)

def main():
    """
    Orchestrate the dynamic Dukascopy ingestion pipeline.

    Reads target parameters from the configuration and iteratively 
    fetches, cleans, and uploads data for each specified currency pair.
    """
    config = load_config()
    start_date = config["data_range"]["start_date"]
    end_date = config["data_range"]["end_date"]
    timeframe = config["assets"]["timeframe"]
    
    for currency in config["assets"]["currencies"]:
        try:
            csv_path = fetch_dukascopy_data(currency, start_date, end_date, timeframe)
            clean_partition_and_upload(csv_path, currency, config)
        except Exception as e:
            logger.error(f"Failed to process {currency}: {e}")

if __name__ == "__main__":
    main()