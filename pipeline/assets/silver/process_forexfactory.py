"""
@bruin.asset
name: process_forexfactory
type: python
image: python:3.9
depends:
  - scrape_forexfactory
"""

import os
import io
import re
import yaml
import chompjs
import pandas as pd
import numpy as np
from datetime import datetime
import pytz
import boto3
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
import warnings
from loguru import logger

# Ignore bs4 warnings about URLs/markup
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

# --- Configuration Loader ---
def load_config() -> dict:
    """Loads the centralized pipeline configuration."""
    # Handle path resolution depending on where Bruin runs the script
    config_path = os.path.join(os.path.dirname(__file__), "../../../config/pipeline_config.yaml")
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

def get_months_in_range(start_str: str, end_str: str) -> list:
    """Generates a list of (year, month) tuples based on the config date range."""
    start_dt = pd.to_datetime(start_str)
    end_dt = pd.to_datetime(end_str)
    months = pd.date_range(start_dt, end_dt, freq='MS')
    return [(dt.year, dt.month) for dt in months]

# --- Cleaning Utilities ---
def clean_html(text: str) -> str:
    """Removes HTML tags from a string."""
    if pd.isna(text) or not isinstance(text, str):
        return ""
    text = text.strip()
    if "<" in text and ">" in text:
        return BeautifulSoup(text, "html.parser").get_text(separator=" ").strip()
    return text

def clean_financial_numeric(val) -> float:
    """Converts financial strings (K/M/B, percentages, fractions, votes) to floats."""
    if pd.isna(val) or val == "" or str(val).strip() == "":
        return np.nan
        
    val = str(val).strip().replace(',', '')
    
    # Bank Votes (e.g., '2-1-6' -> Net Hawkishness)
    if re.match(r'^\d+-\d+-\d+$', val):
        parts = val.split('-')
        return float(parts[0]) - float(parts[1])
        
    # Fractions (e.g., '4.09/2.6' -> Yield)
    if '/' in val:
        val = val.split('/')[0].strip()
        
    # Percentages
    if '%' in val:
        return float(val.replace('%', '')) / 100.0
        
    # Multipliers
    multipliers = {'K': 1e3, 'M': 1e6, 'B': 1e9, 'T': 1e12}
    last_char = str(val)[-1].upper()
    if last_char in multipliers:
        try:
            return float(val[:-1]) * multipliers[last_char]
        except ValueError:
            return np.nan
            
    try:
        return float(val)
    except ValueError:
        return np.nan

# --- Main Processor ---
def main():
    logger.info("Starting Silver Layer Transformation for Forex Factory...")
    config = load_config()
    
    bucket = config["aws"]["s3_bucket"]
    bronze_prefix = config["aws"]["bronze_prefix"]
    silver_prefix = config["aws"]["silver_prefix"]
    
    s3_client = boto3.client('s3', region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1"))
    
    target_months = get_months_in_range(config["data_range"]["start_date"], config["data_range"]["end_date"])
    
    eastern = pytz.timezone('US/Eastern')
    utc = pytz.UTC

    for year, month in target_months:
        month_str = str(month).zfill(2)
        
        bronze_key = f"{bronze_prefix}forex_factory/year={year}/month={month_str}/snippet.json"
        silver_key = f"{silver_prefix}forex_calendar/year={year}/month={month_str}/data.parquet"
        
        # --- IDEMPOTENCY CHECK ---
        try:
            s3_client.head_object(Bucket=bucket, Key=silver_key)
            logger.info(f"⏭️ Silver Parquet for {year}-{month_str} already exists. Skipping.")
            continue
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                logger.error(f"S3 Error checking {silver_key}: {e}")
                continue
                
        # --- PROCESS MISSING DATA ---
        try:
            # 1. Fetch Bronze JSON
            logger.info(f"Processing Bronze data for {year}-{month_str}...")
            obj_response = s3_client.get_object(Bucket=bucket, Key=bronze_key)
            raw_content = obj_response['Body'].read().decode('utf-8')
            
            # 2. Parse Javascript Object
            parsed_data = chompjs.parse_js_object(raw_content)
            
            records = []
            for day in parsed_data.get("days", []):
                for item in day.get("events", []):
                    records.append({
                        "event_id": item.get("id", ""),
                        "date": item.get("date", ""),
                        "time": item.get("timeLabel", ""),
                        "currency": item.get("currency", ""),
                        "country": item.get("country", ""),
                        "impact": item.get("impactTitle", ""), 
                        "event_name": clean_html(item.get("name", "")),
                        "actual": clean_html(item.get("actual", "")),
                        "forecast": clean_html(item.get("forecast", "")),
                        "previous": clean_html(item.get("previous", ""))
                    })
            
            if not records:
                logger.warning(f"No records extracted for {year}-{month_str}")
                continue
                
            df = pd.DataFrame(records)
            
            # 3. Filter for High/Medium Impact
            df = df[df["impact"].astype(str).str.contains("Medium|High", case=False, na=False)].copy()
            
            # 4. Advanced ML Formatting
            for col in ['actual', 'forecast', 'previous']:
                df[col] = df[col].apply(clean_financial_numeric)
            
            # Calculate Surprise Factor
            df['surprise_factor'] = df['actual'] - df['forecast']

            # 5. Timezone Alignment
            def parse_datetime(row):
                time_str = str(row['time']).strip().lower()
                if time_str in ['all day', 'day 1', 'day 2', 'tentative'] or pd.isna(time_str) or time_str == 'nan':
                    time_str = '12:00am'
                try:
                    dt = datetime.strptime(f"{row['date']} {time_str}", "%b %d, %Y %I:%M%p")
                    return eastern.localize(dt).astimezone(utc)
                except:
                    return pd.NaT

            df['datetime_utc'] = df.apply(parse_datetime, axis=1)

            # 6. Upload as Parquet to Silver Layer
            pq_buffer = io.BytesIO()
            df.to_parquet(pq_buffer, engine='pyarrow', index=False)
            
            s3_client.put_object(Bucket=bucket, Key=silver_key, Body=pq_buffer.getvalue())
            logger.success(f"✅ Silver Layer: Uploaded {year}-{month_str} to S3.")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning(f"⚠️ Bronze file not found for {year}-{month_str} ({bronze_key}). Run scraper first.")
            else:
                logger.error(f"S3 Error downloading {bronze_key}: {e}")
        except Exception as e:
            logger.error(f"❌ Transformation failed for {year}-{month_str}: {e}")

if __name__ == "__main__":
    main()