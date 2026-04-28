"""@bruin
name: process_forexfactory
type: python
depends:
  - scrape_forexfactory
@bruin"""

import os
import io
import re
import sys
import yaml
import subprocess

# --- SAFE DEPENDENCY INSTALL ---
def ensure_package(pkg):
    try:
        __import__(pkg)
    except ModuleNotFoundError:
        print(f"[INFO] Installing {pkg}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])

ensure_package("demjson3")
ensure_package("beautifulsoup4")

import demjson3
from bs4 import BeautifulSoup

import pandas as pd
import numpy as np
from datetime import datetime
import pytz
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from loguru import logger

load_dotenv()

# ✅ Country → Currency mapping for joining with Price Action
COUNTRY_TO_CURRENCY = {
    "United States": "USD",
    "Euro Zone": "EUR",
    "United Kingdom": "GBP",
    "Japan": "JPY",
    "Canada": "CAD",
    "Australia": "AUD",
    "New Zealand": "NZD",
    "Switzerland": "CHF",
    "China": "CNY"
}

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "../../../config/pipeline_config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def clean_html(text):
    if pd.isna(text) or not isinstance(text, str):
        return ""
    if "<" in text:
        return BeautifulSoup(text, "html.parser").get_text()
    return text.strip()

def clean_numeric(val):
    if pd.isna(val) or val in ["", "undefined"]:
        return np.nan
    val = str(val).replace(",", "").strip()
    try:
        return float(val.replace("%", "")) / 100 if "%" in val else float(val)
    except:
        return np.nan

# ✅ THE FIX: Bulletproof JS Parser
def clean_js(js):
    # 1. Extract the main JSON object to remove 'var x =' and trailing semicolons
    start = js.find('{')
    end = js.rfind('}')
    if start != -1 and end != -1:
        js = js[start:end+1]

    # 2. Convert JavaScript 'undefined' to JSON 'null'
    js = js.replace("undefined", "null")

    # 3. Fix unquoted keys (e.g., jan1: "value" -> "jan1": "value")
    js = re.sub(r'\b([a-zA-Z_]+[0-9]+)\s*:', r'"\1":', js)

    # 4. Fix unquoted values (e.g., date: jan1 -> date: "jan1")
    # This safely ignores values that are already quoted because a quote is not a letter
    js = re.sub(r'(:\s*)([a-zA-Z_]+[0-9]+)\b', r'\1"\2"', js)

    return js

def main():
    logger.info("⚙️ Starting Forex Factory Silver Transformation...")

    config = load_config()
    bucket = os.getenv("S3_BUCKET_NAME")
    
    # Check if a specific target year is requested, otherwise process all
    target_year = os.getenv("TARGET_YEAR")
    years_to_process = [int(target_year)] if target_year else [2023, 2024, 2025, 2026]

    s3 = boto3.client("s3")
    eastern = pytz.timezone("US/Eastern")
    utc = pytz.UTC

    for year in years_to_process:
        for month in range(1, 13):
            month_str = f"{month:02d}"

            bronze_key = f"bronze/forex_factory/year={year}/month={month_str}/data.json"
            silver_key = f"silver/forex_calendar/year={year}/month={month_str}/data.parquet"

            # --- IDEMPOTENCY CHECK ---
            try:
                s3.head_object(Bucket=bucket, Key=silver_key)
                logger.info(f"⏭️ Skipping {year}-{month_str}, already processed in Silver.")
                continue
            except ClientError:
                pass  # File doesn't exist, proceed to process

            # --- FETCH AND TRANSFORM ---
            try:
                obj = s3.get_object(Bucket=bucket, Key=bronze_key)
                raw = obj["Body"].read().decode("utf-8")

                # Parse the fully cleaned JavaScript object
                parsed = demjson3.decode(clean_js(raw))

                records = []

                for day in parsed.get("days", []):
                    for event in day.get("events", []):
                        country = event.get("country", "")

                        records.append({
                            "title": clean_html(event.get("name") or event.get("title")),
                            "country": country,
                            "currency": COUNTRY_TO_CURRENCY.get(country, "Unknown"),
                            "date": event.get("date"),
                            "time": event.get("timeLabel"),
                            "impact": event.get("impactTitle") or event.get("impactName"),
                            "actual": clean_numeric(clean_html(event.get("actual"))),
                            "forecast": clean_numeric(clean_html(event.get("forecast"))),
                            "previous": clean_numeric(clean_html(event.get("previous")))
                        })

                df = pd.DataFrame(records)

                if df.empty:
                    continue

                # Feature Engineering: Surprise Factor
                df["surprise_factor"] = df["actual"] - df["forecast"]

                # Convert Datetime to precise UTC (Handles 'All Day' events safely)
                def parse_dt(row):
                    try:
                        t = row["time"] if row["time"] and row["time"] not in ["All Day", "Tentative", "Day 1", "Day 2"] else "12:00am"
                        dt_str = f"{row['date']} {t}"
                        # Try parsing standard US format, fallback if needed
                        dt = datetime.strptime(dt_str, "%b %d, %Y %I:%M%p")
                        return eastern.localize(dt).astimezone(utc)
                    except:
                        return pd.NaT

                df["datetime_utc"] = df.apply(parse_dt, axis=1)
                
                # Drop events that couldn't be parsed
                df = df.dropna(subset=['datetime_utc'])

                # Select and order final columns for Athena
                final_cols = [
                    'title', 'country', 'currency', 'impact', 
                    'actual', 'forecast', 'previous', 'surprise_factor', 'datetime_utc'
                ]
                
                buf = io.BytesIO()
                df[final_cols].to_parquet(buf, index=False)

                s3.put_object(Bucket=bucket, Key=silver_key, Body=buf.getvalue())

                logger.success(f"✅ Processed Silver Parquet: {year}-{month_str} ({len(df)} events)")

            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    logger.debug(f"Bronze file not found: {bronze_key} (Normal if data doesn't exist yet)")
                else:
                    logger.error(f"S3 Error: {e}")
            except Exception as e:
                logger.error(f"❌ Failed to transform {year}-{month_str}: {e}")
                # Don't raise the error, allow the loop to continue to the next month
                continue

if __name__ == "__main__":
    main()