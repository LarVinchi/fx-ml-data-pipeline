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

# ✅ NEW: Country → Currency mapping
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

def get_months_in_range(start, end):
    return pd.date_range(start, end, freq="MS")

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

# ✅ FIXED JS cleaner (handles jan1 issue)
def clean_js(js):
    js = re.sub(r'^.*?=\s*', '', js)
    js = js.rstrip(";")

    # fix invalid identifiers like jan1 → "jan1"
    js = re.sub(r'([a-zA-Z]+[0-9]+):', r'"\1":', js)

    js = js.replace("undefined", "null")
    return js

def main():
    logger.info("Starting Forex Factory Silver Transformation...")

    config = load_config()
    bucket = config["aws"]["s3_bucket"]
    bronze_prefix = config["aws"]["bronze_prefix"]
    silver_prefix = config["aws"]["silver_prefix"]

    s3 = boto3.client("s3")

    eastern = pytz.timezone("US/Eastern")
    utc = pytz.UTC

    months = get_months_in_range(
        config["data_range"]["start_date"],
        config["data_range"]["end_date"]
    )

    for dt in months:
        year = dt.year
        month = f"{dt.month:02d}"

        bronze_key = f"{bronze_prefix}forex_factory/year={year}/month={month}/data.json"
        silver_key = f"{silver_prefix}forex_calendar/year={year}/month={month}/data.parquet"

        try:
            s3.head_object(Bucket=bucket, Key=silver_key)
            logger.info(f"Skipping {year}-{month}, already processed.")
            continue
        except:
            pass

        try:
            obj = s3.get_object(Bucket=bucket, Key=bronze_key)
            raw = obj["Body"].read().decode("utf-8")

            parsed = demjson3.decode(clean_js(raw))

            records = []

            for day in parsed.get("days", []):
                for event in day.get("events", []):
                    country = event.get("country", "")

                    records.append({
                        "title": clean_html(event.get("name")),
                        "country": country,
                        "currency": COUNTRY_TO_CURRENCY.get(country),
                        "date": event.get("date"),
                        "time": event.get("timeLabel"),
                        "impact": event.get("impactTitle"),
                        "actual": clean_numeric(clean_html(event.get("actual"))),
                        "forecast": clean_numeric(clean_html(event.get("forecast"))),
                        "previous": clean_numeric(clean_html(event.get("previous")))
                    })

            df = pd.DataFrame(records)

            if df.empty:
                continue

            df["surprise_factor"] = df["actual"] - df["forecast"]

            def parse_dt(row):
                try:
                    t = row["time"] if row["time"] not in ["All Day", "Tentative"] else "12:00am"
                    dt = datetime.strptime(f"{row['date']} {t}", "%b %d, %Y %I:%M%p")
                    return eastern.localize(dt).astimezone(utc)
                except:
                    return pd.NaT

            df["datetime_utc"] = df.apply(parse_dt, axis=1)

            buf = io.BytesIO()
            df.to_parquet(buf, index=False)

            s3.put_object(Bucket=bucket, Key=silver_key, Body=buf.getvalue())

            logger.success(f"Processed {year}-{month} ({len(df)} rows)")

        except Exception as e:
            logger.error(f"Failed {year}-{month}: {e}")
            raise

if __name__ == "__main__":
    main()