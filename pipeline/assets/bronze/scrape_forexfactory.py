"""
Forex Factory Data Extractor (Bronze Layer).

This script bypasses Cloudflare protections using SeleniumBase, extracts the 
raw JavaScript calendar payload from Forex Factory, and uploads the raw .js 
files directly into the AWS S3 Bronze layer.
"""

import os
import time
import random
import re
from datetime import datetime
from seleniumbase import SB
from loguru import logger
import boto3
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://www.forexfactory.com/calendar"
WAIT_TIME = 10
BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "forex-datalake-bucket")

def get_s3_client():
    """Initializes and returns an authenticated S3 client."""
    return boto3.client(
        's3',
        region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1")
    )

def extract_js(page_source: str) -> str:
    """
    Parses the raw HTML to extract the embedded JSON-like JavaScript payload.
    
    Args:
        page_source (str): The raw HTML source of the page.
        
    Returns:
        str: The extracted JavaScript dictionary, or None if not found.
    """
    match = re.search(
        r'window\.calendarComponentStates\[1\]\s*=\s*({[\s\S]*?});', 
        page_source, 
        flags=re.DOTALL
    )
    return match.group(1) if match else None

def scrape_month(sb, year: int, month: int, s3_client) -> None:
    """
    Navigates to the Forex Factory calendar for a specific month, extracts 
    the raw data, and uploads it to S3.
    
    Args:
        sb: The SeleniumBase instance.
        year (int): Target year.
        month (int): Target month.
        s3_client: Authenticated boto3 S3 client.
    """
    month_str = datetime(year, month, 1).strftime("%b-%Y").lower()
    url = f"{BASE_URL}?month={month_str}"
    
    logger.info(f"Targeting: {year}-{month:02d} | {url}")
    sb.activate_cdp_mode(url)
    sb.sleep(WAIT_TIME)
    
    if "cloudflare" in sb.get_page_source().lower():
        logger.warning("Cloudflare detected, attempting GUI bypass...")
        sb.uc_gui_click_captcha()
        sb.sleep(5)

    page_source = sb.get_page_source()
    js_content = extract_js(page_source)
    
    if js_content:
        s3_key = f"bronze/forex_factory/year={year}/month={str(month).zfill(2)}/snippet.json"
        s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=js_content.encode('utf-8'))
        logger.info(f"✅ Success: Uploaded {year}-{month} to s3://{BUCKET_NAME}/{s3_key}")
    else:
        logger.error(f"❌ Failed to find JS snippet for {year}-{month}")

def main():
    """Main execution block for the scraper."""
    logger.info("Initializing Forex Factory Scraper...")
    s3_client = get_s3_client()
    
    with SB(uc=True, headless=True) as sb:
        # Example: Scraping the first 3 months of 2024 for the 38-month backfill
        for m in range(1, 4): 
            try:
                scrape_month(sb, 2024, m, s3_client)
                time.sleep(random.uniform(5, 10))
            except Exception as e:
                logger.error(f"Unexpected error in month {m}: {e}")

if __name__ == "__main__":
    main()