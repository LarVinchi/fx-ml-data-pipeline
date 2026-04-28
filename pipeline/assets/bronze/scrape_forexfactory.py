"""@bruin
name: scrape_forexfactory
type: python
@bruin"""

"""
Forex Factory Data Extractor (Bronze Layer).

Bypasses Cloudflare bot protections using SeleniumBase and extracts the raw 
JavaScript calendar payload directly from the browser's memory. 
Respects centralized data_range constraints from pipeline_config.yaml.
"""

import os
import yaml
import json
import time
import boto3
from datetime import datetime
from seleniumbase import SB
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

def load_config() -> dict:
    """Loads the centralized pipeline configuration from the config directory."""
    config_path = os.path.join(os.path.dirname(__file__), "../../../config/pipeline_config.yaml")
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

def main():
    logger.info("🌐 Starting Config-Driven Forex Factory Scraper...")
    
    # 1. Load Configuration and Environment
    config = load_config()
    bucket = os.getenv("S3_BUCKET_NAME", "forex-datalake-bucket")
    target_year = int(os.getenv("TARGET_YEAR", datetime.now().year))
    end_limit_str = config['data_range']['end_date']
    end_limit = datetime.strptime(end_limit_str, "%Y-%m-%d").date()
    
    s3_client = boto3.client('s3')

    # 2. Initialize SeleniumBase in Undetected Mode
    with SB(uc=True, headless=True) as sb:
        for month in range(1, 13):
            current_date = datetime(target_year, month, 1).date()
            
            # 🛑 Config Constraint: Stop if we passed the end_date in the YAML
            if current_date > end_limit:
                logger.info(f"✋ Reached end_date limit ({end_limit_str}). Stopping.")
                break

            s3_key = f"bronze/forex_factory/year={target_year}/month={month:02d}/data.json"
            
            # Idempotency: Skip if file already exists on S3
            try:
                s3_client.head_object(Bucket=bucket, Key=s3_key)
                logger.info(f"⏭️ {target_year}-{month:02d} already exists. Skipping.")
                continue
            except:
                pass

            logger.info(f"🕵️ Scraping Forex Factory for {target_year}-{month:02d}...")
            
            # Navigate to the specific month/year URL
            month_name = current_date.strftime('%b').lower()
            target_url = f"https://www.forexfactory.com/calendar?month={month_name}.{target_year}"
            
            try:
                sb.uc_open_with_reconnect(target_url, 4)
                time.sleep(5) # Allow JS to initialize
                
                # --- The Ultimate Extraction Method ---
                # We fetch the live JS object directly from the page memory
                data_dict = sb.execute_script("return window.calendarComponentStates[1];")
                
                if data_dict:
                    s3_client.put_object(
                        Bucket=bucket, 
                        Key=s3_key, 
                        Body=json.dumps(data_dict).encode('utf-8')
                    )
                    logger.success(f"✅ Successfully uploaded {target_year}-{month:02d} to S3.")
                else:
                    logger.error(f"❌ Failed to find data for {target_year}-{month:02d}")
                    
            except Exception as e:
                logger.error(f"Failed extraction for {target_year}-{month:02d}: {e}")
            
            # Random delay to prevent rate-limiting
            time.sleep(2)

if __name__ == "__main__":
    main()