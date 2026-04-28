import boto3
import os
from dotenv import load_dotenv

load_dotenv()
s3 = boto3.client('s3', region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1"))
bucket = os.getenv("S3_BUCKET_NAME", "forex-datalake-bucket")

keys_to_delete = [
    "bronze/forex_factory/year=2026/month=02/snippet.json",
    "silver/forex_calendar/year=2026/month=02/data.parquet"
]

for key in keys_to_delete:
    s3.delete_object(Bucket=bucket, Key=key)
    print(f"🗑️ Deleted {key}")

print("✅ Ready to re-scrape!")