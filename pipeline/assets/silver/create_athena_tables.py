"""@bruin
name: create_athena_tables
type: python
depends:
  - process_dukascopy
  - process_forexfactory
@bruin"""

import boto3
import time
import os
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

ATHENA_CLIENT = boto3.client('athena', region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1"))
BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'forex-datalake-bucket')
S3_OUTPUT = f"s3://{BUCKET_NAME}/athena_query_results/"

def run_athena_query(query, database="forex_lakehouse_db"):
    response = ATHENA_CLIENT.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': S3_OUTPUT}
    )
    qid = response['QueryExecutionId']
    while True:
        status = ATHENA_CLIENT.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']['State']
        if status == 'SUCCEEDED': return True
        if status in ['FAILED', 'CANCELLED']: 
            reason = ATHENA_CLIENT.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            logger.error(f"Query Failed: {reason}")
            return False
        time.sleep(1)

def main():
    logger.info("🏛️ Hard-Resetting Athena Schema...")

    # 1. Ensure Database
    run_athena_query("CREATE DATABASE IF NOT EXISTS forex_lakehouse_db")

    # 2. DROP TABLES (Forces Athena to forget the old '40-row' metadata)
    logger.info("🧹 Nuking old metadata to fix partition bug...")
    run_athena_query("DROP TABLE IF EXISTS forex_lakehouse_db.price_action")
    run_athena_query("DROP TABLE IF EXISTS forex_lakehouse_db.forex_calendar")

    # 3. Recreate Price Action with strict Hive-style partitioning
    price_table = f"""
    CREATE EXTERNAL TABLE forex_lakehouse_db.price_action (
        timestamp TIMESTAMP,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        candle_volatility DOUBLE
    )
    PARTITIONED BY (pair STRING, year STRING)
    STORED AS PARQUET
    LOCATION 's3://{BUCKET_NAME}/silver/price_action/'
    TBLPROPERTIES ('classification'='parquet')
    """

    # 4. Recreate News Table
    news_table = f"""
    CREATE EXTERNAL TABLE forex_lakehouse_db.forex_calendar (
        title STRING,
        country STRING,
        currency STRING,
        impact STRING,
        actual DOUBLE,
        forecast DOUBLE,
        previous DOUBLE,
        surprise_factor DOUBLE,
        datetime_utc TIMESTAMP
    )
    PARTITIONED BY (year STRING, month STRING)
    STORED AS PARQUET
    LOCATION 's3://{BUCKET_NAME}/silver/forex_calendar/'
    TBLPROPERTIES ('classification'='parquet')
    """

    run_athena_query(price_table)
    run_athena_query(news_table)
    
    # 5. The "Magic" Sync
    logger.info("🔄 Re-scanning S3 for all partitions...")
    run_athena_query("MSCK REPAIR TABLE forex_lakehouse_db.price_action")
    run_athena_query("MSCK REPAIR TABLE forex_lakehouse_db.forex_calendar")
    
    logger.success("✅ Athena Schema Rebuilt. Run Audit again.")

if __name__ == "__main__":
    main()