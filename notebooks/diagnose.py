import awswrangler as wr
import os
from dotenv import load_dotenv

load_dotenv()

print("🕵️ Running Data Lakehouse Diagnostics...\n")

# 1. Check Dukascopy Data (Silver)
df_price = wr.athena.read_sql_query("SELECT COUNT(*) as count FROM forex_lakehouse_db.price_action", database="forex_lakehouse_db")
print(f"📈 Price Action (Dukascopy) Rows: {df_price['count'][0]}")

# 2. Check ForexFactory Data (Silver)
df_news = wr.athena.read_sql_query("SELECT COUNT(*) as count FROM forex_lakehouse_db.forex_calendar", database="forex_lakehouse_db")
print(f"📰 Forex Calendar (News) Rows: {df_news['count'][0]}")

# 3. Check the Gold Table
df_gold = wr.athena.read_sql_query("SELECT COUNT(*) as count FROM forex_lakehouse_db.gold_ml_features", database="forex_lakehouse_db")
print(f"🏆 Gold ML Features Rows: {df_gold['count'][0]}")

if df_price['count'][0] > 0 and df_news['count'][0] > 0 and df_gold['count'][0] == 0:
    print("\n💡 DIAGNOSIS: You fell into the 'Empty First Run' Trap!")
    print("Your Silver data is perfectly fine, but your Gold table is stuck empty.")