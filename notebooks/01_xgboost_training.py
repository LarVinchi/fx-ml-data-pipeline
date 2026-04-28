import awswrangler as wr
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, accuracy_score
import joblib
import os
import sys
from dotenv import load_dotenv

load_dotenv()

print("1. Fetching Gold Data from AWS Athena...")
# Notice we added 'traded_pair', 'open', and 'close' to calculate the monthly trend
query = """
    SELECT 
        traded_pair,
        impact, 
        surprise_factor, 
        candle_open_time, 
        open, 
        close 
    FROM forex_lakehouse_db.gold_ml_features
    ORDER BY candle_open_time ASC
"""
df = wr.athena.read_sql_query(sql=query, database="forex_lakehouse_db")

print(f"📊 CHECKPOINT 1: Fetched {len(df)} rows from Athena.")

if len(df) == 0:
    print("\n❌ STOPPING: Your Gold table in Athena is empty!")
    sys.exit()

print("2. Aggregating Data to Monthly Macro Trends...")

# Ensure datetime and create a 'year_month' column for grouping
df['candle_open_time'] = pd.to_datetime(df['candle_open_time'])
df['year_month'] = df['candle_open_time'].dt.to_period('M')

# Encode Impact
impact_mapping = {'High Impact Expected': 2, 'Medium Impact Expected': 1, 'Low Impact Expected': 0}
df['impact_encoded'] = df['impact'].map(impact_mapping).fillna(0)

# Group by Currency Pair and Month
monthly_df = df.groupby(['traded_pair', 'year_month']).agg(
    total_surprise=('surprise_factor', 'sum'),
    avg_surprise=('surprise_factor', 'mean'),
    high_impact_count=('impact_encoded', lambda x: (x == 2).sum()),
    first_open=('open', 'first'),
    last_close=('close', 'last')
).reset_index()

# Calculate the trend of the CURRENT month (Close - Open)
monthly_df['this_month_trend'] = monthly_df['last_close'] - monthly_df['first_open']

# -------------------------------------------------------------------------
# THE MAGIC SHIFT: Align THIS month's news with NEXT month's trend
# -------------------------------------------------------------------------
# We group by pair again, and shift the 'this_month_trend' up by 1 row.
monthly_df['target_next_month_trend'] = monthly_df.groupby('traded_pair')['this_month_trend'].shift(-1)

# Drop the last month in the dataset because we don't know its "next month" yet!
monthly_df = monthly_df.dropna(subset=['target_next_month_trend'])

print(f"📊 CHECKPOINT 2: Aggregated into {len(monthly_df)} monthly macro records.")

if len(monthly_df) == 0:
    print("\n❌ STOPPING: Not enough data to create monthly shifts.")
    sys.exit()

print("3. Training XGBoost Model...")

# Features: The news data from THIS month, plus how the price moved THIS month
features = ['total_surprise', 'avg_surprise', 'high_impact_count', 'this_month_trend']
X = monthly_df[features]
y = monthly_df['target_next_month_trend']

# Time-Series Split (Never shuffle time-series data!)
split_idx = int(len(monthly_df) * 0.8)
X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

# Train the model
model = xgb.XGBRegressor(
    n_estimators=100, 
    learning_rate=0.1, 
    max_depth=4, 
    random_state=42
)
model.fit(X_train, y_train)

print("4. Evaluating Macro Model...")
predictions = model.predict(X_test)

# Evaluation 1: Mean Absolute Error (How many pips off are we?)
mae = mean_absolute_error(y_test, predictions)
print(f"📉 Mean Absolute Error: {mae:.5f} price difference")

# Evaluation 2: Directional Accuracy (Did we predict UP or DOWN correctly?)
# Convert values to binary: 1 if positive trend, 0 if negative trend
actual_direction = np.where(y_test > 0, 1, 0)
predicted_direction = np.where(predictions > 0, 1, 0)
accuracy = accuracy_score(actual_direction, predicted_direction)
print(f"🎯 Directional Accuracy: {accuracy * 100:.2f}%")

print("5. Saving Model Artifact...")
os.makedirs("models", exist_ok=True)
joblib.dump(model, "models/xgboost_macro_model.pkl")
print("✅ SUCCESS: Model trained and saved to models/xgboost_macro_model.pkl")