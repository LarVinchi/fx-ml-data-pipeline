import streamlit as st
import awswrangler as wr
import pandas as pd
import plotly.express as px
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- Page Config ---
st.set_page_config(page_title="Forex Macro Intelligence", layout="wide", page_icon="🌐")

def sync_athena():
    """Automatically repairs Athena partitions if the catalog is out of sync."""
    with st.spinner("🛠️ Synchronizing AWS Lakehouse Metadata..."):
        db = "forex_lakehouse_db"
        try:
            wr.athena.start_query_execution("MSCK REPAIR TABLE forex_calendar", database=db)
            wr.athena.start_query_execution("MSCK REPAIR TABLE price_action", database=db)
            st.success("✅ Catalog Synchronized! Please refresh the page.")
        except Exception as e:
            st.error(f"Sync failed: {e}")

@st.cache_data(ttl=600)
def load_gold_data():
    """Fetches macro features from Athena Gold Layer."""
    try:
        query = """
            SELECT * FROM forex_lakehouse_db.gold_ml_features 
            ORDER BY event_time DESC 
            LIMIT 1000
        """
        df = wr.athena.read_sql_query(sql=query, database="forex_lakehouse_db")
        return df
    except Exception as e:
        st.error(f"Failed to load historical data. Ensure Athena tables exist. Error: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def load_predictions():
    """Fetches the latest XGBoost live forecasts from the S3 Gold Layer."""
    bucket = os.getenv('S3_BUCKET_NAME', 'forex-datalake-bucket')
    path = f"s3://{bucket}/gold/predictions/"
    try:
        # dataset=True automatically reads across all year=XXXX partitions
        df = wr.s3.read_parquet(path, dataset=True)
        return df
    except Exception as e:
        # Fails silently and returns empty if the folder doesn't exist yet
        return pd.DataFrame()

# --- Header ---
st.title("🌐 Forex Lakehouse & AI Predictions")
st.markdown("Automated macroeconomic data engineering and XGBoost trend forecasting.")

col1, col2 = st.columns([8, 2])
with col2:
    if st.button("🔄 Sync Lakehouse Partitions"):
        sync_athena()

# Load Data
df = load_gold_data()
preds_df = load_predictions()

# --- Layout: Tabs ---
tab1, tab2 = st.tabs(["🤖 Live AI Forecasts", "📊 Historical Macro Analysis"])

# ==========================================
# TAB 1: AI PREDICTIONS
# ==========================================
with tab1:
    st.subheader("🔮 XGBoost Macro Trend Forecasts")
    st.markdown("These are the predicted price movements for the *next month* based on the latest available macro events.")
    
    if not preds_df.empty:
        # --- THE FIX: Convert AWS Wrangler Categoricals to Strings ---
        preds_df['year'] = preds_df['year'].astype(str)
        preds_df['current_month'] = preds_df['current_month'].astype(str)
        
        # Clean up the display format
        preds_df['forecast_generated_at'] = pd.to_datetime(preds_df['forecast_generated_at']).dt.strftime('%Y-%m-%d %H:%M UTC')
        
        # Sort by the most recent forecasts
        preds_df = preds_df.sort_values(by=['year', 'current_month', 'pair'], ascending=[False, False, True])
        
        # Create metric cards for the absolute latest forecasts
        latest_year = preds_df['year'].max()
        latest_month = preds_df[preds_df['year'] == latest_year]['current_month'].max()
        latest_preds = preds_df[(preds_df['year'] == latest_year) & (preds_df['current_month'] == latest_month)]
        
        st.markdown(f"### Latest Outlook (Based on {latest_month} data)")
        metric_cols = st.columns(len(latest_preds))
        
        for idx, row in latest_preds.iterrows():
            col_idx = len(metric_cols) % (idx + 1) if idx > 0 else 0
            with metric_cols[idx % len(metric_cols)]:
                trend_val = row['predicted_next_month_trend']
                direction = "Bullish 📈" if trend_val > 0 else "Bearish 📉"
                st.metric(
                    label=f"{row['pair']}",
                    value=direction,
                    delta=f"{trend_val:.4f} movement predicted"
                )
                
        st.markdown("---")
        st.markdown("### 🗄️ Full Prediction Registry")
        st.dataframe(
            preds_df[['pair', 'current_month', 'predicted_next_month_trend', 'forecast_generated_at', 'year']],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No AI predictions found. Run the `train_macro_trend` asset in your pipeline to generate forecasts.")

# ==========================================
# TAB 2: HISTORICAL ANALYSIS
# ==========================================
with tab2:
    # --- Visualization ---
    st.subheader("📈 Macro Impact vs. Hourly Price Action")
    
    # Clean data for plotting
    plot_df = df.dropna(subset=['surprise_factor', 'price_change_label'])
    
    if not plot_df.empty:
        fig = px.scatter(
            plot_df, 
            x="surprise_factor", 
            y="price_change_label", 
            color="pair",
            hover_data=['event_name', 'impact', 'event_time'],
            labels={
                "surprise_factor": "Economic Surprise Factor (Actual - Forecast)",
                "price_change_label": "Price Move During Hour (Close - Open)"
            },
            title="Correlation Scatter Map",
            template="plotly_dark"
        )
        
        # Add a zero-line crosshair
        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
        fig.add_vline(x=0, line_dash="dash", line_color="gray", opacity=0.5)
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Not enough numeric data to plot the scatter chart.")

    # --- Raw Data Table ---
    st.markdown("---")
    st.subheader("📋 Gold Features Table (Latest 1000 Events)")
    
    # Format the dataframe for display
    display_df = df.copy()
    if 'event_time' in display_df.columns:
        display_df['event_time'] = pd.to_datetime(display_df['event_time']).dt.strftime('%Y-%m-%d %H:%M')
        
    st.dataframe(display_df, use_container_width=True, hide_index=True)