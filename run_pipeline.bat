@echo off
setlocal enabledelayedexpansion

echo ========================================================
echo   Forex ML Capstone: Automated Quiet Launcher
echo ========================================================

:: 1. Load S3 Bucket Name from .env
if not exist ".env" (
    echo ERROR: .env file missing.
    exit /b 1
)
for /f "tokens=1,2 delims==" %%A in (.env) do (
    if "%%A"=="S3_BUCKET_NAME" set BUCKET_NAME=%%B
)

:: 2. Virtual Environment Setup
if not exist "venv\" (
    python -m venv venv
)
call venv\Scripts\activate.bat

:: 3. SILENT BACKGROUND INSTALLATION
:: Ensures joblib, xgboost, and awswrangler are ready before Bruin starts.
echo [*] Synchronizing environment health...
pip install -q --disable-pip-version-check -r requirements.txt
echo ✅ Environment synchronized.

:: 4. Launch the Smart Trigger
:: Calculates years based on pipeline_config.yaml automatically.
python trigger_pipeline.py

pause