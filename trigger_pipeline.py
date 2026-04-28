import yaml
import subprocess
import os

def main():
    """
    Orchestrates the pipeline by years based on the master config.
    Ensures that TARGET_YEAR is passed to Bruin for isolated processing.
    """
    config_path = "config/pipeline_config.yaml"
    if not os.path.exists(config_path):
        print(f"ERROR: {config_path} not found.")
        return

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    start_date = config['data_range']['start_date']
    end_date = config['data_range']['end_date']
    
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])

    print(f"🚀 Initializing Data Lakehouse: {start_date} to {end_date}")

    for year in range(start_year, end_year + 1):
        print(f"\n📅 Running Pipeline for Year: {year}")
        env = os.environ.copy()
        env["TARGET_YEAR"] = str(year)
        
        # Bruin runs the assets (Scraper, SQL, ML) for this specific year
        subprocess.run(["bruin", "run", "pipeline/"], env=env)

if __name__ == "__main__":
    main()