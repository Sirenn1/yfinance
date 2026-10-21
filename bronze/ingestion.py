import yfinance as yf
import pandas as pd
import time
from datetime import date
from pathlib import Path

def run_bronze():
    print("Running Ingestion")

    BATCH_SIZE = 300
    RUN_DATE = date.today().isoformat()

    BASE_DIR = Path(__file__).resolve().parents[1]

    config_path = BASE_DIR / "config" / "emiten_code_only.csv"
    bronze_dir = BASE_DIR / "bronze" / f"run_date={RUN_DATE}"

    bronze_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(config_path)
    codes = df["Kode_JK"].to_list()

    index = 0
    for i in range(0, len(codes), BATCH_SIZE):
        batch = codes[i:i + BATCH_SIZE]
        index += 1

        yf_data = yf.download(batch, progress=True)

        if yf_data.empty:
            continue

        yf_data.to_parquet(bronze_dir / f"batch_{index}.parquet")
        time.sleep(2)
        print(f"Saved {len(codes)} tickers to Bronze layer as Parquet files")
