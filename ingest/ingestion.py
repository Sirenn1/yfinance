import yfinance as yf
import pandas as pd
import time
import os
from datetime import date

BATCH_SIZE = 50
RUN_DATE = date.today().isoformat()

df = pd.read_csv('../config/emiten_code_only.csv')
codes = df['Kode_JK'].to_list()

bronze_dir = f'../bronze/run_date={RUN_DATE}'

if not os.path.exists(bronze_dir):
    os.makedirs(bronze_dir)

for i in range(0, len(codes), BATCH_SIZE):
    batch = codes[i:i + BATCH_SIZE]

    yf_data = yf.download(batch, progress=False)

    if yf_data.empty:
        continue

    yf_data.to_parquet(
        f'{bronze_dir}/batch_{i}.parquet'
    )

    time.sleep(2)
