import glob
from datetime import date
import pandas as pd

dfs = []

RUN_DATE = date.today().isoformat()
df = pd.concat(map(pd.read_parquet, glob.glob(f'../bronze/run_date={RUN_DATE}/*.parquet')))

print(df.head())
print('!!!-----------!!!')
print(df.tail())


