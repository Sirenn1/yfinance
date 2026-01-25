import glob
from datetime import date
import pandas as pd

dfs = []

RUN_DATE = date.today().isoformat()
df = pd.concat(map(pd.read_parquet, glob.glob(f'../bronze/run_date={RUN_DATE}/*.parquet')))

df = df.reset_index()

df.columns = [
    col[0] if col[0] == 'Date' else f'{col[0]}_{col[1]}'
    for col in df.columns
]

long_df = df.melt(
    id_vars='Date',
    var_name='metric_ticker',
    value_name='value'
)

long_df[['metric', 'ticker']] = long_df['metric_ticker'].str.split('_', n=1, expand=True)

silver = (
    long_df
    .pivot_table(
        index=['Date', 'ticker'],
        columns='metric',
        values='value'
    )
    .reset_index()
)

silver.columns = silver.columns.str.lower()

silver.to_csv('silver.csv')