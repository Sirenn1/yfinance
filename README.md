# IDX Stock Data Pipeline

A batch data pipeline that collects daily stock prices for companies listed on the Indonesia Stock Exchange (IDX) from Yahoo Finance and processes them using the **Medallion Architecture** (Bronze → Silver → Gold).

## Architecture

```
config/emiten_code_only.csv   (list of ~780 IDX tickers)
            │
            ▼
┌──────────────────────┐
│ Bronze               │  Raw OHLCV data from yfinance, downloaded in batches
│ bronze/run_date=...  │  and stored as Parquet, partitioned by run date
└──────────────────────┘
            │
            ▼
┌──────────────────────┐
│ Silver               │  Flattens the multi-index columns and reshapes the
│ silver/silver.csv    │  data into one row per (date, ticker)
└──────────────────────┘
            │
            ▼
┌──────────────────────┐
│ Gold                 │  Analytics-ready tables: daily return, 20-day
│ gold/*.csv           │  moving average, and a latest snapshot per ticker
└──────────────────────┘
```

## Layers

| Layer  | Output | Description |
|--------|--------|-------------|
| Bronze | `bronze/run_date=YYYY-MM-DD/batch_N.parquet` | Raw data, as returned by the API |
| Silver | `silver/silver.csv` | Columns: `date, ticker, open, high, low, close, volume` |
| Gold   | `gold/daily_metrics.csv` | Per ticker per day: `close, volume, daily_return_pct, ma_20` |
| Gold   | `gold/latest_snapshot.csv` | Most recent day per ticker, with `above_ma_20` flag, sorted by top gainers |

## Ticker list

`config/DaftarSaham.csv` is the full list of IDX-listed companies. `config/load_data.py` filters out stocks on the special monitoring board (*Pemantauan Khusus*) and adds the `.JK` suffix used by Yahoo Finance, producing `config/emiten_code_only.csv`.

## How to run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python main.py
```

## Tech stack

- Python, pandas
- yfinance (data source)
- Parquet (pyarrow) for raw storage
