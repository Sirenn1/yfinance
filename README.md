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
