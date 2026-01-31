## Yahoo Finance Pipeline
This pipeline ingests stock market data from yahoo finance (yfinance), stores raw data in a bronze layer (parquet format), and transforms it into a normalized silver layer for analytics.

Bronze layer stores raw market data exactly as received from the source, in partitioned Parquet files.
Silver layer transforms the raw data into a clean, normalized tabular format suitable for analytics and downstream processing.


# Bronze Layer

Raw data downloaded from Yahoo Finance
Stored as Parquet files
Partitioned by run_date
No transformations applied (schema preserved)

# Silver Layer

Normalized, analytics-ready
One row per (date, ticker)
Columns include price metrics such as open, high, low, close, and volume
Stored as a single consolidated Parquet / CSV file
