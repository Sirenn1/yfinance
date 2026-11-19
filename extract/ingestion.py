"""
bronze step. grabs IDX prices off yfinance and dumps them as parquet.
output is long format so athena can read it, saved to
<base>/bronze/prices/run_date=YYYY-MM-DD/part-N.parquet
"""

import time
from datetime import date
from pathlib import Path

import pandas as pd
import yfinance as yf

BATCH_SIZE = 300
MAX_RETRIES = 4
BACKOFF_BASE_SECONDS = 2
SLEEP_BETWEEN_BATCHES = 2

OUTPUT_COLUMNS = [
    "ticker",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
]

_COLUMN_RENAME = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Adj Close": "adj_close",
    "Volume": "volume",
}


def _download_with_retries(batch, *, start=None, period=None, downloader=yf.download):
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            # auto_adjust=False matters, newer yfinance drops Adj Close otherwise
            return downloader(
                batch,
                start=start,
                period=period,
                auto_adjust=False,
                group_by="ticker",
                progress=False,
                threads=True,
            )
        except Exception as err:  # noqa: BLE001
            last_err = err
            wait = BACKOFF_BASE_SECONDS * (2 ** attempt)
            print(f"  download failed (attempt {attempt + 1}/{MAX_RETRIES}): {err}; retrying in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"yf.download failed after {MAX_RETRIES} attempts") from last_err


def reshape_to_long(wide):
    # yfinance gives a wide frame athena can't read, flip it to long and drop all-null rows
    if wide.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    frame = wide.copy()

    if isinstance(frame.columns, pd.MultiIndex):
        # many tickers: level 0 = ticker, level 1 = open/high/low/etc
        long = frame.stack(level=0, future_stack=True).reset_index()
        long = long.rename(columns={long.columns[0]: "trade_date", long.columns[1]: "ticker"})
    else:
        # single ticker has no ticker column, run_bronze fills it in
        long = frame.reset_index().rename(columns={frame.index.name or "index": "trade_date"})
        long["ticker"] = None

    long = long.rename(columns=_COLUMN_RENAME)

    # keep schema stable even if yfinance skipped a column
    for col in ("open", "high", "low", "close", "adj_close", "volume"):
        if col not in long.columns:
            long[col] = pd.NA

    long["trade_date"] = pd.to_datetime(long["trade_date"]).dt.date

    price_cols = ["open", "high", "low", "close", "adj_close", "volume"]
    long = long.dropna(subset=price_cols, how="all")

    return long[OUTPUT_COLUMNS].reset_index(drop=True)


def run_bronze(start=None, period="max", config_path=None, output_dir=None, downloader=yf.download):
    print("Running Ingestion")

    run_date = date.today().isoformat()
    base_dir = Path(__file__).resolve().parents[1]

    if config_path is None:
        config_path = base_dir / "extract" / "config" / "config" / "emiten_code_only.csv"
    config_path = Path(config_path)

    if output_dir is None:
        output_dir = base_dir / "extract" / "bronze" / "prices" / f"run_date={run_date}"
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    codes = pd.read_csv(config_path)["Kode_JK"].dropna().astype(str).tolist()

    # start wins over period if both are set
    period_arg = None if start else period

    rows_written = 0
    returned_tickers = set()
    part = 0

    for i in range(0, len(codes), BATCH_SIZE):
        batch = codes[i:i + BATCH_SIZE]
        wide = _download_with_retries(batch, start=start, period=period_arg, downloader=downloader)

        long = reshape_to_long(wide)

        if not long.empty and long["ticker"].isna().all() and len(batch) == 1:
            long["ticker"] = batch[0]

        if long.empty:
            if i + BATCH_SIZE < len(codes):
                time.sleep(SLEEP_BETWEEN_BATCHES)
            continue

        part += 1
        long.to_parquet(output_dir / f"part-{part}.parquet", index=False)

        rows_written += len(long)
        returned_tickers.update(long["ticker"].dropna().unique())

        # don't sleep after the last batch
        if i + BATCH_SIZE < len(codes):
            time.sleep(SLEEP_BETWEEN_BATCHES)

    failed_tickers = sorted(set(codes) - returned_tickers)

    result = {
        "run_date": run_date,
        "rows_written": rows_written,
        "tickers_returned": len(returned_tickers),
        "failed_tickers": failed_tickers,
    }

    print(
        f"Bronze done: wrote {rows_written} rows for {len(returned_tickers)}/{len(codes)} tickers "
        f"to {output_dir} ({len(failed_tickers)} failed)"
    )
    return result


if __name__ == "__main__":
    run_bronze()
