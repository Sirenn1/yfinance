import pandas as pd
from pathlib import Path

MA_WINDOW = 20


def run_gold():
    BASE_DIR = Path(__file__).resolve().parents[1]

    silver_file = BASE_DIR / "silver" / "silver.csv"
    gold_path = BASE_DIR / "gold"
    gold_path.mkdir(exist_ok=True)

    df = pd.read_csv(silver_file, parse_dates=["date"])
    df = df.dropna(subset=["close"]).sort_values(["ticker", "date"])

    by_ticker = df.groupby("ticker")["close"]
    df["daily_return_pct"] = by_ticker.pct_change() * 100
    df["ma_20"] = by_ticker.transform(lambda s: s.rolling(MA_WINDOW, min_periods=1).mean())

    df[["daily_return_pct", "ma_20"]] = df[["daily_return_pct", "ma_20"]].round(2)
    daily_metrics = df[["date", "ticker", "close", "volume", "daily_return_pct", "ma_20"]]
    daily_metrics.to_csv(gold_path / "daily_metrics.csv", index=False)
