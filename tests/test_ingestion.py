"""tests for bronze ingestion. yf.download is mocked so no network/aws."""

from datetime import date

import pandas as pd
import pytest

from extract.ingestion import OUTPUT_COLUMNS, reshape_to_long, run_bronze

FIELDS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]


def make_wide(tickers, dates, *, null_ticker=None):
    # fake yfinance output: wide frame with a (ticker, field) multi-index
    idx = pd.DatetimeIndex(pd.to_datetime(dates), name="Date")
    columns = pd.MultiIndex.from_product([tickers, FIELDS])
    data = {}
    for t in tickers:
        for f in FIELDS:
            if t == null_ticker:
                data[(t, f)] = [pd.NA] * len(idx)
            elif f == "Volume":
                data[(t, f)] = [1000 + i for i in range(len(idx))]
            else:
                data[(t, f)] = [10.0 + i for i in range(len(idx))]
    return pd.DataFrame(data, index=idx, columns=columns)


@pytest.fixture
def ticker_csv(tmp_path):
    p = tmp_path / "tickers.csv"
    pd.DataFrame({"Kode_JK": ["AALI.JK", "ACES.JK"]}).to_csv(p, index=False)
    return p


def test_schema_and_dtypes(ticker_csv, tmp_path):
    dates = ["2026-01-05", "2026-01-06"]

    def fake_download(batch, **kwargs):
        assert kwargs["auto_adjust"] is False  # must keep Adj Close
        return make_wide(batch, dates)

    result = run_bronze(
        config_path=ticker_csv,
        output_dir=tmp_path / "out",
        downloader=fake_download,
    )

    assert result["run_date"] == date.today().isoformat()
    assert result["tickers_returned"] == 2
    assert result["failed_tickers"] == []
    assert result["rows_written"] == 4

    files = list((tmp_path / "out").glob("*.parquet"))
    assert len(files) == 1

    df = pd.read_parquet(files[0])
    assert list(df.columns) == OUTPUT_COLUMNS
    assert pd.api.types.is_integer_dtype(df["volume"])
    assert pd.api.types.is_float_dtype(df["close"])
    assert set(df["ticker"]) == {"AALI.JK", "ACES.JK"}


def test_empty_response_writes_nothing(ticker_csv, tmp_path):
    def fake_download(batch, **kwargs):
        return pd.DataFrame()

    result = run_bronze(
        config_path=ticker_csv,
        output_dir=tmp_path / "out",
        downloader=fake_download,
    )

    assert result["rows_written"] == 0
    assert result["tickers_returned"] == 0
    assert sorted(result["failed_tickers"]) == ["AALI.JK", "ACES.JK"]
    assert list((tmp_path / "out").glob("*.parquet")) == []


def test_market_holiday_no_rows(ticker_csv, tmp_path):
    # holiday = right columns, zero rows
    def fake_download(batch, **kwargs):
        return make_wide(batch, dates=[])

    result = run_bronze(
        config_path=ticker_csv,
        output_dir=tmp_path / "out",
        downloader=fake_download,
    )

    assert result["rows_written"] == 0
    assert list((tmp_path / "out").glob("*.parquet")) == []


def test_one_bad_ticker_among_valid(ticker_csv, tmp_path):
    # all-null ticker should get dropped and show up as failed
    dates = ["2026-01-05", "2026-01-06"]

    def fake_download(batch, **kwargs):
        return make_wide(batch, dates, null_ticker="ACES.JK")

    result = run_bronze(
        config_path=ticker_csv,
        output_dir=tmp_path / "out",
        downloader=fake_download,
    )

    assert result["tickers_returned"] == 1
    assert result["failed_tickers"] == ["ACES.JK"]

    df = pd.read_parquet(next((tmp_path / "out").glob("*.parquet")))
    assert set(df["ticker"]) == {"AALI.JK"}


def test_retry_then_succeed(ticker_csv, tmp_path, monkeypatch):
    monkeypatch.setattr("extract.ingestion.time.sleep", lambda *_: None)

    calls = {"n": 0}
    dates = ["2026-01-05"]

    def flaky_download(batch, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionError("rate limited")
        return make_wide(batch, dates)

    result = run_bronze(
        config_path=ticker_csv,
        output_dir=tmp_path / "out",
        downloader=flaky_download,
    )

    assert calls["n"] == 2
    assert result["rows_written"] == 2


def test_retry_exhausted_raises(ticker_csv, tmp_path, monkeypatch):
    monkeypatch.setattr("extract.ingestion.time.sleep", lambda *_: None)

    def always_fail(batch, **kwargs):
        raise ConnectionError("rate limited")

    with pytest.raises(RuntimeError, match="failed after"):
        run_bronze(
            config_path=ticker_csv,
            output_dir=tmp_path / "out",
            downloader=always_fail,
        )


def test_reshape_single_ticker_flat_columns():
    # single-ticker downloads have flat columns, no ticker level
    idx = pd.DatetimeIndex(pd.to_datetime(["2026-01-05"]), name="Date")
    wide = pd.DataFrame(
        {
            "Open": [10.0], "High": [11.0], "Low": [9.0],
            "Close": [10.5], "Adj Close": [10.5], "Volume": [1000],
        },
        index=idx,
    )
    long = reshape_to_long(wide)
    assert list(long.columns) == OUTPUT_COLUMNS
    assert len(long) == 1
