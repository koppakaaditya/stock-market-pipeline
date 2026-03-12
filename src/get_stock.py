from datetime import datetime, timedelta
from pathlib import Path
import time

import numpy as np
import pandas as pd
import yfinance as yf


def download_stocks(tickers, start, end, chunk_size=50, pause=1.0):
    frames = []

    total_batches = (len(tickers) - 1) // chunk_size + 1

    for i in range(0, len(tickers), chunk_size):
        batch = tickers[i:i + chunk_size]
        batch_num = i // chunk_size + 1

        print(f"Downloading batch {batch_num}/{total_batches} ({len(batch)} tickers)")

        data = yf.download(
            tickers=batch,
            start=str(start),
            end=str(end),
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            threads=True,
            progress=False
        )

        if data is None or data.empty:
            time.sleep(pause)
            continue

        if isinstance(data.columns, pd.MultiIndex):
            for ticker in batch:
                if ticker in data.columns.get_level_values(0):
                    dft = data[ticker].copy()
                    dft["Ticker"] = ticker
                    dft = dft.reset_index()
                    frames.append(dft)
        else:
            ticker = batch[0]
            dft = data.copy()
            dft["Ticker"] = ticker
            dft = dft.reset_index()
            frames.append(dft)

        time.sleep(pause)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df = df.rename(columns={"Adj Close": "Adj_Close"})
    df = df[["Date", "Ticker", "Open", "High", "Low", "Close", "Adj_Close", "Volume"]]
    df = df.dropna(subset=["Close"]).reset_index(drop=True)

    return df


def add_indicators(group):
    group = group.sort_values("Date").copy()

    group["SMA_20"] = group["Close"].rolling(20).mean()
    group["EMA_20"] = group["Close"].ewm(span=20, adjust=False).mean()

    delta = group["Close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / 14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    group["RSI_14"] = 100 - (100 / (1 + rs))

    mid = group["Close"].rolling(20).mean()
    sd = group["Close"].rolling(20).std()

    group["BB_Middle"] = mid
    group["BB_Upper"] = mid + 2 * sd
    group["BB_Lower"] = mid - 2 * sd

    group["return_1d"] = group["Close"].pct_change()
    group["volatility_20d"] = group["return_1d"].rolling(20).std()

    return group


def main():
    tickers_path = Path("data/raw/tickers.csv")
    output_csv = Path("data/raw/stocks_with_indicators.csv")
    output_parquet = Path("data/raw/stocks_with_indicators.parquet")

    if not tickers_path.exists():
        raise FileNotFoundError("Run 01_get_tickers.py first.")

    tickers = pd.read_csv(tickers_path)["Ticker"].dropna().unique().tolist()

    end = datetime.today().date()
    start = (datetime.today() - timedelta(days=365 * 6)).date()

    print("Range:", start, "to", end)

    stocks = download_stocks(tickers, start, end)
    print("Downloaded stock rows:", len(stocks))

    stocks["Date"] = pd.to_datetime(stocks["Date"])
    stocks = stocks.groupby("Ticker", group_keys=False).apply(add_indicators)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    stocks.to_csv(output_csv, index=False)
    stocks.to_parquet(output_parquet, index=False)

    print(f"Saved stock data to: {output_csv}")
    print(f"Saved stock data to: {output_parquet}")


if __name__ == "__main__":
    main()