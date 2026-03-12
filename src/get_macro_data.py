from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf
from pandas_datareader import data as pdr


def get_sp500_data(start, end):
    sp = yf.download(
        "^GSPC",
        start=str(start),
        end=str(end),
        interval="1d",
        group_by="column",
        auto_adjust=False,
        progress=False
    )

    if isinstance(sp.columns, pd.MultiIndex):
        sp.columns = [col[0] for col in sp.columns]

    sp = sp.reset_index()

    sp = sp.rename(columns={
        "Adj Close": "index_adj_close",
        "Close": "index_close",
        "High": "index_high",
        "Low": "index_low",
        "Open": "index_open",
        "Volume": "index_volume",
    })

    sp = sp[[
        "Date",
        "index_adj_close",
        "index_close",
        "index_high",
        "index_low",
        "index_open",
        "index_volume",
    ]]

    sp["index_return"] = sp["index_close"].pct_change()
    sp["index_volatility"] = sp["index_return"].rolling(20).std()
    sp["Date"] = pd.to_datetime(sp["Date"])

    return sp


def get_fed_funds_rate(start, end):
    fed = pdr.DataReader("FEDFUNDS", "fred", start, end).reset_index()

    date_col = fed.columns[0]
    value_col = fed.columns[1]

    fed = fed.rename(columns={
        date_col: "Date",
        value_col: "fed_rate"
    })

    fed["Date"] = pd.to_datetime(fed["Date"])
    return fed


def main():
    end = datetime.today().date()
    start = (datetime.today() - timedelta(days=365 * 6)).date()

    output_csv = Path("data/raw/macro_data.csv")
    output_parquet = Path("data/raw/macro_data.parquet")

    sp = get_sp500_data(start, end)
    fed = get_fed_funds_rate(start, end)

    macro = sp.merge(fed, on="Date", how="left")
    macro["fed_rate"] = macro["fed_rate"].ffill()

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    macro.to_csv(output_csv, index=False)
    macro.to_parquet(output_parquet, index=False)

    print("Macro shape:", macro.shape)
    print("Columns:", macro.columns.tolist())
    print(f"Saved macro CSV to: {output_csv}")
    print(f"Saved macro Parquet to: {output_parquet}")


if __name__ == "__main__":
    main()