from pathlib import Path

import pandas as pd


def main():
    stocks_path = Path("data/raw/stocks_with_indicators.parquet")
    macro_path = Path("data/raw/macro_data.parquet")

    output_csv = Path("data/processed/TOP250_STOCKS_6Y_FINAL.csv")
    output_parquet = Path("data/processed/TOP250_STOCKS_6Y_FINAL.parquet")

    if not stocks_path.exists():
        raise FileNotFoundError("Run 02_get_stocks.py first.")

    if not macro_path.exists():
        raise FileNotFoundError("Run 03_get_macro_data.py first.")

    stocks = pd.read_parquet(stocks_path)
    macro = pd.read_parquet(macro_path)

    stocks["Date"] = pd.to_datetime(stocks["Date"])
    macro["Date"] = pd.to_datetime(macro["Date"])

    final_df = stocks.merge(macro, on="Date", how="left")
    final_df = final_df.sort_values(["Ticker", "Date"])

    final_df["fed_rate"] = final_df.groupby("Ticker")["fed_rate"].ffill()
    final_df = final_df.dropna().reset_index(drop=True)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(output_csv, index=False)
    final_df.to_parquet(output_parquet, index=False)

    print("Final shape:", final_df.shape)
    print("Columns:", final_df.columns.tolist())
    print(f"Saved final CSV to: {output_csv}")
    print(f"Saved final Parquet to: {output_parquet}")


if __name__ == "__main__":
    main()
    