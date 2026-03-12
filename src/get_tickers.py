import re
import time
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup

TICKER_RE = re.compile(r"^[A-Z]{1,5}(-[A-Z]{1,2})?$")


def get_top_us_tickers(n=250, pause=0.4):
    base = "https://companiesmarketcap.com/usa/largest-companies-in-the-usa-by-market-cap/"
    tickers = []
    page = 1

    while len(tickers) < n and page <= 20:
        url = base if page == 1 else f"{base}?page={page}"
        response = requests.get(
            url,
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")

        for td in soup.select("table tr td"):
            txt = td.get_text(" ", strip=True).upper()
            for token in re.findall(r"[A-Z]{1,5}(?:-[A-Z]{1,2})?", txt):
                if TICKER_RE.match(token):
                    tickers.append(token)

        seen = set()
        tickers = [t for t in tickers if not (t in seen or seen.add(t))]

        page += 1
        time.sleep(pause)

    tickers = [t.replace(".", "-") for t in tickers[:n]]
    return tickers


def validate_tickers_yahoo(tickers, pause=0.05):
    good = []

    for ticker in tickers:
        try:
            info = yf.Ticker(ticker).fast_info
            if info and (
                "timezone" in info
                or "lastPrice" in info
                or "last_price" in info
            ):
                good.append(ticker)
        except Exception:
            pass

        time.sleep(pause)

    return good


def main():
    output_path = Path("data/raw/tickers.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    tickers = get_top_us_tickers(n=250)
    print("Scraped tickers:", len(tickers))

    tickers = validate_tickers_yahoo(tickers)
    print("Validated tickers:", len(tickers))

    pd.DataFrame({"Ticker": tickers}).to_csv(output_path, index=False)
    print(f"Saved tickers to: {output_path}")


if __name__ == "__main__":
    main()