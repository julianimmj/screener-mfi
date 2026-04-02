import yfinance as yf
from update_data import TICKERS_BR, TICKERS_BDR

import concurrent.futures

def check_ticker(ticker):
    try:
        tk = yf.Ticker(ticker)
        hist = tk.history(period="5d", raise_errors=False)
        if hist.empty:
            return ticker, False
        return ticker, True
    except Exception:
        return ticker, False

invalid_br = []
invalid_bdr = []

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    results = executor.map(check_ticker, TICKERS_BR)
    for t, is_valid in results:
        if not is_valid:
            invalid_br.append(t)

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    results = executor.map(check_ticker, TICKERS_BDR)
    for t, is_valid in results:
        if not is_valid:
            invalid_bdr.append(t)

print("Invalid BR:", invalid_br)
print("Invalid BDR:", invalid_bdr)
