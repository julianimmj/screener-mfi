import yfinance as yf
from update_data import TICKERS_BR
import concurrent.futures

to_remove = []

def check_volume(ticker):
    try:
        tk = yf.Ticker(ticker)
        hist = tk.history(period="30d")
        if hist.empty:
            return ticker, 0, 0
        
        avg_vol_shares = hist['Volume'].tail(20).mean()
        avg_price = hist['Close'].tail(20).mean()
        avg_financial_vol = avg_vol_shares * avg_price
        
        return ticker, avg_vol_shares, avg_financial_vol
    except Exception:
        return ticker, 0, 0

print("Checking volumes for", len(TICKERS_BR), "tickers...")

results_list = []
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    results = executor.map(check_volume, TICKERS_BR)
    for r in results:
        results_list.append(r)

# Filter out the underlying list
baixo_volume = []
for ticker, vol_shares, vol_fin in results_list:
    if vol_financial_condition_or_share := vol_fin < 1000000: # R$ 1,000,000
       pass 
    if vol_shares < 1000000:
        baixo_volume.append(f"{ticker}: shares={int(vol_shares):,} | fin=R${int(vol_fin):,}")

print(f"\\nFound {len(baixo_volume)} tickers with < 1,000,000 shares avg volume:")
for t in baixo_volume:
    print(t)
