"""
Diagnostic: Try all 8 possible epoch offsets to find which one produces
MFI values matching TradingView.

TradingView reference values (from browser verification on 2026-04-02):
  VALE3: MFI ≈ 24.15  (our engine: 34.98 — wrong!)
  BBAS3: MFI ≈ 61.19  (our engine: 4.94 — very wrong!)
  ITUB4: MFI ≈ 29.58  (our engine: 35.76 — wrong!)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mfi_engine import _compute_mfi, MFI_LENGTH, MFI_TIMEFRAME, HISTORY_DAYS
import yfinance as yf
import pandas as pd
import numpy as np

# TV reference values
TV_REFS = {
    "VALE3.SA": 24.15,
    "BBAS3.SA": 61.19,
    "ITUB4.SA": 29.58,
}

def resample_with_offset(df, n_days, offset):
    """Resample with a specific epoch offset."""
    if df.empty:
        return pd.DataFrame()
    
    dates = df.index.tz_localize(None)
    block_ids = (dates.map(lambda d: d.toordinal()) - 719163 + offset) // n_days
    
    df_grouped = df.copy()
    df_grouped['_block'] = block_ids
    
    resampled = df_grouped.groupby('_block').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
    })
    
    last_dates = df_grouped.groupby('_block').apply(lambda x: x.index[-1], include_groups=False)
    resampled.index = last_dates.values
    
    return resampled

print(f"Testing 8 epoch offsets for 3 tickers")
print(f"Target TV values: {TV_REFS}")
print("=" * 100)

for ticker, tv_val in TV_REFS.items():
    tk = yf.Ticker(ticker)
    hist = tk.history(period=f"{HISTORY_DAYS}d")
    
    if hist.empty:
        print(f"{ticker}: No data")
        continue
    
    print(f"\n{ticker} (TV target: {tv_val}):")
    print(f"  Daily bars: {len(hist)} ({hist.index[0].date()} to {hist.index[-1].date()})")
    
    best_offset = -1
    best_diff = 999
    
    for offset in range(8):
        resampled = resample_with_offset(hist, MFI_TIMEFRAME, offset)
        mfi = _compute_mfi(resampled, MFI_LENGTH)
        mfi_clean = mfi.dropna()
        
        if mfi_clean.empty:
            continue
        
        current_mfi = mfi_clean.iloc[-1]
        diff = abs(current_mfi - tv_val)
        marker = " ← BEST" if diff < best_diff else ""
        
        if diff < best_diff:
            best_diff = diff
            best_offset = offset
        
        # Show the last bar date and last 3 MFI values
        last_date = mfi_clean.index[-1]
        bar_date = last_date.date() if hasattr(last_date, 'date') else str(last_date)[:10]
        
        last_vals = [f"{v:.2f}" for v in mfi_clean.tail(3).values]
        print(f"  offset={offset}: MFI={current_mfi:.2f} (diff={diff:.2f}) bar_date={bar_date} last3={last_vals}{marker}")
    
    print(f"  → Best offset: {best_offset} (diff={best_diff:.2f})")

print("\n" + "=" * 100)
print("SUMMARY: Find the offset that minimizes total error across all tickers")
print("=" * 100)

# Now compute total error for each offset
total_errors = {}
all_hist = {}

for ticker in TV_REFS:
    tk = yf.Ticker(ticker)
    all_hist[ticker] = tk.history(period=f"{HISTORY_DAYS}d")

for offset in range(8):
    total_err = 0
    details = []
    for ticker, tv_val in TV_REFS.items():
        hist = all_hist[ticker]
        if hist.empty:
            continue
        resampled = resample_with_offset(hist, MFI_TIMEFRAME, offset)
        mfi = _compute_mfi(resampled, MFI_LENGTH)
        mfi_clean = mfi.dropna()
        if mfi_clean.empty:
            continue
        current_mfi = mfi_clean.iloc[-1]
        err = abs(current_mfi - tv_val)
        total_err += err
        details.append(f"{ticker}={current_mfi:.2f}")
    
    total_errors[offset] = total_err
    marker = " ← WINNER" if total_err == min(total_errors.values()) else ""
    print(f"  offset={offset}: total_error={total_err:.2f} | {', '.join(details)}{marker}")

best = min(total_errors, key=total_errors.get)
print(f"\n✅ BEST OFFSET = {best} (total error = {total_errors[best]:.2f})")
print(f"   Current code uses offset=0. Fix: change (ordinal - 719163) to (ordinal - 719163 + {best})")
