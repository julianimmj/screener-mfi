"""
Diagnostic: Find the resampling method that produces ZERO false signals
for VALE3, BBAS3, ITUB4 (user confirmed none have signals on TV right now).
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mfi_engine import _compute_mfi, _find_crossover_signal, MFI_LENGTH, MFI_TIMEFRAME, HISTORY_DAYS
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

# Ground truth: NONE of these tickers have a signal on TV right now
NO_SIGNAL_TICKERS = ["VALE3.SA", "BBAS3.SA", "ITUB4.SA"]

def resample_calendar(df, n_days, offset=0):
    dates = df.index.tz_localize(None)
    block_ids = (dates.map(lambda d: d.toordinal()) - 719163 + offset) // n_days
    df_g = df.copy()
    df_g['_block'] = block_ids
    r = df_g.groupby('_block').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'})
    last_d = df_g.groupby('_block').apply(lambda x: x.index[-1], include_groups=False)
    r.index = last_d.values
    return r

def resample_trading_days(df, n_days, offset=0):
    df2 = df.iloc[offset:].copy()
    block_ids = [i // n_days for i in range(len(df2))]
    df2['_block'] = block_ids
    r = df2.groupby('_block').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'})
    last_d = df2.groupby('_block').apply(lambda x: x.index[-1], include_groups=False)
    r.index = last_d.values
    return r

def check_signal(hist, resample_func, n_days, offset):
    """Returns True if a signal is found (which is a FALSE POSITIVE)."""
    r = resample_func(hist, n_days, offset)
    mfi = _compute_mfi(r, MFI_LENGTH)
    signal = _find_crossover_signal(mfi)
    return signal is not None, signal

# Fetch data
print("Fetching data...")
all_hist = {}
for ticker in NO_SIGNAL_TICKERS:
    tk = yf.Ticker(ticker)
    all_hist[ticker] = tk.history(period=f"{HISTORY_DAYS}d")
    print(f"  {ticker}: {len(all_hist[ticker])} daily bars")

print(f"\nGround truth: ALL tickers should have NO signal (0 false positives = correct)")
print("=" * 90)

# Method A: Calendar days
print("\n--- Method A: CALENDAR DAYS ---")
for offset in range(8):
    false_positives = []
    for ticker in NO_SIGNAL_TICKERS:
        has_sig, sig = check_signal(all_hist[ticker], resample_calendar, 8, offset)
        if has_sig:
            false_positives.append(f"{ticker.replace('.SA','')}: {sig['signal_type']} on {str(sig['signal_date'])[:10]}")
    
    status = f"❌ {len(false_positives)} false" if false_positives else "✅ PERFECT"
    fp_detail = " | ".join(false_positives) if false_positives else ""
    print(f"  cal_offset={offset}: {status}  {fp_detail}")

# Method B: Trading days
print("\n--- Method B: TRADING DAYS (every 8 rows) ---")
for offset in range(8):
    false_positives = []
    for ticker in NO_SIGNAL_TICKERS:
        has_sig, sig = check_signal(all_hist[ticker], resample_trading_days, 8, offset)
        if has_sig:
            false_positives.append(f"{ticker.replace('.SA','')}: {sig['signal_type']} on {str(sig['signal_date'])[:10]}")
    
    status = f"❌ {len(false_positives)} false" if false_positives else "✅ PERFECT"
    fp_detail = " | ".join(false_positives) if false_positives else ""
    print(f"  trd_offset={offset}: {status}  {fp_detail}")

print("\n" + "=" * 90)
print("Look for ✅ PERFECT — zero false positives")
