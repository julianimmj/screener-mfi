"""
Diagnostic: Test if TradingView uses 8 TRADING days or 8 CALENDAR days.

Method A: Calendar-based (current code) — group by (ordinal - epoch) // 8
Method B: Trading-day-based — every 8 rows is one bar (since each row is a trading day)
Method C: pandas resample('8D') with different origins

TradingView reference values (from browser on 2026-04-02, 1D chart, MFI MTF indicator):
  VALE3: ~24.15
  BBAS3: ~61.19  
  ITUB4: ~29.58
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mfi_engine import _compute_mfi, MFI_LENGTH, MFI_TIMEFRAME, HISTORY_DAYS
import yfinance as yf
import pandas as pd
import numpy as np

TV_REFS = {"VALE3.SA": 24.15, "BBAS3.SA": 61.19, "ITUB4.SA": 29.58}

def resample_calendar(df, n_days, offset=0):
    """Method A: Calendar-based (epoch-aligned)."""
    dates = df.index.tz_localize(None)
    block_ids = (dates.map(lambda d: d.toordinal()) - 719163 + offset) // n_days
    df_g = df.copy()
    df_g['_block'] = block_ids
    r = df_g.groupby('_block').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'})
    last_d = df_g.groupby('_block').apply(lambda x: x.index[-1], include_groups=False)
    r.index = last_d.values
    return r

def resample_trading_days(df, n_days, offset=0):
    """Method B: Every N trading days = one bar. Offset shifts start."""
    df2 = df.copy()
    # Trim offset rows from the start so grouping shifts
    df2 = df2.iloc[offset:]
    block_ids = [i // n_days for i in range(len(df2))]
    df2['_block'] = block_ids
    r = df2.groupby('_block').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'})
    last_d = df2.groupby('_block').apply(lambda x: x.index[-1], include_groups=False)
    r.index = last_d.values
    return r

print("=" * 90)
print("COMPARING: Calendar Days vs Trading Days resampling")
print("=" * 90)

all_hist = {}
for ticker in TV_REFS:
    tk = yf.Ticker(ticker)
    all_hist[ticker] = tk.history(period=f"{HISTORY_DAYS}d")

# Method A: Calendar-based with all offsets (0-7)
print("\n--- Method A: CALENDAR DAYS (current approach) ---")
for offset in range(8):
    errs = []
    vals = []
    for ticker, tv_val in TV_REFS.items():
        hist = all_hist[ticker]
        r = resample_calendar(hist, 8, offset)
        mfi = _compute_mfi(r, MFI_LENGTH)
        c = mfi.dropna()
        v = c.iloc[-1] if not c.empty else 0
        errs.append(abs(v - tv_val))
        vals.append(f"{ticker.replace('.SA','')}={v:.1f}")
    total = sum(errs)
    print(f"  cal_offset={offset}: err={total:.1f} | {', '.join(vals)}")

# Method B: Trading-day-based with all offsets (0-7) 
print("\n--- Method B: TRADING DAYS (every 8 rows) ---")
for offset in range(8):
    errs = []
    vals = []
    for ticker, tv_val in TV_REFS.items():
        hist = all_hist[ticker]
        r = resample_trading_days(hist, 8, offset)
        mfi = _compute_mfi(r, MFI_LENGTH)
        c = mfi.dropna()
        v = c.iloc[-1] if not c.empty else 0
        errs.append(abs(v - tv_val))
        vals.append(f"{ticker.replace('.SA','')}={v:.1f}")
    total = sum(errs)
    print(f"  trd_offset={offset}: err={total:.1f} | {', '.join(vals)}")

# Method C: pandas resample with various origins
print("\n--- Method C: pandas resample('8D') ---")
for origin in ['epoch', 'start', 'start_day']:
    errs = []
    vals = []
    for ticker, tv_val in TV_REFS.items():
        hist = all_hist[ticker]
        r = hist.resample('8D', origin=origin).agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'}).dropna()
        mfi = _compute_mfi(r, MFI_LENGTH)
        c = mfi.dropna()
        v = c.iloc[-1] if not c.empty else 0
        errs.append(abs(v - tv_val))
        vals.append(f"{ticker.replace('.SA','')}={v:.1f}")
    total = sum(errs)
    print(f"  origin={origin:10s}: err={total:.1f} | {', '.join(vals)}")

print("\n" + "=" * 90)
print("Look for the method+offset with the LOWEST total error")
