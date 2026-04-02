"""
Diagnostic: Compare our MFI crossover detection with what should appear on TradingView.
For a few tickers, prints the full MFI series (8D bars) and any detected crossovers.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mfi_engine import (
    calculate_mfi, _resample_ohlcv, _compute_mfi, _find_crossover_signal,
    MFI_LENGTH, MFI_TIMEFRAME, MFI_OVERBOUGHT, MFI_OVERSOLD, HISTORY_DAYS
)
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone

# Test tickers - pick a few that the user might have verified on TV
TEST_TICKERS = ["PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBAS3.SA", "WEGE3.SA"]

print(f"Config: Length={MFI_LENGTH}, TF={MFI_TIMEFRAME}D, OB={MFI_OVERBOUGHT}, OS={MFI_OVERSOLD}")
print(f"History: {HISTORY_DAYS} days")
print(f"Today: {datetime.now()}")
print("=" * 80)

for ticker in TEST_TICKERS:
    print(f"\n{'='*80}")
    print(f"  {ticker}")
    print(f"{'='*80}")
    
    tk = yf.Ticker(ticker)
    hist = tk.history(period=f"{HISTORY_DAYS}d")
    
    if hist.empty:
        print("  No data")
        continue
    
    print(f"  Daily bars: {len(hist)} (from {hist.index[0].date()} to {hist.index[-1].date()})")
    
    # Resample
    resampled = _resample_ohlcv(hist, MFI_TIMEFRAME)
    print(f"  8D bars: {len(resampled)}")
    
    # Compute MFI
    mfi = _compute_mfi(resampled, MFI_LENGTH)
    mfi_clean = mfi.dropna()
    
    # Show the last 8 MFI values (bars) and the dates of those bars
    print(f"\n  Last 8 MFI values (8D bars):")
    for dt, val in mfi_clean.tail(8).items():
        marker = ""
        if len(mfi_clean) >= 2:
            idx = mfi_clean.index.get_loc(dt)
            if idx > 0:
                prev = mfi_clean.iloc[idx - 1]
                if prev < MFI_OVERBOUGHT and val > MFI_OVERBOUGHT:
                    marker = " <<<< OB CROSS"
                elif prev > MFI_OVERSOLD and val < MFI_OVERSOLD:
                    marker = " <<<< OS CROSS"
        bar_date = dt.date() if hasattr(dt, 'date') else str(dt)[:10]
        print(f"    {bar_date} → MFI = {val:.2f}{marker}")
    
    # Find crossover using engine
    signal = _find_crossover_signal(mfi)
    if signal:
        sig_date = signal['signal_date']
        if hasattr(sig_date, 'date'):
            sig_date = sig_date.date()
        days_ago = (datetime.now(timezone.utc).date() - pd.Timestamp(sig_date).date()).days
        print(f"\n  Engine signal: {signal['signal_type']} on {sig_date} ({days_ago} days ago)")
        print(f"    MFI at signal: {signal['mfi_at_signal']}, MFI prev: {signal['mfi_prev_at_signal']}")
        if days_ago <= 7:
            print(f"    ✅ Within 7-day window")
        else:
            print(f"    ❌ OUTSIDE 7-day window (would be filtered by app.py)")
    else:
        print(f"\n  Engine signal: NONE (no crossover found)")
    
    # Also check what calculate_mfi returns 
    result = calculate_mfi(ticker)
    if result:
        print(f"  Full result: MFI={result['MFI']}, Signal={result['Signal Type']}, Date={result['Signal Date']}")
