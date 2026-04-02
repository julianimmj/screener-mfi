"""
Diagnostic for MULT3.SA: Check why it shows OVERBOUGHT (high MFI) 
in Python vs OVERSOLD (low MFI) in TradingView.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import yfinance as yf
from mfi_engine import _resample_ohlcv, _compute_mfi, MFI_LENGTH, HISTORY_DAYS

ticker = "MULT3.SA"
print(f"Fetching data for {ticker}...")
tk = yf.Ticker(ticker)
hist = tk.history(period="200d")

if hist.empty:
    print("NO DATA")
    sys.exit(0)

# Resample using the current logic
resampled = _resample_ohlcv(hist, 8)
print("\nResampled 8D bars (last 5):")
print(resampled.tail())

print("\nStep-by-step MFI calculation for the last 5 bars:")
hlc3 = (resampled['High'] + resampled['Low'] + resampled['Close']) / 3
raw_mf = hlc3 * resampled['Volume']
direction = hlc3.diff()
positive_mf = raw_mf.where(direction > 0, 0.0)
negative_mf = raw_mf.where(direction < 0, 0.0)

pos_sum = positive_mf.rolling(window=MFI_LENGTH).sum()
neg_sum = negative_mf.rolling(window=MFI_LENGTH).sum()
mfr = pos_sum / neg_sum.replace(0, pd.NA)
mfi = 100 - 100 / (1 + mfr)

df_calc = pd.DataFrame({
    'HLC3': hlc3,
    'Dir': direction,
    'Vol': resampled['Volume'],
    'RawMF': raw_mf,
    '+MF': positive_mf,
    '-MF': negative_mf,
    'PosSum(4)': pos_sum,
    'NegSum(4)': neg_sum,
    'MFR': mfr,
    'MFI': mfi
})

print(df_calc.tail(10))
