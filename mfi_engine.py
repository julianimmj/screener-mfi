"""
mfi_engine.py — Motor de Cálculo do Money Flow Index (MFI)

Tradução fiel do Pine Script v3 "Money Flow Index MTF + Alerts" para Python.

Parâmetros configuráveis:
  - Timeframe: 8 dias (resample de barras diárias em blocos de 8 dias)
  - Período (length): 4
  - Sobrecompra (OB): 86
  - Sobrevenda (OS): 24

Classificação:
  MFI >= 86  → 🔴 Sobrecompra (saída iminente)
  MFI <= 24  → 🟢 Sobrevenda (entrada iminente)
  MFI > 60   → 🔵 Tendência de Alta
  MFI < 40   → 🟠 Tendência de Baixa
  40–60      → ⚪ Zona de Transição
"""

import yfinance as yf
import pandas as pd
import numpy as np
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─────────────────────────────────────────────
# Configuration — matches user's Pine Script parameters
# ─────────────────────────────────────────────
MFI_LENGTH = 4        # Período do MFI (rolling window)
MFI_TIMEFRAME = 8     # Resample diário → blocos de N dias
MFI_OVERSOLD = 24     # Nível de sobrevenda
MFI_OVERBOUGHT = 86   # Nível de sobrecompra

# How many calendar days of history to download (enough for resampling + warm-up)
HISTORY_DAYS = 120


# ─────────────────────────────────────────────
# MFI Calculation
# ─────────────────────────────────────────────

def _resample_ohlcv(df: pd.DataFrame, n_days: int) -> pd.DataFrame:
    """
    Resample daily OHLCV data into n-day bars.
    Uses fixed blocks of n trading days (not calendar days).
    """
    if df.empty or len(df) < n_days:
        return pd.DataFrame()

    # Work with trading-day blocks (not calendar)
    n_blocks = len(df) // n_days
    if n_blocks == 0:
        return pd.DataFrame()

    # Trim from the start to have complete blocks aligned to the end
    trim = len(df) - n_blocks * n_days
    df = df.iloc[trim:].copy()

    # Create block labels
    block_ids = np.repeat(range(n_blocks), n_days)
    df['_block'] = block_ids

    resampled = df.groupby('_block').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
    })

    # Recover the last date of each block as index
    dates = df.groupby('_block').apply(lambda x: x.index[-1])
    resampled.index = dates.values

    return resampled


def _compute_mfi(ohlcv: pd.DataFrame, length: int = MFI_LENGTH) -> pd.Series:
    """
    Calculate Money Flow Index from OHLCV data.

    Pine Script equivalent:
        rawMoneyFlow = hlc3 * volume
        positiveMoneyFlow() => hlc3 > hlc3[1] ? rawMoneyFlow : 0
        negativeMoneyFlow() => hlc3 < hlc3[1] ? rawMoneyFlow : 0
        moneyFlowRatio = sma(positiveMF, length) / sma(negativeMF, length)
        MFI = 100 - 100 / (1 + moneyFlowRatio)
    """
    if len(ohlcv) < length + 1:
        return pd.Series(dtype=float)

    # hlc3 = (H + L + C) / 3
    hlc3 = (ohlcv['High'] + ohlcv['Low'] + ohlcv['Close']) / 3

    # Raw Money Flow
    raw_mf = hlc3 * ohlcv['Volume']

    # Positive / Negative Money Flow
    direction = hlc3.diff()
    positive_mf = raw_mf.where(direction > 0, 0.0)
    negative_mf = raw_mf.where(direction < 0, 0.0)

    # SMA of positive and negative money flow
    pos_sma = positive_mf.rolling(window=length).sum()
    neg_sma = negative_mf.rolling(window=length).sum()

    # Money Flow Ratio — avoid division by zero
    mfr = pos_sma / neg_sma.replace(0, np.nan)

    # MFI = 100 - 100 / (1 + MFR)
    mfi = 100 - 100 / (1 + mfr)

    return mfi


def _classify_mfi(mfi_value: float, mfi_prev: float) -> dict:
    """
    Classify asset based on MFI value and detect OB/OS crossovers.

    Returns dict with:
      - status: classification label
      - trend_zone: uptrend / downtrend / transition
      - ob_cross: True if MFI just crossed above OB level
      - os_cross: True if MFI just crossed below OS level
      - signal: 'ENTRADA' / 'SAÍDA' / 'NEUTRO'
    """
    # Trend Zone (lines 40 and 60 from Pine Script)
    if mfi_value > 60:
        trend_zone = "🔵 Tendência de Alta"
        trend_code = "ALTA"
    elif mfi_value < 40:
        trend_zone = "🟠 Tendência de Baixa"
        trend_code = "BAIXA"
    else:
        trend_zone = "⚪ Zona de Transição"
        trend_code = "TRANSIÇÃO"

    # OB/OS classification
    ob_cross = (mfi_prev < MFI_OVERBOUGHT) and (mfi_value >= MFI_OVERBOUGHT) if pd.notna(mfi_prev) else False
    os_cross = (mfi_prev > MFI_OVERSOLD) and (mfi_value <= MFI_OVERSOLD) if pd.notna(mfi_prev) else False

    if mfi_value >= MFI_OVERBOUGHT:
        status = "🔴 Sobrecompra"
        signal = "SAÍDA"
    elif mfi_value <= MFI_OVERSOLD:
        status = "🟢 Sobrevenda"
        signal = "ENTRADA"
    elif mfi_value > 60:
        status = "🔵 Alta"
        signal = "ENTRADA"
    elif mfi_value < 40:
        status = "🟠 Baixa"
        signal = "SAÍDA"
    else:
        status = "⚪ Transição"
        signal = "NEUTRO"

    return {
        "Status": status,
        "Zona": trend_zone,
        "Trend": trend_code,
        "Signal": signal,
        "OB Cross": ob_cross,
        "OS Cross": os_cross,
    }


# ─────────────────────────────────────────────
# Single Ticker Calculation
# ─────────────────────────────────────────────

def calculate_mfi(ticker_symbol: str) -> dict | None:
    """
    Calculate MFI for a single ticker.

    Downloads daily OHLCV, resamples to MFI_TIMEFRAME-day bars,
    computes MFI with MFI_LENGTH period.

    Returns:
        dict with all metrics, or None on failure.
    """
    try:
        tk = yf.Ticker(ticker_symbol)

        # Download daily OHLCV
        hist = tk.history(period=f"{HISTORY_DAYS}d")

        if hist.empty or len(hist) < MFI_TIMEFRAME * (MFI_LENGTH + 2):
            return None

        # Resample to MFI_TIMEFRAME-day bars
        resampled = _resample_ohlcv(hist, MFI_TIMEFRAME)

        if resampled.empty or len(resampled) < MFI_LENGTH + 1:
            return None

        # Compute MFI series
        mfi_series = _compute_mfi(resampled, MFI_LENGTH)

        if mfi_series.empty or mfi_series.dropna().empty:
            return None

        # Get the latest and previous MFI values
        mfi_clean = mfi_series.dropna()
        mfi_current = mfi_clean.iloc[-1]
        mfi_prev = mfi_clean.iloc[-2] if len(mfi_clean) >= 2 else np.nan

        # Classify
        classification = _classify_mfi(mfi_current, mfi_prev)

        # Get volume (average daily volume from original data)
        avg_volume = hist['Volume'].tail(20).mean()

        # Get price
        info = tk.info
        price = info.get('currentPrice', info.get('previousClose', 0))
        name = info.get('shortName', ticker_symbol)
        sector = info.get('sector', 'N/A')

        return {
            'Ticker': ticker_symbol,
            'Nome': name,
            'Preço': price,
            'MFI': round(mfi_current, 2),
            'MFI Anterior': round(mfi_prev, 2) if pd.notna(mfi_prev) else None,
            'Status': classification['Status'],
            'Zona': classification['Zona'],
            'Trend': classification['Trend'],
            'Signal': classification['Signal'],
            'OB Cross': classification['OB Cross'],
            'OS Cross': classification['OS Cross'],
            'Volume Médio': int(avg_volume) if pd.notna(avg_volume) else 0,
            'Setor': sector,
        }

    except Exception as e:
        return None


def _calculate_with_retry(ticker_symbol: str, max_retries: int = 3) -> dict | None:
    """Wrap calculate_mfi with exponential backoff retry."""
    for attempt in range(max_retries):
        result = calculate_mfi(ticker_symbol)
        if result is not None:
            return result
        wait = 2 ** attempt
        time.sleep(wait)
    return None


# ─────────────────────────────────────────────
# Batch Runner
# ─────────────────────────────────────────────

def run_mfi_screener(tickers: list[str],
                     progress_callback=None,
                     max_workers: int = 4) -> pd.DataFrame:
    """
    Run MFI screener for a list of tickers.

    Args:
        tickers: List of ticker symbols
        progress_callback: Optional callable(current, total)
        max_workers: Parallel workers (keep low for rate limits)
    """
    results = []
    total = len(tickers)
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {}
        for i, t in enumerate(tickers):
            if i > 0 and i % max_workers == 0:
                time.sleep(1.0)
            future = executor.submit(_calculate_with_retry, t.strip())
            future_to_ticker[future] = t.strip()

        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            completed += 1
            try:
                row = future.result()
                if row is not None:
                    results.append(row)
            except Exception:
                pass

            if progress_callback:
                progress_callback(completed, total)

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df.sort_values('MFI', ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df
