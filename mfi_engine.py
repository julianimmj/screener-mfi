"""
mfi_engine.py — Motor de Cálculo do Money Flow Index (MFI)

Tradução fiel do Pine Script v3 "Money Flow Index MTF + Alerts" para Python.

Parâmetros configuráveis:
  - Timeframe: 8 dias (resample de barras diárias em blocos de 8 dias)
  - Período (length): 4
  - Sobrecompra (OB): 86
  - Sobrevenda (OS): 24

Detecção de sinais (crossover, fiel ao Pine Script):
  OB Cross: mfi_prev < OB  AND  mfi_current > OB
  OS Cross: mfi_prev > OS  AND  mfi_current < OS

Filtragem: somente sinais cujo crossover ocorreu nos últimos 7 dias.
"""

import yfinance as yf
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─────────────────────────────────────────────
# Configuration — matches user's Pine Script parameters
# ─────────────────────────────────────────────
MFI_LENGTH = 4        # Período do MFI (rolling window)
MFI_TIMEFRAME = 8     # Resample diário → blocos de N dias
MFI_OVERSOLD = 24     # Nível de sobrevenda
MFI_OVERBOUGHT = 86   # Nível de sobrecompra

# How many calendar days of history to download (enough for resampling + warm-up)
HISTORY_DAYS = 200

# Maximum age of a signal (calendar days) to be included in the screener
SIGNAL_MAX_AGE_DAYS = 7


# ─────────────────────────────────────────────
# MFI Calculation
# ─────────────────────────────────────────────

def _resample_ohlcv(df: pd.DataFrame, n_days: int) -> pd.DataFrame:
    """
    Resample daily OHLCV data into n-calendar-day bars.

    TradingView's "8D" custom timeframe uses calendar days (not trading days).
    This matches the user's Pine Script configuration: CustomRes = "8D"
    """
    if df.empty:
        return pd.DataFrame()

    # Resample by calendar days using pandas resample
    resampled = df.resample(f'{n_days}D').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
    })

    # Drop incomplete bars (last bar may be incomplete)
    resampled = resampled.dropna()

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

    # Positive / Negative Money Flow (faithful to Pine Script)
    # positiveMoneyFlow() => hlc3 > hlc3[1] ? rawMoneyFlow : 0
    # negativeMoneyFlow() => hlc3 < hlc3[1] ? rawMoneyFlow : 0
    direction = hlc3.diff()
    positive_mf = raw_mf.where(direction > 0, 0.0)
    negative_mf = raw_mf.where(direction < 0, 0.0)

    # SMA of positive and negative money flow over 'length' periods
    pos_sma = positive_mf.rolling(window=length).mean()
    neg_sma = negative_mf.rolling(window=length).mean()

    # Money Flow Ratio — avoid division by zero
    mfr = pos_sma / neg_sma.replace(0, np.nan)

    # MFI = 100 - 100 / (1 + MFR)
    mfi = 100 - 100 / (1 + mfr)

    return mfi


def _find_crossover_signal(mfi_series: pd.Series,
                           ob: float = MFI_OVERBOUGHT,
                           os_level: float = MFI_OVERSOLD,
                           max_age_days: int = SIGNAL_MAX_AGE_DAYS) -> dict | None:
    """
    Scan the MFI series to find genuine OB/OS crossover events
    that occurred within the last `max_age_days` calendar days.

    Pine Script crossover conditions (faithful translation):
        overbought = moneyFlowIndex[1] < ob  AND  moneyFlowIndex > ob
        oversold   = moneyFlowIndex[1] > os  AND  moneyFlowIndex < os

    IMPORTANT: Pine Script only plots the crossover circle on the bar where
    it happens. It does NOT retroactively show old crossovers. Therefore we
    only consider bars whose date falls within the recency window.

    Returns dict with signal info or None if no recent crossover found.
    """
    clean = mfi_series.dropna()
    if len(clean) < 2:
        return None

    # Calculate the cutoff date — only bars on or after this date are eligible
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)

    # Walk backwards through the series to find the most recent crossover
    values = clean.values
    dates = clean.index

    for i in range(len(values) - 1, 0, -1):
        bar_date = dates[i]

        # Convert bar_date to timezone-aware for comparison
        if hasattr(bar_date, 'tzinfo') and bar_date.tzinfo is not None:
            bar_dt = bar_date
        else:
            bar_dt = pd.Timestamp(bar_date).tz_localize('UTC')

        # Stop scanning if we've gone past the recency window
        if bar_dt < cutoff:
            break

        mfi_curr = values[i]
        mfi_prev = values[i - 1]

        # Overbought crossover: previous < OB AND current > OB (strict, as Pine)
        if mfi_prev < ob and mfi_curr > ob:
            return {
                'signal_type': 'SOBRECOMPRA',
                'signal_label': '🔴 Sobrecompra',
                'signal_flow': 'SAÍDA',
                'signal_date': bar_date,
                'mfi_at_signal': round(float(mfi_curr), 2),
                'mfi_prev_at_signal': round(float(mfi_prev), 2),
                'ob_cross': True,
                'os_cross': False,
            }

        # Oversold crossover: previous > OS AND current < OS (strict, as Pine)
        if mfi_prev > os_level and mfi_curr < os_level:
            return {
                'signal_type': 'SOBREVENDA',
                'signal_label': '🟢 Sobrevenda',
                'signal_flow': 'ENTRADA',
                'signal_date': bar_date,
                'mfi_at_signal': round(float(mfi_curr), 2),
                'mfi_prev_at_signal': round(float(mfi_prev), 2),
                'ob_cross': False,
                'os_cross': True,
            }

    return None


def _classify_trend(mfi_value: float) -> dict:
    """Classify the current MFI value into trend zones (40/60 lines from Pine)."""
    if mfi_value > 60:
        return {"Zona": "🔵 Tendência de Alta", "Trend": "ALTA"}
    elif mfi_value < 40:
        return {"Zona": "🟠 Tendência de Baixa", "Trend": "BAIXA"}
    else:
        return {"Zona": "⚪ Zona de Transição", "Trend": "TRANSIÇÃO"}


# ─────────────────────────────────────────────
# Single Ticker Calculation
# ─────────────────────────────────────────────

def calculate_mfi(ticker_symbol: str) -> dict | None:
    """
    Calculate MFI for a single ticker.

    Downloads daily OHLCV, resamples to MFI_TIMEFRAME-day bars,
    computes MFI with MFI_LENGTH period, and detects crossover signals.

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

        # Get the latest MFI value
        mfi_clean = mfi_series.dropna()
        mfi_current = mfi_clean.iloc[-1]

        # Find the most recent crossover signal
        signal = _find_crossover_signal(mfi_series)

        # Trend zone classification (based on current MFI)
        trend = _classify_trend(mfi_current)

        # Determine signal status
        if signal is not None:
            status = signal['signal_label']
            signal_type = signal['signal_type']
            signal_flow = signal['signal_flow']
            ob_cross = signal['ob_cross']
            os_cross = signal['os_cross']
            # Convert signal date to string for serialization
            sig_date = signal['signal_date']
            if hasattr(sig_date, 'strftime'):
                signal_date_str = sig_date.strftime('%Y-%m-%d')
            else:
                signal_date_str = str(sig_date)[:10]
        else:
            # No crossover found — classify by current value only (no signal)
            if mfi_current > 60:
                status = "🔵 Alta"
                signal_flow = "NEUTRO"
            elif mfi_current < 40:
                status = "🟠 Baixa"
                signal_flow = "NEUTRO"
            else:
                status = "⚪ Transição"
                signal_flow = "NEUTRO"
            signal_type = "NENHUM"
            ob_cross = False
            os_cross = False
            signal_date_str = ""

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
            'Status': status,
            'Zona': trend['Zona'],
            'Trend': trend['Trend'],
            'Signal': signal_flow,
            'Signal Type': signal_type,
            'Signal Date': signal_date_str,
            'OB Cross': ob_cross,
            'OS Cross': os_cross,
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
