"""
mfi_engine.py — Motor de Cálculo do Money Flow Index (MFI)

Tradução fiel do Pine Script v3 "Money Flow Index MTF + Alerts" para Python.

Parâmetros configuráveis (inputs do usuário no TradingView):
  - Timeframe: Semanal (7D) — "Use Current Chart Resolution?" desmarcado, Custom = "7D"
  - Período (length): 3
  - Sobrecompra (OB): 88
  - Sobrevenda (OS): 18

Detecção de sinais (crossover, fiel ao Pine Script):
  OB Cross: mfi_prev < OB  AND  mfi_current > OB
  OS Cross: mfi_prev > OS  AND  mfi_current < OS
"""

import yfinance as yf
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed


def _reset_yfinance_session():
    """Limpa sessão/crumb do yfinance para forçar reautenticação."""
    try:
        import yfinance.data as _yfdata
        if hasattr(_yfdata, '_crumb') and hasattr(_yfdata, '_cookie'):
            _yfdata._crumb = None
            _yfdata._cookie = None
    except Exception:
        pass
    try:
        if hasattr(yf, 'shared') and hasattr(yf.shared, '_CACHE'):
            yf.shared._CACHE = {}
    except Exception:
        pass

# ─────────────────────────────────────────────
# Configuration — matches user's Pine Script parameters
# ─────────────────────────────────────────────
MFI_LENGTH = 3        # Período do MFI (rolling window) — 3 barras semanais (input do usuário: length = 3)
MFI_TIMEFRAME = 7     # Timeframe semanal (7D) — "Use Current Chart Resolution?" desmarcado, Custom = "7D"
MFI_OVERSOLD = 18     # Nível de sobrevenda para crossover (input do usuário: os = 18)
MFI_OVERBOUGHT = 88   # Nível de sobrecompra para crossover (input do usuário: ob = 88)

# Zone validation thresholds — stricter than crossover thresholds.
# A signal is only ACTIVE if MFI is still in the extreme zone:
MFI_ZONE_OS = 12      # Sobrevenda: sinal ativo somente se MFI ≤ 12
MFI_ZONE_OB = 88      # Sobrecompra: sinal ativo somente se MFI ≥ 88

# How many calendar days of history to download (enough for warm-up)
HISTORY_DAYS = 400

# Maximum age of a signal (calendar days) to be included in the screener
SIGNAL_MAX_AGE_DAYS = 7


# ─────────────────────────────────────────────
# MFI Calculation
# ─────────────────────────────────────────────

def _resample_ohlcv(df: pd.DataFrame, n_days: int) -> pd.DataFrame:
    """
    Resample daily OHLCV data into multi-day bars.
    
    For n_days == 7 (weekly), uses calendar-week resampling ('W') to match
    TradingView's "7D" custom timeframe. The incomplete current week is
    DROPPED because TradingView's security() only uses completed candles.
    
    For other values of n_days, groups by n_days trading days.
    """
    if df.empty:
        return pd.DataFrame()

    if n_days == 7:
        # Calendar-week resample (Mon-Sun, label=Sunday, then shift to Monday)
        resampled = df.resample('W').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum',
        }).dropna()
        
        # Drop the incomplete current week (last bar) if it doesn't contain
        # a full trading week. TradingView only shows completed candles.
        if len(resampled) > 1:
            # Check if the last bar's week is still in progress
            last_bar_date = resampled.index[-1]
            latest_data_date = df.index[-1]
            # If the latest data date is not on a Friday (weekday 4),
            # the current week is incomplete — drop it
            if latest_data_date.weekday() < 4:  # Mon=0..Thu=3 means week not done
                resampled = resampled.iloc[:-1]
        
        return resampled

    # For non-weekly: group by n_days trading days
    df_reset = df.reset_index(drop=True)
    trimmed = df_reset
    
    n_rows = len(trimmed)
    remainder = n_rows % n_days
    if remainder > 0:
        trimmed = trimmed.iloc[remainder:]
    
    trimmed = trimmed.copy()
    block_ids = [i // n_days for i in range(len(trimmed))]
    trimmed['_block'] = block_ids
    
    resampled = trimmed.groupby('_block').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
    })
    
    block_to_idx = trimmed.groupby('_block').apply(lambda x: x.index[-1])
    resampled.index = df.index[block_to_idx.values]
    
    return resampled



def _compute_mfi(ohlcv: pd.DataFrame, length: int = MFI_LENGTH) -> pd.Series:
    """
    Calculate Money Flow Index from OHLCV data.

    FAITHFUL translation of standard MFI:
        hlc3 = (High + Low + Close) / 3
        rawMoneyFlow = hlc3 * Volume
        positiveMoneyFlow = rawMoneyFlow if hlc3 > hlc3[1] else 0
        negativeMoneyFlow = rawMoneyFlow if hlc3 < hlc3[1] else 0
        MFR = SMA(positiveMoneyFlow, length) / SMA(negativeMoneyFlow, length)
        MFI = 100 - 100 / (1 + MFR)
    """
    if len(ohlcv) < length + 1:
        return pd.Series(dtype=float)

    # hlc3 = (H + L + C) / 3
    hlc3 = (ohlcv['High'] + ohlcv['Low'] + ohlcv['Close']) / 3

    # Raw Money Flow
    raw_mf = hlc3 * ohlcv['Volume']

    positive_mf = pd.Series(0.0, index=ohlcv.index)
    negative_mf = pd.Series(0.0, index=ohlcv.index)

    for i in range(1, len(hlc3)):
        if hlc3.iloc[i] > hlc3.iloc[i-1]:
            positive_mf.iloc[i] = raw_mf.iloc[i]
        elif hlc3.iloc[i] < hlc3.iloc[i-1]:
            negative_mf.iloc[i] = raw_mf.iloc[i]

    # SMA of positive and negative money flows
    pos_sma = positive_mf.rolling(window=length).mean()
    neg_sma = negative_mf.rolling(window=length).mean()

    # Calculate MFI, handling division by zero (when neg_sma == 0)
    mfi = 100.0 - 100.0 / (1.0 + (pos_sma / neg_sma))
    mfi = np.where(neg_sma == 0, np.where(pos_sma > 0, 100.0, 50.0), mfi)
    mfi = pd.Series(mfi, index=ohlcv.index)
    mfi[pos_sma.isna() | neg_sma.isna()] = np.nan

    return mfi



def _find_crossover_signal(mfi_series: pd.Series,
                            ob: float = MFI_OVERBOUGHT,
                            os_level: float = MFI_OVERSOLD,
                            max_age_days: int = SIGNAL_MAX_AGE_DAYS) -> dict | None:
    """
    Scan the MFI series to find the MOST RECENT crossover event within max_age_days.
    
    Pine Script crossover conditions (faithful translation):
        overbought = moneyFlowIndex[1] < ob  AND  moneyFlowIndex > ob
        oversold   = moneyFlowIndex[1] > os  AND  moneyFlowIndex < os
    """
    clean = mfi_series.dropna()
    if len(clean) < 2:
        return None

    values = clean.values
    dates = clean.index
    latest_date = dates[-1]

    # Walk backwards to find recent crossover within max_age_days
    for i in range(len(values) - 1, 0, -1):
        bar_date = dates[i]

        # Stop searching if date is older than max_age_days from latest bar
        try:
            dt_bar = pd.to_datetime(bar_date)
            dt_latest = pd.to_datetime(latest_date)
            if (dt_latest - dt_bar).days > max_age_days:
                break
        except Exception:
            pass

        mfi_curr = values[i]
        mfi_prev = values[i - 1]

        # Overbought crossover: previous < OB AND current > OB
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

        # Oversold crossover: previous > OS AND current < OS
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
        hist = pd.DataFrame()
        for attempt in range(2):
            try:
                hist = tk.history(period=f"{HISTORY_DAYS}d")
                if not hist.empty:
                    break
            except Exception:
                _reset_yfinance_session()
                time.sleep(0.5)

        if hist.empty or len(hist) < MFI_TIMEFRAME * (MFI_LENGTH + 2):
            return None

        # Clean negative/zero values (common yfinance adjustment bug)
        for col in ['Open', 'High', 'Low', 'Close']:
            if col in hist.columns:
                hist[col] = hist[col].apply(lambda x: np.nan if x <= 0 else x)
        hist = hist.ffill().bfill()

        # Resample to MFI_TIMEFRAME-day bars (skip if daily)
        if MFI_TIMEFRAME > 1:
            resampled = _resample_ohlcv(hist, MFI_TIMEFRAME)
        else:
            resampled = hist[['Open', 'High', 'Low', 'Close', 'Volume']].copy()

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

        # Strict zone validation: signal is only active if current MFI
        # is STILL in the actual extreme zone.
        # Uses MFI_ZONE_OS (12) and MFI_ZONE_OB (88) — stricter than
        # the crossover thresholds (OS=18, OB=88).
        if signal is not None:
            sig_type = signal['signal_type']
            if sig_type == 'SOBREVENDA' and mfi_current > MFI_ZONE_OS:
                signal = None  # MFI rose above 12 — no longer in extreme oversold
            elif sig_type == 'SOBRECOMPRA' and mfi_current < MFI_ZONE_OB:
                signal = None  # MFI dropped below 88 — no longer in extreme overbought

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

        # Get price and other info with fallback
        price = 0.0
        if not hist.empty and 'Close' in hist.columns:
            price = float(hist['Close'].iloc[-1])

        name = ticker_symbol
        sector = 'N/A'
        try:
            info = tk.info
            if isinstance(info, dict):
                price = info.get('currentPrice', info.get('previousClose', price))
                name = info.get('shortName', ticker_symbol)
                sector = info.get('sector', 'N/A')
        except Exception:
            _reset_yfinance_session()

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
        _reset_yfinance_session()
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
