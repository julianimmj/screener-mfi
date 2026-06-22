"""
backfill_history.py — Script de Backfill do Histórico do Screener MFI.

Recalcula stritamente os crossovers diários (1D) para todos os ativos ativos
nos últimos 60 dias e sobrescreve data/mfi_history.csv com dados puros e limpos.
"""

import os
import sys
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Adicionar o diretório atual ao path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mfi_engine import _compute_mfi, _classify_trend, MFI_LENGTH, MFI_OVERSOLD, MFI_OVERBOUGHT

# Configurações do Backfill
BACKFILL_DAYS = 60
HISTORY_DAYS = 400


def get_active_tickers_info():
    """Recupera tickers ativos, nomes e setores a partir de mfi_screener.csv."""
    screener_path = "data/mfi_screener.csv"
    if not os.path.exists(screener_path):
        print(f"Erro: {screener_path} não encontrado. Execute o update_data.py primeiro.")
        sys.exit(1)
        
    df = pd.read_csv(screener_path)
    tickers_info = {}
    for _, row in df.iterrows():
        tickers_info[row['Ticker']] = {
            'Nome': row['Nome'],
            'Setor': row['Setor']
        }
    return tickers_info


def process_ticker(ticker, info):
    """Calcula crossovers históricos diários para um único ticker."""
    try:
        tk = yf.Ticker(ticker)
        # Baixar dados históricos
        hist = tk.history(period=f"{HISTORY_DAYS}d")
        if hist.empty or len(hist) < MFI_LENGTH + 2:
            return []

        # Limpar zeros/negativos
        for col in ['Open', 'High', 'Low', 'Close']:
            if col in hist.columns:
                hist[col] = hist[col].apply(lambda x: np.nan if x <= 0 else x)
        hist = hist.ffill().bfill()

        # Calcular MFI diário
        mfi_series = _compute_mfi(hist, MFI_LENGTH)
        if mfi_series.empty or mfi_series.dropna().empty:
            return []

        mfi_clean = mfi_series.dropna()
        if len(mfi_clean) < 2:
            return []

        # Limite de corte do histórico (60 dias atrás)
        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=BACKFILL_DAYS)).date()

        crossovers = []
        
        # Percorrer a série a partir do segundo elemento
        for i in range(1, len(mfi_clean)):
            date_val = mfi_clean.index[i]
            
            # Se for datetime do pandas, extrair a data pura
            if hasattr(date_val, 'date'):
                date_only = date_val.date()
            else:
                date_only = pd.to_datetime(date_val).date()

            if date_only < cutoff_date:
                continue

            mfi_curr = mfi_clean.iloc[i]
            mfi_prev = mfi_clean.iloc[i - 1]

            # Detectar crossovers
            is_ob_cross = mfi_prev < MFI_OVERBOUGHT and mfi_curr > MFI_OVERBOUGHT
            is_os_cross = mfi_prev > MFI_OVERSOLD and mfi_curr < MFI_OVERSOLD

            if is_ob_cross or is_os_cross:
                sig_type = 'SOBRECOMPRA' if is_ob_cross else 'SOBREVENDA'
                sig_label = '🔴 Sobrecompra' if is_ob_cross else '🟢 Sobrevenda'
                sig_flow = 'SAÍDA' if is_ob_cross else 'ENTRADA'

                # Tendência no momento do cruzamento
                trend_info = _classify_trend(mfi_curr)

                # Preço e Volume Médio de 20 dias no momento do cruzamento
                price_at_cross = hist['Close'].loc[date_val]
                
                # Volume médio dos 20 dias anteriores até a data do cruzamento
                loc_idx = hist.index.get_loc(date_val)
                start_idx = max(0, loc_idx - 19)
                avg_vol = hist['Volume'].iloc[start_idx:loc_idx + 1].mean()

                crossovers.append({
                    'Ticker': ticker,
                    'Nome': info['Nome'],
                    'Preço': round(float(price_at_cross), 2) if pd.notna(price_at_cross) else 0.0,
                    'MFI': round(float(mfi_curr), 2),
                    'Status': sig_label,
                    'Zona': trend_info['Zona'],
                    'Trend': trend_info['Trend'],
                    'Signal': sig_flow,
                    'Signal Type': sig_type,
                    'Signal Date': date_only.strftime('%Y-%m-%d'),
                    'OB Cross': is_ob_cross,
                    'OS Cross': is_os_cross,
                    'Volume Médio': int(avg_vol) if pd.notna(avg_vol) else 0,
                    'Setor': info['Setor']
                })
        return crossovers
    except Exception as e:
        print(f"Erro ao processar {ticker}: {e}")
        return []


def main():
    print("=== Iniciando Backfill do Histórico MFI (Diário) ===")
    tickers_info = get_active_tickers_info()
    tickers = list(tickers_info.keys())
    print(f"Encontrados {len(tickers)} tickers ativos em mfi_screener.csv")

    all_crossovers = []
    completed = 0
    total = len(tickers)

    # Executar em paralelo
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_ticker, t, tickers_info[t]): t for t in tickers}
        for future in as_completed(futures):
            ticker = futures[future]
            completed += 1
            try:
                results = future.result()
                if results:
                    all_crossovers.extend(results)
                    print(f"[{completed}/{total}] {ticker}: Encontrados {len(results)} crossovers")
                else:
                    print(f"[{completed}/{total}] {ticker}: Nenhum crossover recente")
            except Exception as e:
                print(f"[{completed}/{total}] {ticker}: Falha com erro: {e}")

    # Salvar resultados
    if all_crossovers:
        df_history = pd.DataFrame(all_crossovers)
        
        # Ordenar e salvar
        df_history.sort_values(['Signal Date', 'Ticker'], ascending=[False, True], inplace=True)
        df_history.reset_index(drop=True, inplace=True)
        
        # Garantir diretório data
        os.makedirs("data", exist_ok=True)
        history_path = "data/mfi_history.csv"
        df_history.to_csv(history_path, index=False)
        print(f"\n[OK] Backfill completo! Salvo {len(df_history)} registros em {history_path}")
    else:
        print("\n[WARNING] Nenhum crossover encontrado no período de backfill.")


if __name__ == "__main__":
    main()
