"""
update_data.py — Daily data fetcher for Screener MFI "Fluxo Financeiro"

Runs via GitHub Actions every day at 06:00 UTC.
Fetches OHLCV for all tickers, calculates MFI,
and saves results to data/mfi_screener.csv.

The Streamlit app reads from this CSV — zero API calls at runtime.
"""

import os
import sys
import time
import pandas as pd
from datetime import datetime, timezone

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mfi_engine import calculate_mfi, _calculate_with_retry

# ─────────────────────────────────────────────
# 200+ Ações Brasileiras (maior volume B3)
# ATUALIZADO: Removidos deslistados/OPA, atualizados tickers renameados
# ─────────────────────────────────────────────
TICKERS_BR = [
    # Blue Chips / Ibovespa core
    "PETR4.SA", "PETR3.SA", "VALE3.SA", "ITUB4.SA", "ITUB3.SA", "BBDC4.SA",
    "BBAS3.SA", "ABEV3.SA", "WEGE3.SA", "RENT3.SA", "SUZB3.SA",
    "B3SA3.SA", "HAPV3.SA", "RDOR3.SA", "RAIL3.SA", "SBSP3.SA",
    "GGBR4.SA", "CSNA3.SA", "CMIG4.SA", "RADL3.SA", "VIVT3.SA",
    "MGLU3.SA", "LREN3.SA", "CSAN3.SA", "BPAC11.SA", "PRIO3.SA",
    "ENEV3.SA", "TOTS3.SA", "VBBR3.SA", "KLBN11.SA", "UGPA3.SA",
    "EQTL3.SA", "CPFE3.SA", "CPLE3.SA", "TAEE11.SA", "CYRE3.SA",
    "MRVE3.SA", "SANB11.SA", "ITSA4.SA", "BBSE3.SA", "PSSA3.SA",
    "IRBR3.SA", "CXSE3.SA", "ASAI3.SA", "HYPE3.SA",
    "GOAU4.SA", "USIM5.SA", "BRKM5.SA", "UNIP6.SA", "FESA4.SA",
    "AURE3.SA", "CSMG3.SA", "SAPR11.SA", "ISAE4.SA", "TIMS3.SA",
    "EZTC3.SA", "DIRR3.SA", "EVEN3.SA", "TEND3.SA", "JHSF3.SA",
    "MULT3.SA", "IGTI11.SA", "ALOS3.SA",
    "LWSA3.SA", "CASH3.SA", "POSI3.SA", "INTB3.SA",
    "FLRY3.SA", "DASA3.SA", "ODPV3.SA",
    "EMBJ3.SA", "HBSA3.SA",  # AZUL4/AZUL54 deslistado - removido
    "MOVI3.SA", "VAMO3.SA", "SIMH3.SA", "SMTO3.SA", "SLCE3.SA",
    "COGN3.SA", "YDUQ3.SA", "ECOR3.SA", "EGIE3.SA",

    # Mid / Small Caps com volume (incluindo novos tickers de alta liquidez)
    "RAPT4.SA", "KLBN4.SA", "KLBN3.SA", "GUAR3.SA", "LIGT3.SA",
    "ABCB4.SA", "BMGB4.SA", "BRSR6.SA", "GRND3.SA", "ALPA4.SA",
    "AXIA3.SA", "RCSL4.SA",
    
    # ETFs de índice
    "SMAL11.SA",
    "MDIA3.SA", "BRAV3.SA", "RECV3.SA", "CMIN3.SA", "CMIG3.SA",
    "ENGI11.SA",
    "NEOE3.SA", "COCE5.SA", "RANI3.SA", "DXCO3.SA",
    "KEPL3.SA", "TUPY3.SA", "POMO4.SA", "RAIZ4.SA",
    "MLAS3.SA", "SEQL3.SA", "ESPA3.SA",
    "GMAT3.SA", "AUAU3.SA", "AZZA3.SA", "CEAB3.SA",
    "ANIM3.SA", "SEER3.SA", "PNVL3.SA", "BLAU3.SA",
    "MATD3.SA", "QUAL3.SA", "ONCO3.SA", "AALR3.SA",
    "CVCB3.SA", "SMFT3.SA",
    "WIZC3.SA", "BBDC3.SA",
    "AGRO3.SA", "CAML3.SA", "JBSS32.SA", "MTRE3.SA",
    "LAVV3.SA", "MDNE3.SA", "PLPL3.SA", "TRIS3.SA",
    "LOGG3.SA",
    "DESK3.SA", "FIQE3.SA",
    "VULC3.SA", "ALPK3.SA", "TECN3.SA", "LEVE3.SA",
    "BRAP4.SA", "OIBR3.SA",
    "TFCO4.SA", "PTBL3.SA", "SOJA3.SA",
    "SBFG3.SA", "VIVR3.SA", "TASA4.SA",
    "BMOB3.SA", "IFCM3.SA", "NGRD3.SA",
    "PGMN3.SA", "AMBP3.SA",
    "CBAV3.SA", "CSED3.SA",
    "ARML3.SA", "LJQQ3.SA",
    "OPCT3.SA", "MEAL3.SA", "LAND3.SA",
    "ROMI3.SA", "FRAS3.SA",
    "TGMA3.SA", "LOGN3.SA", "RSID3.SA",
    "PDTC3.SA", "PMAM3.SA", "DMVF3.SA",
    "AERI3.SA", "SYNE3.SA",
    "MYPK3.SA", "WEST3.SA", "PINE4.SA", "TTEN3.SA",
    "MILS3.SA", "GGPS3.SA", "CURY3.SA",
    "VLID3.SA", "SCAR3.SA", "HBRE3.SA",
    "AMAR3.SA",
]

# ─────────────────────────────────────────────
# BDRs (apenas com volume > 2000 negocios/dia)
# ATUALIZADO: Removidos BDRs com baixo volume
# ─────────────────────────────────────────────
TICKERS_BDR = [
    # High volume tech giants
    "AAPL34.SA", "MSFT34.SA", "GOGL34.SA", "AMZO34.SA",
    "NVDC34.SA", "TSLA34.SA", "AVGO34.SA", "ORCL34.SA",
    "ADBE34.SA", "A1MD34.SA", "ITLC34.SA", "QCOM34.SA",
    "IBMB34.SA", "INTU34.SA", "M1TA34.SA",

    # Financials high volume
    "JPMC34.SA", "BOAC34.SA", "WFCO34.SA", "GSGI34.SA", "MSBR34.SA",
    "CTGP34.SA", "BLAK34.SA", "SCHW34.SA", "AXPB34.SA", "USBC34.SA",
    "VISA34.SA", "MAST34.SA", "PYPL34.SA",

    # Healthcare
    "UNHH34.SA", "JNJB34.SA", "PFIZ34.SA", "ABBV34.SA", "MRCK34.SA",
    "LILY34.SA", "ABTT34.SA",
    "AMGN34.SA", "GILD34.SA",

    # Consumer
    "PGCO34.SA", "COCA34.SA", "PEPB34.SA", "COWC34.SA", "WALM34.SA",
    "MCDC34.SA", "NIKE34.SA", "SBUB34.SA", "TGTB34.SA",
    "HDTH34.SA", "LOWC34.SA",

    # Media / Entertainment
    "DISB34.SA", "NFLX34.SA", "CMCS34.SA", "BKNG34.SA",

    # Energy
    "EXXO34.SA", "CHVX34.SA",

    # Materials / Industrials
    "CATP34.SA", "DEEC34.SA", "HONB34.SA", "UPSS34.SA",
    "LMTB34.SA", "BOGE34.SA", "GEOO34.SA",

    # Other popular BDRs (high volume)
    "BABA34.SA", "BIDU34.SA", "XPBR31.SA",
    "TSMC34.SA", "ASML34.SA",
    "MELI34.SA",
    "ROXO34.SA", "PAGS34.SA",
    "ARMT34.SA", "ACNB34.SA",
    "V1RS34.SA",
    "A1EP34.SA",
    "MDTC34.SA",
]

ALL_TICKERS = TICKERS_BR + TICKERS_BDR


def fetch_all(tickers: list[str], min_volume: int = 0) -> pd.DataFrame:
    """Fetch data for all tickers with delays to avoid rate limiting."""
    results = []
    total = len(tickers)

    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i}/{total}] {ticker}...", end=" ", flush=True)
        row = _calculate_with_retry(ticker, max_retries=3)
        if row is not None:
            if row['Volume Médio'] >= min_volume:
                results.append(row)
                print(f"✓ MFI={row['MFI']}")
            else:
                print(f"✗ (volume {row['Volume Médio']} < {min_volume})")
        else:
            print("✗ (skipped)")

        # Rate limiting — 1.5s between each ticker
        if i < total:
            time.sleep(1.5)

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df.sort_values('MFI', ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def main():
    """Main entry point for the daily data update."""
    os.makedirs("data", exist_ok=True)

    now = datetime.now(timezone.utc)
    print(f"=== Screener MFI — Fluxo Financeiro ===")
    print(f"    Date: {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"    Tickers: {len(ALL_TICKERS)} ({len(TICKERS_BR)} BR + {len(TICKERS_BDR)} BDR)")
    print()

    # ── Fetch BR ──────────────────────
    print("── Fetching MFI data (BR) ──")
    df_br = fetch_all(TICKERS_BR, min_volume=1000000)

    # ── Fetch BDR ─────────────────────
    print("\\n── Fetching MFI data (BDR) ──")
    df_bdr = fetch_all(TICKERS_BDR, min_volume=0)

    # Concatenate results
    if not df_br.empty and not df_bdr.empty:
        df = pd.concat([df_br, df_bdr], ignore_index=True)
    elif not df_br.empty:
        df = df_br
    elif not df_bdr.empty:
        df = df_bdr
    else:
        df = pd.DataFrame()

    if not df.empty:
        df.to_csv("data/mfi_screener.csv", index=False)
        print(f"\n✓ Saved data/mfi_screener.csv ({len(df)} tickers)")
    else:
        print("\n✗ No data fetched")

    # ── Metadata ─────────────────────
    meta = {
        "last_updated": now.isoformat(),
        "tickers_total": len(ALL_TICKERS),
        "tickers_br": len(TICKERS_BR),
        "tickers_bdr": len(TICKERS_BDR),
        "tickers_ok": len(df) if not df.empty else 0,
    }
    pd.Series(meta).to_json("data/metadata.json")
    print(f"\n✓ Metadata saved to data/metadata.json")
    print(f"\n=== Done! ===")


if __name__ == "__main__":
    main()
