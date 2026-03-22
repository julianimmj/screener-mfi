"""
app.py — Screener MFI "Fluxo Financeiro"
Dashboard profissional para Streamlit Cloud.
Mostra APENAS ativos com crossover de Sobrecompra ou Sobrevenda nos últimos 7 dias.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import json
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta

# ─────────────────────────────────────────
# Page Config
# ─────────────────────────────────────────
st.set_page_config(
    page_title="Screener MFI · Fluxo Financeiro",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────
# Data Paths
# ─────────────────────────────────────────
DATA_DIR = Path(__file__).parent / "data"
CSV_MFI = DATA_DIR / "mfi_screener.csv"
METADATA_FILE = DATA_DIR / "metadata.json"

# Signal recency filter (calendar days)
SIGNAL_MAX_AGE_DAYS = 7

# ─────────────────────────────────────────
# Custom CSS — Premium Dark Theme
# ─────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* ── Hero Header ───────────────────── */
    .hero {
        background: linear-gradient(135deg, #0a1628 0%, #1a2744 40%, #0d2137 70%, #0a1628 100%);
        padding: 2.5rem 3rem;
        border-radius: 20px;
        color: #ffffff;
        margin-bottom: 2rem;
        position: relative;
        overflow: hidden;
        border: 1px solid rgba(0, 200, 255, 0.1);
    }
    .hero::before {
        content: '';
        position: absolute;
        top: -50%;
        right: -20%;
        width: 400px;
        height: 400px;
        background: radial-gradient(circle, rgba(0, 200, 255, 0.08) 0%, transparent 70%);
        border-radius: 50%;
    }
    .hero::after {
        content: '';
        position: absolute;
        bottom: -30%;
        left: -10%;
        width: 300px;
        height: 300px;
        background: radial-gradient(circle, rgba(0, 255, 136, 0.06) 0%, transparent 70%);
        border-radius: 50%;
    }
    .hero h1 {
        margin: 0;
        font-size: 2.6rem;
        font-weight: 800;
        letter-spacing: -1px;
        background: linear-gradient(90deg, #fff 50%, #00c8ff 80%, #00ff88);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .hero .subtitle {
        margin: 0.6rem 0 0;
        font-size: 1.05rem;
        opacity: 0.7;
        font-weight: 300;
    }

    /* ── KPI Cards ─────────────────────── */
    .kpi-card {
        background: linear-gradient(145deg, #111b2e, #0d1926);
        border: 1px solid rgba(0, 200, 255, 0.15);
        border-radius: 16px;
        padding: 1.2rem 1rem;
        text-align: center;
        width: 100%;
        min-height: 110px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        box-shadow: 0 4px 20px rgba(0,0,0,0.4);
        transition: all 0.3s ease;
        margin-bottom: 0.5rem;
    }
    .kpi-card:hover {
        border-color: rgba(0, 200, 255, 0.4);
        box-shadow: 0 6px 25px rgba(0,0,0,0.5);
        transform: translateY(-2px);
    }
    .kpi-card .value {
        font-weight: 800;
        margin: 0;
        line-height: 1.1;
        width: 100%;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }
    .kpi-card .label {
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 1px;
        opacity: 0.7;
        margin-top: 0.5rem;
        color: #aac;
        line-height: 1.2;
    }

    /* ── Clickable KPI Buttons ────────── */
    .stButton > button {
        border-radius: 10px !important;
        font-weight: 600 !important;
        transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1) !important;
    }
    div[data-testid="stColumn"] .stButton button {
        background: rgba(0, 200, 255, 0.1) !important;
        border: 1px solid rgba(0, 200, 255, 0.3) !important;
        color: #fff !important;
        font-size: 0.82rem !important;
        height: 2.5rem !important;
        margin-top: 0 !important;
        width: 100%;
    }
    div[data-testid="stColumn"] .stButton button:hover {
        background: rgba(0, 200, 255, 0.3) !important;
        border-color: #00c8ff !important;
        color: #fff !important;
        transform: translateY(-3px);
        box-shadow: 0 5px 15px rgba(0, 200, 255, 0.3);
    }
    div[data-testid="stColumn"] .stButton button:active {
        transform: translateY(-1px);
    }

    /* ── Section Headers ───────────────── */
    .section-title {
        font-size: 1.3rem;
        font-weight: 700;
        margin: 1.5rem 0 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid rgba(0, 200, 255, 0.2);
        color: #fafafa;
    }

    /* ── Sidebar ───────────────────────── */
    section[data-testid="stSidebar"] > div:first-child {
        background: linear-gradient(180deg, #0a1628 0%, #111b2e 100%);
    }

    /* ── Table polish ──────────────────── */
    div[data-testid="stDataFrame"] {
        border-radius: 12px;
        overflow: hidden;
    }

    /* ── Tab styling ───────────────────── */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.5rem;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 10px;
        padding: 8px 20px;
        font-weight: 600;
    }

    /* ── Data freshness badge ──────────── */
    .freshness {
        background: rgba(0, 200, 255, 0.08);
        border: 1px solid rgba(0, 200, 255, 0.2);
        border-radius: 12px;
        padding: 8px 16px;
        font-size: 0.85rem;
        color: #8899bb;
        display: inline-block;
        margin-bottom: 1rem;
    }
    .freshness b {
        color: #00c8ff;
    }

    /* ── Universe selector cards ────────── */
    .universe-card {
        background: linear-gradient(145deg, #111b2e, #0d1926);
        border: 1px solid rgba(0, 200, 255, 0.12);
        border-radius: 14px;
        padding: 0.9rem 1rem;
        text-align: center;
        transition: all 0.3s ease;
        cursor: pointer;
    }
    .universe-card:hover {
        border-color: rgba(0, 200, 255, 0.4);
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
    }
    .universe-card.active {
        border-color: #00c8ff;
        background: linear-gradient(145deg, #152540, #112035);
        box-shadow: 0 0 20px rgba(0, 200, 255, 0.15);
    }
    .universe-card .icon {
        font-size: 1.8rem;
        margin-bottom: 0.3rem;
    }
    .universe-card .name {
        font-weight: 700;
        font-size: 0.85rem;
        color: #e0e8f0;
    }
    .universe-card .count {
        font-size: 0.7rem;
        color: #6688aa;
        margin-top: 0.2rem;
    }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────
# Hero Header
# ─────────────────────────────────────────
st.markdown("""
<div class="hero">
    <h1>🌊 Screener MFI — Fluxo Financeiro</h1>
    <p class="subtitle">
        Money Flow Index para <b>ações brasileiras</b> e <b>BDRs</b> —
        identifique ativos com <b>crossover</b> de <b>sobrecompra</b> ou <b>sobrevenda</b>
        nos últimos 7 dias.
        Timeframe 8D · Período 4 · OB 86 · OS 24
    </p>
</div>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# Load cached data
# ─────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_cached_data(csv_path: str) -> pd.DataFrame:
    """Load pre-generated CSV data."""
    if os.path.exists(csv_path):
        return pd.read_csv(csv_path)
    return pd.DataFrame()


def get_last_updated() -> str:
    """Read the last update timestamp from metadata."""
    if METADATA_FILE.exists():
        try:
            with open(METADATA_FILE) as f:
                meta = json.load(f)
            dt = datetime.fromisoformat(meta.get("last_updated", ""))
            return dt.strftime("%d/%m/%Y às %H:%M UTC")
        except Exception:
            pass
    return "Desconhecido"


def is_bdr(ticker: str) -> bool:
    """Check if a ticker is a BDR."""
    if not ticker.endswith('.SA'):
        return False
    code = ticker.replace('.SA', '')
    if len(code) >= 5:
        suffix = code[-2:]
        if suffix in ('34', '33', '31', '32', '35', '39'):
            return True
    return False


def filter_recent_signals(df: pd.DataFrame, max_age_days: int = SIGNAL_MAX_AGE_DAYS) -> pd.DataFrame:
    """
    Filter DataFrame to only include rows with crossover signals
    that occurred within the last `max_age_days` calendar days.
    """
    if df.empty or 'Signal Date' not in df.columns:
        return pd.DataFrame()

    # Only keep rows that have a signal (OB or OS crossover)
    has_signal = df['Signal Type'].isin(['SOBRECOMPRA', 'SOBREVENDA'])
    df_signals = df[has_signal].copy()

    if df_signals.empty:
        return pd.DataFrame()

    # Filter by recency
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=max_age_days)

    def is_recent(date_str):
        if not date_str or pd.isna(date_str) or str(date_str).strip() == '':
            return False
        try:
            sig_date = pd.to_datetime(str(date_str)).date()
            return sig_date >= cutoff
        except Exception:
            return False

    recent_mask = df_signals['Signal Date'].apply(is_recent)
    return df_signals[recent_mask].copy()


# ─────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🌊 Fluxo Financeiro")
    st.caption("Screener Money Flow Index")
    st.markdown("---")

    # ── Universe selector ──
    st.subheader("🌎 Universo")
    universe = st.radio(
        "Exibir ativos de:",
        ["Todos", "Apenas Ações Brasileiras", "Apenas BDRs"],
        index=0,
        label_visibility="collapsed",
    )

    st.markdown("---")

    # ── Zone filter (sobrecompra / sobrevenda) ──
    st.subheader("📡 Zona de Sinal")
    zone_options = ["Todos (OB + OS)", "🟢 Sobrevenda (MFI ≤ 24)", "🔴 Sobrecompra (MFI ≥ 86)"]

    if "zone_filter" not in st.session_state:
        st.session_state.zone_filter = "Todos (OB + OS)"

    try:
        zone_idx = zone_options.index(st.session_state.zone_filter)
    except ValueError:
        zone_idx = 0

    zone_filter = st.radio(
        "Filtrar por zona:",
        zone_options,
        index=zone_idx,
        label_visibility="collapsed",
    )

    if zone_filter != st.session_state.zone_filter:
        st.session_state.zone_filter = zone_filter
        st.rerun()

    st.markdown("---")

    # MFI Parameters (read-only display)
    st.subheader("⚙️ Parâmetros MFI")
    st.markdown("""
    | Parâmetro | Valor |
    |-----------|-------|
    | Timeframe | **8 dias** |
    | Período | **4** |
    | Sobrecompra | **> 86** |
    | Sobrevenda | **< 24** |
    | Janela de Sinal | **7 dias** |
    """)

    st.markdown("---")

    # Methodology
    with st.expander("📐 Metodologia MFI"):
        st.markdown("""
**Money Flow Index** combina **preço** e **volume** para medir a pressão de compra vs venda.

```
hlc3 = (H + L + C) / 3
Raw MF = hlc3 × Volume
MFR = Σ Positive MF / Σ Negative MF
MFI = 100 - 100 / (1 + MFR)
```

**Sinais de Crossover (fiel ao Pine Script):**
- **OB Cross**: MFI anterior < 86 E MFI atual > 86 → 🔴 Sobrecompra
- **OS Cross**: MFI anterior > 24 E MFI atual < 24 → 🟢 Sobrevenda

⏰ Apenas sinais dos **últimos 7 dias** são exibidos.
        """)

    st.markdown("---")

    # Manual refresh
    st.subheader("🔄 Atualizar Dados")
    refresh_btn = st.button(
        "🔄 Atualizar Dados Agora",
        use_container_width=True,
        type="primary",
        help="Busca dados OHLCV atualizados do Yahoo Finance para todos os ativos."
    )
    st.caption(f"📅 Última atualização: **{get_last_updated()}**")
    st.info(
        "💡 Os dados são atualizados **automaticamente a cada 24 horas** "
        "(às 03:00 BRT via GitHub Actions).\n\n"
        "Use o botão acima apenas se precisar de dados em tempo real.",
        icon="ℹ️"
    )

# ─────────────────────────────────────────
# Load Data
# ─────────────────────────────────────────
csv_path = str(CSV_MFI)

if refresh_btn:
    from update_data import ALL_TICKERS
    from mfi_engine import run_mfi_screener
    st.cache_data.clear()

    progress_bar = st.progress(0, text="⏳ Conectando ao Yahoo Finance...")
    status_text = st.empty()
    total = len(ALL_TICKERS)

    def update_progress(current, total_count):
        pct = current / total_count
        progress_bar.progress(pct, text=f"⏳ Processando {current}/{total_count} ativos...")

    status_text.info(f"🔄 Calculando MFI para {total} ativos...")
    df_all = run_mfi_screener(ALL_TICKERS, progress_callback=update_progress)

    if not df_all.empty:
        os.makedirs(str(DATA_DIR), exist_ok=True)
        df_all.to_csv(csv_path, index=False)

        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        meta = {
            "last_updated": now.isoformat(),
            "tickers_total": total,
            "tickers_ok": len(df_all),
        }
        pd.Series(meta).to_json(str(METADATA_FILE))

        progress_bar.empty()
        status_text.success(f"✅ Dados atualizados! {len(df_all)} ativos processados.")
    else:
        progress_bar.empty()
        status_text.error("❌ Erro ao buscar dados. Tente novamente em alguns minutos.")
        st.stop()
else:
    df_all = load_cached_data(csv_path)

    if df_all.empty:
        st.error(
            "❌ Dados ainda não disponíveis.\n\n"
            "Clique em **🔄 Atualizar Dados Agora** na barra lateral para buscar os dados."
        )
        st.stop()

# ─────────────────────────────────────────
# Pre-process: Add type flag + filter to recent crossover signals ONLY
# ─────────────────────────────────────────
df_all['Tipo'] = df_all['Ticker'].apply(lambda t: 'BDR' if is_bdr(t) else 'Ação')

# ★ CORE FILTER: Only keep assets with crossover signals from last 7 days
total_analyzed = len(df_all)
df = filter_recent_signals(df_all, max_age_days=SIGNAL_MAX_AGE_DAYS)

if df.empty:
    last_updated = get_last_updated()
    st.markdown(
        f'<div class="freshness">📅 Dados de: <b>{last_updated}</b> · '
        f'{total_analyzed} ativos analisados · '
        f'Nenhum crossover de sobrecompra ou sobrevenda nos últimos {SIGNAL_MAX_AGE_DAYS} dias</div>',
        unsafe_allow_html=True
    )
    st.info(
        "📊 **Nenhum sinal de crossover recente.**\n\n"
        f"Nenhum ativo apresentou cruzamento de sobrecompra (MFI > 86) ou "
        f"sobrevenda (MFI < 24) nos últimos {SIGNAL_MAX_AGE_DAYS} dias.\n\n"
        "Os dados são atualizados diariamente — volte mais tarde para checar novos sinais."
    )
    st.stop()

# Tag zone
df['Zona Signal'] = df['Signal Type'].apply(
    lambda v: '🟢 Sobrevenda' if v == 'SOBREVENDA' else '🔴 Sobrecompra'
)

# Format signal date for display
df['Data do Sinal'] = df['Signal Date'].apply(
    lambda d: pd.to_datetime(str(d)).strftime('%d/%m/%Y') if d and str(d).strip() else '–'
)

# Show data freshness
last_updated = get_last_updated()
n_acoes = len(df[df['Tipo'] == 'Ação'])
n_bdrs = len(df[df['Tipo'] == 'BDR'])
n_ob = int((df['Signal Type'] == 'SOBRECOMPRA').sum())
n_os = int((df['Signal Type'] == 'SOBREVENDA').sum())

st.markdown(
    f'<div class="freshness">📅 Dados de: <b>{last_updated}</b> · '
    f'{total_analyzed} ativos analisados · '
    f'<b>{len(df)}</b> crossovers nos últimos {SIGNAL_MAX_AGE_DAYS} dias '
    f'({n_ob} sobrecompra + {n_os} sobrevenda)</div>',
    unsafe_allow_html=True
)

# ─────────────────────────────────────────
# Apply Universe Filter
# ─────────────────────────────────────────
if universe == "Apenas Ações Brasileiras":
    df = df[df['Tipo'] == 'Ação'].copy()
elif universe == "Apenas BDRs":
    df = df[df['Tipo'] == 'BDR'].copy()

if df.empty:
    st.info("Nenhum ativo encontrado para esse universo com crossover recente.")
    st.stop()

# ─────────────────────────────────────────
# Apply Zone Filter
# ─────────────────────────────────────────
view_zone = st.session_state.zone_filter

if "Sobrevenda" in view_zone:
    filtered = df[df['Signal Type'] == 'SOBREVENDA'].copy()
    filtered.sort_values('MFI', ascending=True, inplace=True)
elif "Sobrecompra" in view_zone:
    filtered = df[df['Signal Type'] == 'SOBRECOMPRA'].copy()
    filtered.sort_values('MFI', ascending=False, inplace=True)
else:
    # "Todos (OB + OS)"
    filtered = df.copy()
    filtered.sort_values('MFI', ascending=True, inplace=True)

filtered.reset_index(drop=True, inplace=True)

# ─────────────────────────────────────────
# KPI Cards
# ─────────────────────────────────────────
n_total_signals = len(df)
n_sobrecompra = int((df['Signal Type'] == 'SOBRECOMPRA').sum())
n_sobrevenda = int((df['Signal Type'] == 'SOBREVENDA').sum())

k1, k2, k3, k4 = st.columns(4)


def kpi_box(col, val, label, btn_label, state_val, color="#00c8ff", val_size="2.6rem"):
    col.markdown(f"""
    <div class="kpi-card">
        <p class="value" style="color: {color}; font-size: {val_size}">{val}</p>
        <p class="label">{label}</p>
    </div>
    """, unsafe_allow_html=True)
    if col.button(btn_label, key=f"btn_{label}_{val}", use_container_width=True):
        st.session_state.zone_filter = state_val
        st.rerun()


kpi_box(k1, total_analyzed, "Total Analisados", "🔍 Ver Sinais", "Todos (OB + OS)", "#6688aa", "2rem")
kpi_box(k2, n_sobrecompra, "🔴 Sobrecompra", "🔴 Filtrar", "🔴 Sobrecompra (MFI ≥ 86)", "#ff1744")
kpi_box(k3, n_sobrevenda, "🟢 Sobrevenda", "🟢 Filtrar", "🟢 Sobrevenda (MFI ≤ 24)", "#00e676")
kpi_box(k4, n_total_signals, "Crossovers (7d)", "🔍 Ver Todos", "Todos (OB + OS)", "#ffab00")

st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────
# Tabs
# ─────────────────────────────────────────
tab_ranking, tab_gauge, tab_split = st.tabs([
    "📋 Lista de Sinais", "📊 Gauge por Ativo", "🔀 Ações vs BDRs"
])

# ── Tab 1: Ranking Table ─────────────
with tab_ranking:
    zone_labels = {
        "Todos (OB + OS)": "Sobrecompra + Sobrevenda",
        "🟢 Sobrevenda (MFI ≤ 24)": "Sobrevenda (Entrada de Fluxo)",
        "🔴 Sobrecompra (MFI ≥ 86)": "Sobrecompra (Saída de Fluxo)",
    }
    view_label = zone_labels.get(view_zone, view_zone)
    st.markdown(
        f'<div class="section-title">Crossovers Recentes — {view_label} (últimos {SIGNAL_MAX_AGE_DAYS} dias)</div>',
        unsafe_allow_html=True
    )

    if filtered.empty:
        st.info(f"Nenhum crossover encontrado na zona '{view_label}' nos últimos {SIGNAL_MAX_AGE_DAYS} dias.")
    else:
        display_cols = ['Ticker', 'Nome', 'Preço', 'MFI', 'Zona Signal', 'Data do Sinal', 'Tipo', 'Volume Médio']
        available = [c for c in display_cols if c in filtered.columns]
        display = filtered[available].copy()

        # Format
        display['Preço'] = filtered['Preço'].map(
            lambda v: f"{v:,.2f}" if pd.notna(v) and v else "–"
        )
        display['MFI'] = filtered['MFI'].map(
            lambda v: f"{v:.1f}" if pd.notna(v) else "–"
        )
        display['Volume Médio'] = filtered['Volume Médio'].map(
            lambda v: f"{v:,.0f}" if pd.notna(v) and v > 0 else "–"
        )

        col_config = {
            "Ticker": st.column_config.TextColumn("Ativo", width="small"),
            "Nome": st.column_config.TextColumn("Nome", width="medium"),
            "Preço": st.column_config.TextColumn("Preço", width="small"),
            "MFI": st.column_config.TextColumn("MFI", width="small"),
            "Zona Signal": st.column_config.TextColumn("Zona", width="medium"),
            "Data do Sinal": st.column_config.TextColumn("Data Sinal", width="small"),
            "Tipo": st.column_config.TextColumn("Tipo", width="small"),
            "Volume Médio": st.column_config.TextColumn("Vol. Médio", width="small"),
        }

        st.dataframe(
            display,
            use_container_width=True,
            hide_index=True,
            height=min(700, 35 * len(display) + 38),
            column_config=col_config,
        )

        st.caption(
            f"Exibindo {len(display)} crossovers recentes · "
            f"{total_analyzed} ativos analisados · MFI TF 8D, Per 4 · "
            f"Janela: {SIGNAL_MAX_AGE_DAYS} dias"
        )

# ── Tab 2: Gauge Charts ─────────────
with tab_gauge:
    st.markdown('<div class="section-title">Gauge MFI por Ativo</div>', unsafe_allow_html=True)

    if filtered.empty:
        st.info("Nenhum ativo com crossover recente disponível para exibição.")
    else:
        selected_ticker = st.selectbox(
            "Selecione um ativo com sinal:",
            filtered['Ticker'].tolist(),
            key="gauge_ticker"
        )

        row = filtered[filtered['Ticker'] == selected_ticker].iloc[0]
        mfi_val = row['MFI']
        is_ob = row.get('Signal Type', '') == 'SOBRECOMPRA'

        # Build gauge
        bar_color = '#ff1744' if is_ob else '#00e676'

        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=mfi_val,
            title={
                'text': (
                    f"<b>{selected_ticker}</b><br>"
                    f"<span style='font-size:0.8em;color:#888'>{row.get('Nome', '')}</span>"
                )
            },
            number={'font': {'size': 60, 'color': bar_color}},
            gauge={
                'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "#444", 'dtick': 10},
                'bar': {'color': bar_color, 'thickness': 0.35},
                'bgcolor': 'rgba(0,0,0,0)',
                'borderwidth': 0,
                'steps': [
                    {'range': [0, 24], 'color': 'rgba(0,230,118,0.15)'},
                    {'range': [24, 40], 'color': 'rgba(255,255,255,0.02)'},
                    {'range': [40, 60], 'color': 'rgba(255,255,255,0.02)'},
                    {'range': [60, 86], 'color': 'rgba(255,255,255,0.02)'},
                    {'range': [86, 100], 'color': 'rgba(255,23,68,0.15)'},
                ],
                'threshold': {
                    'line': {'color': '#fff', 'width': 3},
                    'thickness': 0.8,
                    'value': mfi_val
                },
            }
        ))

        fig.add_annotation(x=0.13, y=0.08, text="OS 24", showarrow=False,
                          font=dict(color="#00e676", size=12, family="Inter"))
        fig.add_annotation(x=0.87, y=0.08, text="OB 86", showarrow=False,
                          font=dict(color="#ff1744", size=12, family="Inter"))

        fig.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font={'family': 'Inter', 'color': '#ccc'},
            height=350,
            margin={'l': 30, 'r': 30, 't': 80, 'b': 0},
        )

        st.plotly_chart(fig, use_container_width=True)

        # Info cards below gauge
        g1, g2, g3, g4 = st.columns(4)
        g1.metric("MFI", f"{mfi_val:.1f}")
        g2.metric("Zona", "🔴 Sobrecompra" if is_ob else "🟢 Sobrevenda")
        g3.metric("Fluxo", "Saída" if is_ob else "Entrada")
        g4.metric("Data Sinal", row.get('Data do Sinal', '–'))

        if is_ob:
            st.error(
                "🔴 **Sobrecompra (Crossover)** — MFI cruzou acima de 86. "
                "Indica saída de fluxo financeiro. Possível topo ou distribuição."
            )
        else:
            st.success(
                "🟢 **Sobrevenda (Crossover)** — MFI cruzou abaixo de 24. "
                "Indica entrada de fluxo financeiro. Possível fundo ou acumulação."
            )

# ── Tab 3: Split View ───────────────
with tab_split:
    st.markdown(
        f'<div class="section-title">Ações Brasileiras vs BDRs — Crossovers Recentes ({SIGNAL_MAX_AGE_DAYS}d)</div>',
        unsafe_allow_html=True
    )

    df_acoes = df[df['Tipo'] == 'Ação'].copy()
    df_bdrs = df[df['Tipo'] == 'BDR'].copy()

    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("#### 🇧🇷 Ações Brasileiras")
        if not df_acoes.empty:
            ob_a = df_acoes[df_acoes['Signal Type'] == 'SOBRECOMPRA']
            os_a = df_acoes[df_acoes['Signal Type'] == 'SOBREVENDA']

            ca1, ca2 = st.columns(2)
            ca1.metric("🔴 Sobrecompra", len(ob_a))
            ca2.metric("🟢 Sobrevenda", len(os_a))

            if not os_a.empty:
                st.markdown("**🟢 Sobrevenda (Entrada de Fluxo):**")
                display_os = os_a[['Ticker', 'MFI', 'Zona Signal', 'Data do Sinal']].copy()
                display_os['MFI'] = display_os['MFI'].map(lambda v: f"{v:.1f}")
                st.dataframe(display_os, hide_index=True, use_container_width=True)

            if not ob_a.empty:
                st.markdown("**🔴 Sobrecompra (Saída de Fluxo):**")
                display_ob = ob_a[['Ticker', 'MFI', 'Zona Signal', 'Data do Sinal']].copy()
                display_ob['MFI'] = display_ob['MFI'].map(lambda v: f"{v:.1f}")
                st.dataframe(display_ob, hide_index=True, use_container_width=True)
        else:
            st.info("Nenhuma ação brasileira com crossover recente.")

    with col_b:
        st.markdown("#### 🌐 BDRs")
        if not df_bdrs.empty:
            ob_b = df_bdrs[df_bdrs['Signal Type'] == 'SOBRECOMPRA']
            os_b = df_bdrs[df_bdrs['Signal Type'] == 'SOBREVENDA']

            cb1, cb2 = st.columns(2)
            cb1.metric("🔴 Sobrecompra", len(ob_b))
            cb2.metric("🟢 Sobrevenda", len(os_b))

            if not os_b.empty:
                st.markdown("**🟢 Sobrevenda (Entrada de Fluxo):**")
                display_os_b = os_b[['Ticker', 'MFI', 'Zona Signal', 'Data do Sinal']].copy()
                display_os_b['MFI'] = display_os_b['MFI'].map(lambda v: f"{v:.1f}")
                st.dataframe(display_os_b, hide_index=True, use_container_width=True)

            if not ob_b.empty:
                st.markdown("**🔴 Sobrecompra (Saída de Fluxo):**")
                display_ob_b = ob_b[['Ticker', 'MFI', 'Zona Signal', 'Data do Sinal']].copy()
                display_ob_b['MFI'] = display_ob_b['MFI'].map(lambda v: f"{v:.1f}")
                st.dataframe(display_ob_b, hide_index=True, use_container_width=True)
        else:
            st.info("Nenhum BDR com crossover recente.")


# ─────────────────────────────────────────
# Footer
# ─────────────────────────────────────────
st.markdown("---")
st.markdown(f"""
<div style="text-align:center; opacity:0.4; font-size:0.8rem; padding: 1rem 0">
    <b>Screener MFI "Fluxo Financeiro"</b> · Dados via Yahoo Finance (atualização diária) ·
    <a href="https://github.com/julianimmj/screener-mfi" target="_blank" style="color:#00c8ff">github.com/julianimmj</a><br>
    Indicador: Money Flow Index (TF 8D · Per 4 · OB 86 · OS 24) ·
    Crossovers dos últimos {SIGNAL_MAX_AGE_DAYS} dias apenas
</div>
""", unsafe_allow_html=True)
