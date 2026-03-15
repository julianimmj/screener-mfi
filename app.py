"""
app.py — Screener MFI "Fluxo Financeiro"
Dashboard profissional para Streamlit Cloud.
Dados carregados de CSV pré-gerado (atualizado diariamente via GitHub Actions).
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import json
import os
from pathlib import Path
from datetime import datetime, timezone

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
    .kpi-container {
        display: flex;
        gap: 1rem;
        margin-bottom: 2rem;
    }
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

    /* ── Signal badges ─────────────────── */
    .signal-entrada {
        background: linear-gradient(135deg, rgba(0,230,118,0.2), rgba(0,230,118,0.05));
        border: 1px solid rgba(0,230,118,0.4);
        color: #00e676;
        padding: 4px 12px;
        border-radius: 8px;
        font-weight: 700;
        font-size: 0.8rem;
        display: inline-block;
    }
    .signal-saida {
        background: linear-gradient(135deg, rgba(255,23,68,0.2), rgba(255,23,68,0.05));
        border: 1px solid rgba(255,23,68,0.4);
        color: #ff1744;
        padding: 4px 12px;
        border-radius: 8px;
        font-weight: 700;
        font-size: 0.8rem;
        display: inline-block;
    }
    .signal-neutro {
        background: rgba(255,255,255,0.05);
        border: 1px solid rgba(255,255,255,0.15);
        color: #888;
        padding: 4px 12px;
        border-radius: 8px;
        font-weight: 600;
        font-size: 0.8rem;
        display: inline-block;
    }

    /* ── MFI Meter bar ─────────────────── */
    .mfi-bar {
        height: 8px;
        border-radius: 4px;
        background: linear-gradient(90deg, #00e676, #ffab00, #ff1744);
        position: relative;
        margin: 0.5rem 0;
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
        identifique <b>entrada</b> e <b>saída</b> de fluxo financeiro em tempo real.
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
    """Check if a ticker is a BDR (ends with 34.SA, 33.SA, 31.SA, etc)."""
    if not ticker.endswith('.SA'):
        return False
    code = ticker.replace('.SA', '')
    # BDRs typically end in 34, 33, 31, 32, 35, 39
    if len(code) >= 5:
        suffix = code[-2:]
        if suffix in ('34', '33', '31', '32', '35', '39'):
            return True
    return False


# ─────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🌊 Fluxo Financeiro")
    st.caption("Screener Money Flow Index")
    st.markdown("---")

    # Market toggle
    st.subheader("🌎 Universo")
    universe = st.radio(
        "Exibir:",
        ["Todos", "🇧🇷 Apenas Ações BR", "🌐 Apenas BDRs"],
        index=0,
    )

    st.markdown("---")

    # Signal filter
    st.subheader("📡 Filtro de Fluxo")
    signal_options = ["Todos", "🟢 Entrada", "🔴 Saída", "⚪ Neutro"]

    if "signal_filter" not in st.session_state:
        st.session_state.signal_filter = "Todos"

    try:
        signal_idx = signal_options.index(st.session_state.signal_filter)
    except ValueError:
        signal_idx = 0

    signal_filter = st.radio(
        "Filtrar por sinal:",
        signal_options,
        index=signal_idx,
    )

    if signal_filter != st.session_state.signal_filter:
        st.session_state.signal_filter = signal_filter
        st.rerun()

    st.markdown("---")

    # MFI Parameters (read-only display)
    st.subheader("⚙️ Parâmetros MFI")
    st.markdown("""
    | Parâmetro | Valor |
    |-----------|-------|
    | Timeframe | **8 dias** |
    | Período | **4** |
    | Sobrecompra | **≥ 86** |
    | Sobrevenda | **≤ 24** |
    """)

    st.markdown("---")

    # Methodology
    with st.expander("📐 Metodologia MFI"):
        st.markdown("""
**Money Flow Index** combina **preço** e **volume** em um único indicador, medindo a pressão de compra vs venda.

```
hlc3 = (H + L + C) / 3
Raw MF = hlc3 × Volume
MFR = Σ Positive MF / Σ Negative MF
MFI = 100 - 100 / (1 + MFR)
```

**Zonas:**
- **> 60**: Tendência de Alta (fluxo positivo)
- **40–60**: Transição / Consolidação
- **< 40**: Tendência de Baixa (fluxo negativo)

**Sinais:**
- **≥ 86**: Sobrecompra → possível saída
- **≤ 24**: Sobrevenda → possível entrada
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
    df = run_mfi_screener(ALL_TICKERS, progress_callback=update_progress)

    if not df.empty:
        os.makedirs(str(DATA_DIR), exist_ok=True)
        df.to_csv(csv_path, index=False)

        # Update metadata
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        meta = {
            "last_updated": now.isoformat(),
            "tickers_total": total,
            "tickers_ok": len(df),
        }
        pd.Series(meta).to_json(str(METADATA_FILE))

        progress_bar.empty()
        status_text.success(f"✅ Dados atualizados! {len(df)} ativos processados.")
    else:
        progress_bar.empty()
        status_text.error("❌ Erro ao buscar dados. Tente novamente em alguns minutos.")
        st.stop()
else:
    df = load_cached_data(csv_path)

    if df.empty:
        st.error(
            "❌ Dados ainda não disponíveis.\n\n"
            "Clique em **🔄 Atualizar Dados Agora** na barra lateral para buscar os dados."
        )
        st.stop()

# Add BDR flag
df['Tipo'] = df['Ticker'].apply(lambda t: 'BDR' if is_bdr(t) else 'Ação')

# Show data freshness
last_updated = get_last_updated()
n_acoes = len(df[df['Tipo'] == 'Ação'])
n_bdrs = len(df[df['Tipo'] == 'BDR'])
st.markdown(
    f'<div class="freshness">📅 Dados de: <b>{last_updated}</b> · '
    f'{len(df)} ativos ({n_acoes} ações + {n_bdrs} BDRs) · '
    f'Timeframe 8D · Período 4</div>',
    unsafe_allow_html=True
)

# ─────────────────────────────────────────
# Apply Universe Filter
# ─────────────────────────────────────────
if universe == "🇧🇷 Apenas Ações BR":
    df = df[df['Tipo'] == 'Ação'].copy()
elif universe == "🌐 Apenas BDRs":
    df = df[df['Tipo'] == 'BDR'].copy()

if df.empty:
    st.info("Nenhum ativo encontrado para esse universo.")
    st.stop()

# ─────────────────────────────────────────
# Apply Signal Filter
# ─────────────────────────────────────────
view_filter = st.session_state.signal_filter

if "Entrada" in view_filter:
    filtered = df[df['Signal'] == 'ENTRADA'].copy()
elif "Saída" in view_filter:
    filtered = df[df['Signal'] == 'SAÍDA'].copy()
elif "Neutro" in view_filter:
    filtered = df[df['Signal'] == 'NEUTRO'].copy()
else:
    filtered = df.copy()

filtered.reset_index(drop=True, inplace=True)

# ─────────────────────────────────────────
# KPI Cards
# ─────────────────────────────────────────
n_total = len(df)
n_entrada = int((df['Signal'] == 'ENTRADA').sum())
n_saida = int((df['Signal'] == 'SAÍDA').sum())
n_neutro = int((df['Signal'] == 'NEUTRO').sum())

# Find extreme values
if not df.empty:
    ob_count = int((df['MFI'] >= 86).sum())
    os_count = int((df['MFI'] <= 24).sum())

k1, k2, k3, k4, k5 = st.columns(5)


def kpi_box(col, val, label, btn_label, state_val, color="#00c8ff", val_size="2.4rem"):
    col.markdown(f"""
    <div class="kpi-card">
        <p class="value" style="color: {color}; font-size: {val_size}">{val}</p>
        <p class="label">{label}</p>
    </div>
    """, unsafe_allow_html=True)
    if col.button(btn_label, key=f"btn_{label}_{val}", use_container_width=True):
        st.session_state.signal_filter = state_val
        st.rerun()


kpi_box(k1, n_total, "Analisados", "🔍 Ver Todos", "Todos")
kpi_box(k2, n_entrada, "🟢 Entrada de Fluxo", "🟢 Filtrar", "🟢 Entrada", "#00e676")
kpi_box(k3, n_saida, "🔴 Saída de Fluxo", "🔴 Filtrar", "🔴 Saída", "#ff1744")
kpi_box(k4, n_neutro, "⚪ Neutro", "⚪ Filtrar", "⚪ Neutro", "#888")
kpi_box(k5, f"{ob_count} OB / {os_count} OS", "Extremos", "🔍 Ver Todos", "Todos", "#ffab00", "1.6rem")

st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────
# Tabs
# ─────────────────────────────────────────
tab_ranking, tab_gauge, tab_split, tab_dist = st.tabs([
    "📋 Ranking MFI", "📊 Gauge por Ativo", "🔀 Ações vs BDRs", "📈 Distribuição"
])

# ── Tab 1: Ranking Table ─────────────
with tab_ranking:
    label_map = {
        "🟢 Entrada": "Entrada de Fluxo",
        "🔴 Saída": "Saída de Fluxo",
        "⚪ Neutro": "Neutro",
        "Todos": "Todos",
    }
    view_label = label_map.get(view_filter, view_filter)
    st.markdown(
        f'<div class="section-title">Ranking por MFI — {view_label}</div>',
        unsafe_allow_html=True
    )

    if filtered.empty:
        st.info(f"Nenhum ativo encontrado com o filtro '{view_filter}'.")
    else:
        # Prepare display DataFrame
        display_cols = ['Ticker', 'Nome', 'Preço', 'MFI', 'Status', 'Signal', 'Zona', 'Tipo', 'Volume Médio']
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
            "Status": st.column_config.TextColumn("Status", width="medium"),
            "Signal": st.column_config.TextColumn("Sinal", width="small"),
            "Zona": st.column_config.TextColumn("Zona", width="medium"),
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

        st.caption(f"Exibindo {len(display)} de {n_total} ativos · MFI com Timeframe 8D, Período 4")

# ── Tab 2: Gauge Charts ─────────────
with tab_gauge:
    st.markdown('<div class="section-title">Gauge MFI por Ativo</div>', unsafe_allow_html=True)

    if filtered.empty:
        st.info("Nenhum ativo disponível para exibição.")
    else:
        # Ativo selector
        selected_ticker = st.selectbox(
            "Selecione um ativo:",
            filtered['Ticker'].tolist(),
            key="gauge_ticker"
        )

        row = filtered[filtered['Ticker'] == selected_ticker].iloc[0]
        mfi_val = row['MFI']

        # Build gauge
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=mfi_val,
            title={'text': f"<b>{selected_ticker}</b><br><span style='font-size:0.8em;color:#888'>{row.get('Nome', '')}</span>"},
            number={'font': {'size': 56, 'color': '#fff'}},
            gauge={
                'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "#444", 'dtick': 10},
                'bar': {'color': '#00c8ff', 'thickness': 0.3},
                'bgcolor': 'rgba(0,0,0,0)',
                'borderwidth': 0,
                'steps': [
                    {'range': [0, 24], 'color': 'rgba(0,230,118,0.15)'},
                    {'range': [24, 40], 'color': 'rgba(255,171,0,0.08)'},
                    {'range': [40, 60], 'color': 'rgba(255,255,255,0.03)'},
                    {'range': [60, 86], 'color': 'rgba(255,171,0,0.08)'},
                    {'range': [86, 100], 'color': 'rgba(255,23,68,0.15)'},
                ],
                'threshold': {
                    'line': {'color': '#fff', 'width': 3},
                    'thickness': 0.8,
                    'value': mfi_val
                },
            }
        ))

        # Add reference lines as annotations
        fig.add_annotation(x=0.13, y=0.08, text="OS 24", showarrow=False,
                          font=dict(color="#00e676", size=11))
        fig.add_annotation(x=0.87, y=0.08, text="OB 86", showarrow=False,
                          font=dict(color="#ff1744", size=11))
        fig.add_annotation(x=0.35, y=-0.05, text="40", showarrow=False,
                          font=dict(color="#ffab00", size=10))
        fig.add_annotation(x=0.65, y=-0.05, text="60", showarrow=False,
                          font=dict(color="#ffab00", size=10))

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
        g2.metric("Status", row['Status'])
        g3.metric("Sinal", row['Signal'])
        g4.metric("Zona", row.get('Trend', '–'))

        # OB/OS cross alert
        if row.get('OB Cross', False):
            st.warning("⚠️ **Cruzamento de Sobrecompra detectado!** MFI acabou de cruzar acima de 86.")
        if row.get('OS Cross', False):
            st.success("✅ **Cruzamento de Sobrevenda detectado!** MFI acabou de cruzar abaixo de 24.")

# ── Tab 3: Split View ───────────────
with tab_split:
    st.markdown('<div class="section-title">Ações BR vs BDRs — Comparativo de Fluxo</div>', unsafe_allow_html=True)

    df_acoes = df[df['Tipo'] == 'Ação'].copy()
    df_bdrs = df[df['Tipo'] == 'BDR'].copy()

    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("#### 🇧🇷 Ações Brasileiras")
        if not df_acoes.empty:
            # Stats
            entrada_a = int((df_acoes['Signal'] == 'ENTRADA').sum())
            saida_a = int((df_acoes['Signal'] == 'SAÍDA').sum())
            media_mfi_a = df_acoes['MFI'].mean()

            ca1, ca2, ca3 = st.columns(3)
            ca1.metric("Entrada", f"{entrada_a}")
            ca2.metric("Saída", f"{saida_a}")
            ca3.metric("MFI Médio", f"{media_mfi_a:.1f}")

            # Top 10 entrance
            top_entrada = df_acoes[df_acoes['Signal'] == 'ENTRADA'].head(10)
            if not top_entrada.empty:
                st.markdown("**Top Entrada de Fluxo:**")
                st.dataframe(
                    top_entrada[['Ticker', 'MFI', 'Status', 'Signal']],
                    hide_index=True,
                    use_container_width=True,
                )

            # Top 10 exit
            top_saida = df_acoes[df_acoes['Signal'] == 'SAÍDA'].nlargest(10, 'MFI')
            if not top_saida.empty:
                st.markdown("**Top Saída de Fluxo:**")
                st.dataframe(
                    top_saida[['Ticker', 'MFI', 'Status', 'Signal']],
                    hide_index=True,
                    use_container_width=True,
                )
        else:
            st.info("Nenhuma ação BR disponível.")

    with col_b:
        st.markdown("#### 🌐 BDRs")
        if not df_bdrs.empty:
            entrada_b = int((df_bdrs['Signal'] == 'ENTRADA').sum())
            saida_b = int((df_bdrs['Signal'] == 'SAÍDA').sum())
            media_mfi_b = df_bdrs['MFI'].mean()

            cb1, cb2, cb3 = st.columns(3)
            cb1.metric("Entrada", f"{entrada_b}")
            cb2.metric("Saída", f"{saida_b}")
            cb3.metric("MFI Médio", f"{media_mfi_b:.1f}")

            top_entrada_b = df_bdrs[df_bdrs['Signal'] == 'ENTRADA'].head(10)
            if not top_entrada_b.empty:
                st.markdown("**Top Entrada de Fluxo:**")
                st.dataframe(
                    top_entrada_b[['Ticker', 'MFI', 'Status', 'Signal']],
                    hide_index=True,
                    use_container_width=True,
                )

            top_saida_b = df_bdrs[df_bdrs['Signal'] == 'SAÍDA'].nlargest(10, 'MFI')
            if not top_saida_b.empty:
                st.markdown("**Top Saída de Fluxo:**")
                st.dataframe(
                    top_saida_b[['Ticker', 'MFI', 'Status', 'Signal']],
                    hide_index=True,
                    use_container_width=True,
                )
        else:
            st.info("Nenhum BDR disponível.")

# ── Tab 4: Distribution ─────────────
with tab_dist:
    st.markdown('<div class="section-title">Distribuição do MFI</div>', unsafe_allow_html=True)

    if not df.empty and 'MFI' in df.columns:
        # Histogram
        fig_hist = go.Figure()

        fig_hist.add_trace(go.Histogram(
            x=df['MFI'],
            nbinsx=25,
            marker_color='rgba(0, 200, 255, 0.6)',
            marker_line=dict(color='rgba(0, 200, 255, 0.8)', width=1),
            name='MFI'
        ))

        # Add vertical lines for OS/OB thresholds
        fig_hist.add_vline(x=24, line_dash="dash", line_color="#00e676",
                          annotation_text="OS 24", annotation_font_color="#00e676")
        fig_hist.add_vline(x=86, line_dash="dash", line_color="#ff1744",
                          annotation_text="OB 86", annotation_font_color="#ff1744")
        fig_hist.add_vline(x=40, line_dash="dot", line_color="rgba(255,171,0,0.5)",
                          annotation_text="40", annotation_font_color="rgba(255,171,0,0.7)")
        fig_hist.add_vline(x=60, line_dash="dot", line_color="rgba(255,171,0,0.5)",
                          annotation_text="60", annotation_font_color="rgba(255,171,0,0.7)")

        fig_hist.update_layout(
            xaxis_title="MFI",
            yaxis_title="Número de Ativos",
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font={'family': 'Inter', 'size': 13, 'color': '#ccc'},
            height=400,
            margin={'l': 50, 'r': 30, 't': 30, 'b': 50},
            bargap=0.05,
        )

        st.plotly_chart(fig_hist, use_container_width=True)

        # Scatter: MFI vs Volume
        st.markdown('<div class="section-title">MFI vs Volume Médio</div>', unsafe_allow_html=True)

        scatter_df = df.copy()
        scatter_df['Vol M'] = scatter_df['Volume Médio'] / 1e6

        fig_scatter = px.scatter(
            scatter_df,
            x='MFI',
            y='Vol M',
            color='Signal',
            hover_name='Ticker',
            hover_data={
                'MFI': ':.1f',
                'Vol M': ':.2f',
                'Status': True,
                'Tipo': True,
                'Signal': False,
            },
            color_discrete_map={
                'ENTRADA': '#00e676',
                'SAÍDA': '#ff1744',
                'NEUTRO': '#666',
            },
            size_max=12,
            template='plotly_dark',
        )

        fig_scatter.add_vline(x=24, line_dash="dash", line_color="rgba(0,230,118,0.4)")
        fig_scatter.add_vline(x=86, line_dash="dash", line_color="rgba(255,23,68,0.4)")

        fig_scatter.update_layout(
            xaxis_title="MFI",
            yaxis_title="Volume Médio (Milhões)",
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font={'family': 'Inter', 'size': 13, 'color': '#ccc'},
            legend_title_text="Sinal",
            height=450,
            margin={'l': 50, 'r': 30, 't': 30, 'b': 50},
        )

        st.plotly_chart(fig_scatter, use_container_width=True)

        st.info(
            "💡 **Dica:** Ativos com MFI ≤ 24 e alto volume podem indicar "
            "acumulação institucional. Ativos com MFI ≥ 86 e volume crescente "
            "podem indicar distribuição."
        )


# ─────────────────────────────────────────
# Footer
# ─────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style="text-align:center; opacity:0.4; font-size:0.8rem; padding: 1rem 0">
    <b>Screener MFI "Fluxo Financeiro"</b> · Dados via Yahoo Finance (atualização diária) ·
    <a href="https://github.com/julianimmj/screener-mfi" target="_blank" style="color:#00c8ff">github.com/julianimmj</a><br>
    Indicador: Money Flow Index (TF 8D · Per 4 · OB 86 · OS 24)
</div>
""", unsafe_allow_html=True)
