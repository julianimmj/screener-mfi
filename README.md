# 🌊 Screener MFI — Fluxo Financeiro

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://share.streamlit.io/julianimmj/screener-mfi/main/app.py)

Aplicação Streamlit para identificar **entrada e saída de fluxo financeiro** em ações brasileiras e BDRs usando o indicador **Money Flow Index (MFI)**.

> O MFI combina **preço e volume** em um único indicador — mede a pressão real de compra vs venda, diferente de indicadores puramente baseados em preço.

---

## 📐 Metodologia

```
hlc3 = (High + Low + Close) / 3
Raw Money Flow = hlc3 × Volume
Money Flow Ratio = Σ Positive MF / Σ Negative MF
MFI = 100 - 100 / (1 + MFR)
```

### Parâmetros

| Parâmetro | Valor |
|-----------|-------|
| Timeframe | 8 dias (resample) |
| Período | 4 |
| Sobrecompra | ≥ 86 |
| Sobrevenda | ≤ 24 |

### Classificação

| MFI | Status | Sinal |
|-----|--------|-------|
| ≥ 86 | 🔴 Sobrecompra | SAÍDA |
| > 60 | 🔵 Alta | ENTRADA |
| 40–60 | ⚪ Transição | NEUTRO |
| < 40 | 🟠 Baixa | SAÍDA |
| ≤ 24 | 🟢 Sobrevenda | ENTRADA |

---

## 🎯 Funcionalidades

- **200+ ações brasileiras** e **110+ BDRs** de maior volume
- **Classificação automática** por entrada/saída de fluxo financeiro
- **Gauge chart** com zonas OB/OS para cada ativo
- **Comparativo Ações vs BDRs** lado a lado
- **Distribuição MFI** com histograma e scatter plot
- **Tema dark profissional** com design premium
- **Atualização diária automática** via GitHub Actions

---

## 🛠️ Deploy no Streamlit Cloud

1. Faça fork ou clone deste repositório
2. Acesse [share.streamlit.io](https://share.streamlit.io)
3. Conecte à sua conta GitHub
4. Selecione este repositório, branch `main`, e arquivo `app.py`
5. Clique em **Deploy** 🚀

### Execução Local

```bash
pip install -r requirements.txt
streamlit run app.py
```

---

## 📂 Estrutura

```
├── app.py                    # Interface Streamlit (Dashboard)
├── mfi_engine.py             # Motor de cálculo MFI
├── update_data.py            # Fetcher diário + lista de tickers
├── requirements.txt          # Dependências Python
├── README.md                 # Documentação
├── .gitignore                # Ignorar cache/temp
├── .streamlit/
│   └── config.toml           # Tema visual (Dark Mode)
├── .github/
│   └── workflows/
│       └── update_data.yml   # GitHub Actions (atualização diária)
└── data/
    ├── mfi_screener.csv      # Dados pré-calculados (auto-gerado)
    └── metadata.json         # Metadados de atualização (auto-gerado)
```

---

**Autor:** [julianimmj](https://github.com/julianimmj) · Indicador: Money Flow Index (Pine Script v3 → Python)
