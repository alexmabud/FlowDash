from __future__ import annotations


from calendar import monthrange, isleap
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import sqlite3
from flowdash_pages.utils_timezone import hoje_br

from shared.db import ensure_db_path_or_raise, get_conn
from flowdash_pages.lancamentos.pagina.ui_cards_pagina import render_card_row, render_card_rows
from flowdash_pages.dataframes import dataframes as df_utils
from flowdash_pages.dataframes import contas_a_pagar as cap
from flowdash_pages.dre.dre import (
    _calc_mes,
    _listar_anos,
    _load_vars,
    _persist_overrides_to_db,
    _vars_dynamic_overrides,
)
from flowdash_pages.finance_logic import _somar_bancos_totais, _ultimo_caixas_ate
from flowdash_pages.dashboard.prophet_engine import criar_grafico_previsao


# ========================= Helpers gerais =========================
DATE_COLS = [
    "Data",
    "data",
    "data_venda",
    "data_lanc",
    "data_emissao",
    "data_liq",
    "data_liquidacao",
    "data_liquidacao_prevista",
    "data_prevista",
    "data_pagamento",
]
VAL_COLS = [
    "Valor",
    "valor",
    "valor_total",
    "valor_liquido",
    "valor_bruto",
    "valor_previsto",
    "Valor_Recebido",
    "Valor_Recebdio",
    "valor_recebido",
]
PAGAMENTO_COLS = [
    "Forma_Pagamento",
    "forma_pagamento",
    "forma",
    "pagamento",
    "metodo_pagamento",
    "metodo",
    "tipo_pagamento",
    "tipo",
]

MESES_LABELS = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]


def _fmt_currency(v: float) -> str:
    try:
        return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"


def _fmt_percent(v: float) -> str:
    try:
        return f"{float(v):.1f}%"
    except Exception:
        return "0,0%"


def _hover_currency(label_prefix: str | None = None, show_x: bool = False) -> str:
    """
    Gera um hovertemplate padrão para valores em R$.
    Exemplo: "%{x}<br>R$ %{y:,.2f}<extra></extra>"
    """
    base = "R$ %{y:,.2f}"
    parts: List[str] = []
    if show_x:
        parts.append("%{x}")
    if label_prefix:
        parts.append(label_prefix)
    parts.append(base)
    return "<br>".join(parts) + "<extra></extra>"


def _plotly_config(simplified: bool = False) -> dict:
    """
    Retorna a configuração padrão de exibição dos gráficos Plotly.
    Se simplified=True, desativa zoom/pan e esconde botões, mantendo hover.
    """
    if simplified:
        return {
            "displaylogo": False,
            "scrollZoom": False,
            "doubleClick": False,
            "displayModeBar": False,
            "responsive": True,
        }
    return {
        "displaylogo": False,
        "responsive": True,
    }


def _apply_simplified_view(fig, simplified: bool):
    """
    Se simplified=True, trava zoom/pan em todos os eixos, mantendo hover ativo.
    """
    if not simplified or fig is None:
        return fig
    for attr in dir(fig.layout):
        if attr.startswith("xaxis") or attr.startswith("yaxis"):
            axis = getattr(fig.layout, attr, None)
            if axis is not None:
                axis.fixedrange = True
    fig.update_layout(dragmode=False)
    return fig


def _first_existing(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in df.columns:
            return cand
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None


def _resolve_db_path(caminho_banco: Optional[str]) -> str:
    try:
        return ensure_db_path_or_raise(caminho_banco)
    except Exception:
        return ensure_db_path_or_raise(None)


def _load_table(db_path: str, name: str) -> pd.DataFrame:
    try:
        with get_conn(db_path) as conn:
            return pd.read_sql(f'SELECT * FROM "{name}"', conn)
    except Exception:
        return pd.DataFrame()


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["Data", "Valor", "ano", "mes"])
    df_norm = df.copy()
    date_col = _first_existing(df_norm, DATE_COLS)
    val_col = _first_existing(df_norm, VAL_COLS)
    if not date_col:
        date_col = "Data"
        df_norm[date_col] = pd.NaT
    df_norm["Data"] = pd.to_datetime(df_norm[date_col], errors="coerce")
    if not val_col:
        val_col = "Valor"
        df_norm[val_col] = 0.0
    df_norm["Valor"] = pd.to_numeric(df_norm[val_col], errors="coerce").fillna(0.0)
    df_norm["ano"] = df_norm["Data"].dt.year
    df_norm["mes"] = df_norm["Data"].dt.month
    return df_norm


def _load_entradas_saidas(db_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df_entrada_raw = _load_table(db_path, "entrada")
    df_saida_raw = _load_table(db_path, "saida")

    if df_entrada_raw.empty:
        df_entrada_raw = df_utils.carregar_df_entrada()
    if df_saida_raw.empty:
        df_saida_raw = df_utils.carregar_df_saidas()

    df_entrada = _normalize_df(df_entrada_raw)
    df_saida = _normalize_df(df_saida_raw)
    return df_entrada, df_saida


def _load_mercadorias(db_path: str) -> pd.DataFrame:
    df = _load_table(db_path, "mercadorias")
    if df.empty:
        df = df_utils.carregar_df_mercadorias()
    return _normalize_df(df)


def _load_vars_runtime(db_path: str):
    vars_dre = _load_vars(db_path)
    vars_dre = _vars_dynamic_overrides(db_path, vars_dre)
    _persist_overrides_to_db(db_path, vars_dre)
    return vars_dre


def _calc_monthly_metrics(db_path: str, ano: int, vars_dre) -> List[Dict]:
    metrics: List[Dict] = []
    for mes in range(1, 13):
        try:
            metrics.append(_calc_mes(db_path, ano, mes, vars_dre))
        except Exception:
            metrics.append({})
    return metrics


def _growth_mm(metrics: List[Dict], last_month: int) -> float:
    if last_month <= 1:
        return 0.0
    try:
        atual = float(metrics[last_month - 1].get("fat", 0.0) or 0.0)
        anterior = float(metrics[last_month - 2].get("fat", 0.0) or 0.0)
        if anterior == 0:
            return 0.0
        return (atual - anterior) / anterior * 100.0
    except Exception:
        return 0.0


def _formas_pagamento(df_entrada: pd.DataFrame, ano: int) -> pd.DataFrame:
    if df_entrada.empty:
        return pd.DataFrame(columns=["Forma", "Total"])
    col = _first_existing(df_entrada, PAGAMENTO_COLS)
    if not col:
        return pd.DataFrame(columns=["Forma", "Total"])
    df = df_entrada[df_entrada["ano"] == ano].copy()
    if df.empty:
        return pd.DataFrame(columns=["Forma", "Total"])
    df["Forma"] = df[col].astype(str).replace({"": "Não informado"})
    df["valor_base"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)
    agrupado = df.groupby("Forma", dropna=False)["valor_base"].sum().reset_index()
    return agrupado.rename(columns={"valor_base": "Total"})


def _previsto_dx(df_entrada: pd.DataFrame) -> Dict[str, float]:
    if df_entrada.empty:
        return {"d1": 0.0, "d7": 0.0}
    cand_cols = [c for c in df_entrada.columns if ("liq" in c.lower()) or ("prev" in c.lower())]
    if not cand_cols:
        return {"d1": 0.0, "d7": 0.0}
    col = cand_cols[0]
    datas_prev = pd.to_datetime(df_entrada[col], errors="coerce")
    valores = pd.to_numeric(df_entrada["Valor"], errors="coerce").fillna(0.0)
    df = pd.DataFrame({"data_prev": datas_prev, "valor": valores})
    df["data_prev"] = df["data_prev"].dt.date
    df = df.dropna()
    hoje = hoje_br()
    d1 = df[df["data_prev"] == hoje + timedelta(days=1)]["valor"].sum()
    d7 = df[(df["data_prev"] > hoje) & (df["data_prev"] <= hoje + timedelta(days=7))]["valor"].sum()
    return {"d1": float(d1), "d7": float(d7)}


def _cards_row(items: List[Tuple[str, str, Optional[str]]], cols_per_row: int = 4) -> None:
    for i in range(0, len(items), cols_per_row):
        cols = st.columns(cols_per_row)
        for col, (label, value, delta) in zip(cols, items[i : i + cols_per_row]):
            with col:
                st.metric(label, value, delta=delta)


def _inicio_semana_dashboard(ref_day: date) -> date:
    return ref_day - timedelta(days=ref_day.weekday())


def _coluna_dia_dashboard(ref_day: date) -> str:
    dias = ["segunda", "terca", "quarta", "quinta", "sexta", "sabado", "domingo"]
    try:
        return dias[ref_day.weekday()]
    except Exception:
        return "segunda"


def _calcular_percentual_dashboard(valor: float, meta: float) -> float:
    if not meta:
        return 0.0
    try:
        return round((float(valor) / float(meta)) * 100.0, 1)
    except Exception:
        return 0.0


def build_meta_gauge_dashboard(titulo: str, percentual: float, bronze_pct: float, prata_pct: float, valor_label: str) -> go.Figure:
    max_axis = 120.0
    value = float(max(0.0, min(max_axis, percentual)))
    steps = [
        {"range": [0, bronze_pct], "color": "#E53935"},
        {"range": [bronze_pct, prata_pct], "color": "#CD7F32"},
        {"range": [prata_pct, 100], "color": "#C0C0C0"},
        {"range": [100, max_axis], "color": "#FFD700"},
    ]
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=value,
            number={"suffix": "%"},
            title={"text": titulo, "font": {"size": 18}},
            gauge={
                "shape": "angular",
                "axis": {"range": [0, max_axis]},
                "bgcolor": "rgba(0,0,0,0)",
                "bar": {"color": "rgba(0,200,83,0.75)"},
                "steps": steps,
                "borderwidth": 0,
            },
        )
    )
    if valor_label:
        fig.add_annotation(
            x=0.5,
            y=0.0,
            xref="paper",
            yref="paper",
            yanchor="top",
            yshift=-6,
            text=f"<span style='font-size:18px;font-weight:700;color:#00C853'>{valor_label}</span>",
            showarrow=False,
            align="center",
        )
    fig.update_layout(margin=dict(l=10, r=10, t=80, b=80), height=320)
    return fig


def build_meta_mes_gauge_dashboard(pct_meta: float, valor_atual: float, valor_meta: float) -> go.Figure:
    bronze_pct = 75.0
    prata_pct = 87.5
    max_axis = 120.0
    value = float(max(0.0, min(max_axis, pct_meta)))
    steps = [
        {"range": [0, bronze_pct], "color": "#E53935"},
        {"range": [bronze_pct, prata_pct], "color": "#CD7F32"},
        {"range": [prata_pct, 100], "color": "#C0C0C0"},
        {"range": [100, max_axis], "color": "#FFD700"},
    ]
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=value,
            number={"suffix": "%"},
            title={"text": "Meta do Mês", "font": {"size": 18}},
            gauge={
                "shape": "angular",
                "axis": {"range": [0, max_axis]},
                "bgcolor": "rgba(0,0,0,0)",
                "bar": {"color": "rgba(0,200,83,0.75)"},
                "steps": steps,
                "borderwidth": 0,
            },
        )
    )
    valor_label = _fmt_currency(valor_atual)
    if valor_label:
        fig.add_annotation(
            x=0.5,
            y=0.0,
            xref="paper",
            yref="paper",
            yanchor="top",
            yshift=-6,
            text=f"<span style='font-size:18px;font-weight:700;color:#00C853'>{valor_label}</span>",
            showarrow=False,
            align="center",
        )
    fig.update_layout(margin=dict(l=10, r=10, t=80, b=80), height=320)
    return fig


def _calc_meta_mes_dashboard(db_path: str) -> Tuple[float, float, float]:
    hoje = hoje_br()
    ano, mes = hoje.year, hoje.month
    df_ent = _normalize_df(_load_table(db_path, "entrada"))
    if df_ent.empty:
        df_ent = _normalize_df(df_utils.carregar_df_entrada())
    valor_atual = float(df_ent[(df_ent["ano"] == ano) & (df_ent["mes"] == mes)]["Valor"].sum())

    df_meta = _load_table(db_path, "metas")
    if df_meta.empty:
        return 0.0, valor_atual, 0.0
    df_meta["vendedor"] = df_meta.get("vendedor", "LOJA").astype(str).str.upper().fillna("LOJA")
    df_meta = df_meta[df_meta["vendedor"] == "LOJA"].copy()
    if df_meta.empty:
        return 0.0, valor_atual, 0.0
    if "mes" in df_meta.columns:
        df_meta["mes_key"] = df_meta["mes"].astype(str).str[:7]
    else:
        df_meta["mes_key"] = None
    ref_key = f"{ano:04d}-{mes:02d}"
    df_meta = df_meta[df_meta["mes_key"].isna() | (df_meta["mes_key"] <= ref_key)]
    if df_meta.empty:
        return 0.0, valor_atual, 0.0
    df_meta = df_meta.sort_values("mes_key").tail(1)
    meta_cols = ["meta_ouro", "mensal", "meta_mensal", "ouro"]
    valor_meta = 0.0
    for col in meta_cols:
        if col in df_meta.columns:
            try:
                valor_meta = float(pd.to_numeric(df_meta[col], errors="coerce").fillna(0.0).iloc[-1])
                if valor_meta:
                    break
            except Exception:
                continue
    pct_meta = (valor_atual / valor_meta * 100.0) if valor_meta else 0.0
    return pct_meta, valor_atual, valor_meta


def _load_df_metas_dashboard(db_path: str) -> pd.DataFrame:
    df = _load_table(db_path, "metas")
    if df.empty:
        return df
    df["vendedor"] = df.get("vendedor", "LOJA").astype(str).fillna("LOJA")
    if "mes" in df.columns:
        df["mes"] = df["mes"].astype(str).str[:7]
    else:
        df["mes"] = None
    return df


def _metas_vigentes_dashboard(df_metas: pd.DataFrame, ref_day: date) -> pd.DataFrame:
    if df_metas.empty or "vendedor" not in df_metas.columns:
        return pd.DataFrame(columns=["vendedor"])
    df = df_metas.copy()
    df["vendedor"] = df["vendedor"].astype(str).str.strip()
    if "mes" in df.columns:
        df["mes"] = df["mes"].astype(str).str[:7]
    else:
        df["mes"] = None
    ref_key = f"{ref_day:%Y-%m}"

    def _key(row):
        mes = str(row.get("mes") or "")
        return mes if mes else "0000-00"

    df["_mes_key"] = df.apply(_key, axis=1)
    df = df[df["_mes_key"] <= ref_key]
    if df.empty:
        return pd.DataFrame(columns=["vendedor"])

    df = df.sort_values(["vendedor", "_mes_key"]).groupby("vendedor", as_index=False).tail(1)

    def _num(s, default=0.0): return pd.to_numeric(s, errors="coerce").fillna(default)

    if "mensal" not in df.columns:
        df["mensal"] = _num(df.get("meta_mensal", 0.0), 0.0)
    if "semanal" not in df.columns:
        df["semanal"] = _num(df["mensal"], 0.0) * (_num(df.get("perc_semanal", 25.0), 25.0) / 100.0)

    for col in ["segunda", "terca", "quarta", "quinta", "sexta", "sabado", "domingo"]:
        if col not in df.columns:
            pcol = f"perc_{col}"
            df[col] = _num(df["semanal"]) * (_num(df.get(pcol, 0.0), 0.0) / 100.0)

    if "meta_ouro" not in df.columns:
        df["meta_ouro"] = _num(df["mensal"])
    if "meta_prata" not in df.columns:
        df["meta_prata"] = _num(df["mensal"]) * (_num(df.get("perc_prata", 87.5), 87.5) / 100.0)
    if "meta_bronze" not in df.columns:
        df["meta_bronze"] = _num(df["mensal"]) * (_num(df.get("perc_bronze", 75.0), 75.0) / 100.0)

    return df.drop(columns=["_mes_key"], errors="ignore")


def _extrair_metas_completo_dashboard(df_metas_vig: pd.DataFrame, vendedor_upper: str, coluna_dia: str) -> Tuple[float, float, float, float, float, float]:
    metas_v = df_metas_vig[df_metas_vig["vendedor"].astype(str).str.strip().str.upper() == vendedor_upper]
    if metas_v.empty:
        return (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    row = metas_v.iloc[-1]

    def _get(c, d=0.0):
        try:
            return float(row.get(c, d) or 0.0)
        except Exception:
            return d

    return (
        _get(coluna_dia, 0.0),
        _get("semanal", 0.0),
        _get("mensal", 0.0),
        _get("meta_ouro", 0.0),
        _get("meta_prata", 0.0),
        _get("meta_bronze", 0.0),
    )


def render_metas_resumo_dashboard(db_path: str) -> None:
    simplified = bool(st.session_state.get("fd_modo_mobile", False))
    ref_day = hoje_br()
    inicio_sem = _inicio_semana_dashboard(ref_day)
    inicio_mes = ref_day.replace(day=1)
    coluna_dia = _coluna_dia_dashboard(ref_day)

    df_entrada = _normalize_df(_load_table(db_path, "entrada"))
    if df_entrada.empty:
        df_entrada = _normalize_df(df_utils.carregar_df_entrada())
    df_entrada["UsuarioUpper"] = df_entrada.get("Usuario", "LOJA").astype(str).str.upper()
    df_loja = df_entrada[df_entrada["UsuarioUpper"] != "LOJA"]
    mask_dia = df_loja["Data"].dt.date == ref_day
    mask_sem = (df_loja["Data"].dt.date >= inicio_sem) & (df_loja["Data"].dt.date <= ref_day)
    mask_mes = (df_loja["Data"].dt.date >= inicio_mes) & (df_loja["Data"].dt.date <= ref_day)
    valor_dia = df_loja.loc[mask_dia, "Valor"].sum()
    valor_sem = df_loja.loc[mask_sem, "Valor"].sum()
    valor_mes = df_loja.loc[mask_mes, "Valor"].sum()

    df_metas = _load_df_metas_dashboard(db_path)
    df_m_vig = _metas_vigentes_dashboard(df_metas, ref_day)
    m_dia, m_sem, m_mes, ouro, prata, bronz = _extrair_metas_completo_dashboard(df_m_vig, "LOJA", coluna_dia)

    perc_dia = _calcular_percentual_dashboard(valor_dia, m_dia)
    perc_sem = _calcular_percentual_dashboard(valor_sem, m_sem)
    perc_mes = _calcular_percentual_dashboard(valor_mes, m_mes)

    bronze_pct_calc = 75.0 if ouro <= 0 else round(100.0 * (bronz / ouro), 1)
    prata_pct_calc = 87.5 if ouro <= 0 else round(100.0 * (prata / ouro), 1)

    c1, c2, c3 = st.columns(3)
    c1.plotly_chart(
        _apply_simplified_view(
            build_meta_gauge_dashboard("Meta do Dia", perc_dia, bronze_pct_calc, prata_pct_calc, _fmt_currency(valor_dia)),
            simplified,
        ),
        use_container_width=True,
        config=_plotly_config(simplified=simplified),
    )
    c2.plotly_chart(
        _apply_simplified_view(
            build_meta_gauge_dashboard("Meta da Semana", perc_sem, bronze_pct_calc, prata_pct_calc, _fmt_currency(valor_sem)),
            simplified,
        ),
        use_container_width=True,
        config=_plotly_config(simplified=simplified),
    )
    c3.plotly_chart(
        _apply_simplified_view(
            build_meta_gauge_dashboard("Meta do Mês", perc_mes, bronze_pct_calc, prata_pct_calc, _fmt_currency(valor_mes)),
            simplified,
        ),
        use_container_width=True,
        config=_plotly_config(simplified=simplified),
    )

    def _tabela_periodo(label: str, meta_base: float, val: float) -> pd.DataFrame:
        prata_val = meta_base * (prata_pct_calc / 100.0)
        bronze_val = meta_base * (bronze_pct_calc / 100.0)
        dados = {
            "Nível": ["Ouro", "Prata", "Bronze"],
            "Meta": [_fmt_currency(meta_base), _fmt_currency(prata_val), _fmt_currency(bronze_val)],
            "Falta": [
                _fmt_currency(max(meta_base - val, 0.0)),
                _fmt_currency(max(prata_val - val, 0.0)),
                _fmt_currency(max(bronze_val - val, 0.0)),
            ],
        }
        return pd.DataFrame(dados)

    t1, t2, t3 = st.columns(3)
    t1.markdown("**Dia**")
    t1.table(_tabela_periodo("Dia", m_dia, valor_dia))
    t2.markdown("**Semana**")
    t2.table(_tabela_periodo("Semana", m_sem, valor_sem))
    t3.markdown("**Mês**")
    t3.table(_tabela_periodo("Mês", m_mes, valor_mes))


# ========================= Blocos de UI =========================
def render_chips_principais(
    df_entrada: pd.DataFrame,
    db_path: str,
    ano_selecionado: int,
    vars_dre,
) -> None:
    """
    Bloco de KPIs principais do dashboard.
    Total de 10 Indicadores em 'Crescimento & Comparações'.
    """
    hoje = hoje_br()
    ano_atual = hoje.year
    mes_atual = hoje.month
    dia_atual = hoje.day

    if df_entrada is None or df_entrada.empty:
        st.info("Sem dados de entrada para calcular os indicadores principais.")
        return

    df = df_entrada.copy()
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
    df["Data_date"] = df["Data"].dt.date
    df["ano"] = df["Data"].dt.year
    df["mes"] = df["Data"].dt.month

    # --- Helpers Locais ---
    def _sum_ytd(df_in: pd.DataFrame, target_year: int, ref_month: int, ref_day: int) -> float:
        """Soma YTD (Year to Date) até o dia/mês de referência."""
        if target_year not in df_in["ano"].unique():
            return 0.0
        try:
            limit_date = date(target_year, ref_month, ref_day)
        except ValueError:
            limit_date = date(target_year, ref_month, 28)
        start_date = date(target_year, 1, 1)
        mask = (df_in["Data_date"] >= start_date) & (df_in["Data_date"] <= limit_date)
        return float(df_in.loc[mask, "Valor"].sum())

    def _fmt_kpi_html(current: float, ref: float) -> str:
        """Gera HTML com cor condicional."""
        if ref == 0:
            pct = 0.0
            delta = 0.0
            txt_display = "0.0%"
        else:
            pct = (current - ref) / ref * 100.0
            delta = current - ref
            txt_display = f"{pct:.1f}% ({_fmt_currency(delta)})"

        if pct > 0: color = "#2ecc71"
        elif pct < 0: color = "#e74c3c"
        else: color = "#ffffff"

        return f"<span style='color:{color};font-size:18px;font-weight:600;'>{txt_display}</span>"

    def _fmt_ref_chip(val: float) -> str:
        """Gera chip com valor de referência."""
        return f"<span style='background:#262A35; color:#cfd3df; border-radius:4px; padding:2px 6px; font-size:0.75rem; font-weight:600; margin-top:4px;'>Ref: {_fmt_currency(val)}</span>"

    # --- Cálculos Principais (Vendas Atuais) ---
    mask_dia = df["Data_date"] == hoje
    vendas_dia = float(df.loc[mask_dia, "Valor"].sum())
    mask_mes = (df["ano"] == ano_atual) & (df["mes"] == mes_atual)
    vendas_mes = float(df.loc[mask_mes, "Valor"].sum())
    mask_ano = df["ano"] == ano_atual
    vendas_ano = float(df.loc[mask_ano, "Valor"].sum())

    bancos_dict = _somar_bancos_totais(db_path, hoje)
    disp_caixa, disp_caixa2, _ = _ultimo_caixas_ate(db_path, hoje)
    saldo_disp = float(disp_caixa + disp_caixa2 + sum(bancos_dict.values()))

    # --- Identificação de Referências Históricas ---
    ano_prev = ano_atual - 1
    
    # Base histórica (excluindo ano atual para buscar recordes fechados)
    df_hist = df[df["ano"] < ano_atual].copy()

    # 1. MELHOR ANO GERAL
    vendas_por_ano = df_hist.groupby("ano")["Valor"].sum()
    if not vendas_por_ano.empty:
        best_ano = int(vendas_por_ano.idxmax())
        val_best_ano_total = float(vendas_por_ano.max())
    else:
        best_ano = None
        val_best_ano_total = 0.0

    # 2. MELHOR MÊS SAZONAL (O melhor "Dezembro" da história)
    # Filtra apenas o mês atual (ex: Dezembro) de todos os anos históricos
    df_mes_hist_full = df_hist[df_hist["mes"] == mes_atual].copy()
    vendas_por_ano_mes_especifico = df_mes_hist_full.groupby("ano")["Valor"].sum()

    if not vendas_por_ano_mes_especifico.empty:
        best_mes_ano_especifico = int(vendas_por_ano_mes_especifico.idxmax())
        val_best_mes_especifico = float(vendas_por_ano_mes_especifico.max())
    else:
        best_mes_ano_especifico = None
        val_best_mes_especifico = 0.0

    # Cálculo do Período Equivalente para o Melhor Mês Sazonal
    v_mes_best_ano_parcial = 0.0
    if best_mes_ano_especifico:
        try:
            ultimo_dia_mes_recorde = monthrange(best_mes_ano_especifico, mes_atual)[1]
            dia_limite = min(dia_atual, ultimo_dia_mes_recorde)
            mask_periodo_recorde = (
                (df["ano"] == best_mes_ano_especifico) & 
                (df["mes"] == mes_atual) & 
                (df["Data"].dt.day <= dia_limite)
            )
            v_mes_best_ano_parcial = float(df.loc[mask_periodo_recorde, "Valor"].sum())
        except Exception:
            v_mes_best_ano_parcial = 0.0

    # 3. MELHOR MÊS ABSOLUTO (Independente do nome - Ex: Maior faturamento da história da loja)
    # Agrupa por Ano e Mês para achar o maior valor individual
    vendas_por_mes_absoluto = df_hist.groupby(["ano", "mes"])["Valor"].sum()
    
    if not vendas_por_mes_absoluto.empty:
        best_ever_val = float(vendas_por_mes_absoluto.max())
        best_ever_idx = vendas_por_mes_absoluto.idxmax() # Retorna Tupla (ano, mes)
        best_ever_ano, best_ever_mes_num = best_ever_idx
    else:
        best_ever_val = 0.0
        best_ever_ano, best_ever_mes_num = (None, None)

    # Cálculo do Período Equivalente para o Melhor Mês Absoluto
    val_best_ever_periodo = 0.0
    if best_ever_ano:
        try:
            ultimo_dia_mes_ever = monthrange(best_ever_ano, best_ever_mes_num)[1]
            dia_limite_ever = min(dia_atual, ultimo_dia_mes_ever)
            mask_ever_period = (
                (df["ano"] == best_ever_ano) & 
                (df["mes"] == best_ever_mes_num) & 
                (df["Data"].dt.day <= dia_limite_ever)
            )
            val_best_ever_periodo = float(df.loc[mask_ever_period, "Valor"].sum())
        except:
            val_best_ever_periodo = 0.0

    # --- GERAÇÃO DOS 10 INDICADORES ---
    nome_mes_atual = MESES_LABELS[mes_atual - 1]
    
    # [1] Mês vs Mês Anterior
    fat_meses_atual = df[df["ano"] == ano_atual].groupby("mes")["Valor"].sum()
    v_ant_mes = float(fat_meses_atual.get(mes_atual - 1, 0.0)) if mes_atual > 1 else 0.0
    kpi_mm = _fmt_kpi_html(vendas_mes, v_ant_mes)
    idx_mes_ant = (mes_atual - 2) % 12
    lbl_mes_ant = MESES_LABELS[idx_mes_ant] if mes_atual > 1 else "Dez"

    # [2] Mês vs Ano Anterior (Mesmo Período)
    v_mes_ano_ant = 0.0
    if ano_prev in df["ano"].unique():
        try:
            lim = min(dia_atual, monthrange(ano_prev, mes_atual)[1])
            msk = (df["ano"] == ano_prev) & (df["mes"] == mes_atual) & (df["Data"].dt.day <= lim)
            v_mes_ano_ant = float(df.loc[msk, "Valor"].sum())
        except: pass
    kpi_ma = _fmt_kpi_html(vendas_mes, v_mes_ano_ant)

    # [3] Mês vs Melhor Mês Sazonal (ESPECÍFICO) - Total
    # Ex: Dezembro/2025 vs Melhor Dezembro
    kpi_recorde_total = _fmt_kpi_html(vendas_mes, val_best_mes_especifico)
    lbl_best_mes_data = f"{nome_mes_atual}/{best_mes_ano_especifico}" if best_mes_ano_especifico else "N/A"

    # [4] Mês vs Melhor Mês Sazonal (ESPECÍFICO) - Período
    kpi_recorde_periodo = _fmt_kpi_html(vendas_mes, v_mes_best_ano_parcial)

    # [5] Mês vs Melhor Mês da História (ABSOLUTO) - Total
    # Ex: Dezembro/2025 vs O Melhor Mês que já existiu (seja ele Nov/2023, Dez/2022...)
    kpi_ever_total = _fmt_kpi_html(vendas_mes, best_ever_val)
    lbl_best_ever = f"{MESES_LABELS[best_ever_mes_num-1]}/{best_ever_ano}" if best_ever_ano else "N/A"

    # [6] Mês vs Melhor Mês da História (ABSOLUTO) - Período
    kpi_ever_periodo = _fmt_kpi_html(vendas_mes, val_best_ever_periodo)

    # [7] Ano vs Ano Ant (Total)
    v_ano_ant_tot = float(df.loc[df["ano"] == ano_prev, "Valor"].sum()) if ano_prev in df["ano"].unique() else 0.0
    kpi_at = _fmt_kpi_html(vendas_ano, v_ano_ant_tot)

    # [8] Ano vs Ano Ant (YTD)
    v_ano_ant_ytd = _sum_ytd(df, ano_prev, mes_atual, dia_atual)
    kpi_ay = _fmt_kpi_html(vendas_ano, v_ano_ant_ytd)

    # [9] Ano vs Melhor Ano (Total)
    kpi_amt = _fmt_kpi_html(vendas_ano, val_best_ano_total)
    lbl_best_ano = str(best_ano) if best_ano else "N/A"

    # [10] Ano vs Melhor Ano (YTD)
    v_best_ano_ytd = _sum_ytd(df, best_ano, mes_atual, dia_atual) if best_ano else 0.0
    kpi_amy = _fmt_kpi_html(vendas_ano, v_best_ano_ytd)

    # --- Renderização ---
    st.markdown("## Indicadores de Vendas")

    render_card_rows(
        "📊 Vendas & Saldo",
        [
            [("Vendas do dia", vendas_dia, True), ("Vendas do mês", vendas_mes, True), ("Vendas do ano", vendas_ano, True)],
            [("Saldo disponível", saldo_disp, True)],
        ],
    )

    render_card_rows(
        "📈 Crescimento & Comparações",
        [
            # Linha 1: Comparativos Básicos
            [
                (f"Mês vs Mês Anterior ({lbl_mes_ant}) - Total", [kpi_mm, _fmt_ref_chip(v_ant_mes)], False),
                (f"Mês vs Ano Anterior ({ano_prev}) - Período", [kpi_ma, _fmt_ref_chip(v_mes_ano_ant)], False),
            ],
            # Linha 2: Comparativo com Recorde Sazonal (Dez vs Melhor Dez)
            [
                (f"Mês vs Melhor {nome_mes_atual} Histórico ({best_mes_ano_especifico if best_mes_ano_especifico else 'N/A'}) - Total", [kpi_recorde_total, _fmt_ref_chip(val_best_mes_especifico)], False),
                (f"Mês vs Melhor {nome_mes_atual} Histórico ({best_mes_ano_especifico if best_mes_ano_especifico else 'N/A'}) - Período", [kpi_recorde_periodo, _fmt_ref_chip(v_mes_best_ano_parcial)], False),
            ],
             # Linha 3: Comparativo com Recorde Absoluto (Dez vs O Melhor Mês que já existiu)
            [
                (f"Mês vs Recorde Histórico Absoluto ({lbl_best_ever}) - Total", [kpi_ever_total, _fmt_ref_chip(best_ever_val)], False),
                (f"Mês vs Recorde Histórico Absoluto ({lbl_best_ever}) - Período", [kpi_ever_periodo, _fmt_ref_chip(val_best_ever_periodo)], False),
            ],
            # Linha 4: Comparativo Anual
            [
                (f"Ano vs Ano Anterior ({ano_prev}) - Total", [kpi_at, _fmt_ref_chip(v_ano_ant_tot)], False),
                (f"Ano vs Ano Anterior ({ano_prev}) - Período", [kpi_ay, _fmt_ref_chip(v_ano_ant_ytd)], False),
            ],
            # Linha 5: Comparativo Melhor Ano
            [
                (f"Ano vs Melhor Ano ({lbl_best_ano}) - Total", [kpi_amt, _fmt_ref_chip(val_best_ano_total)], False),
                (f"Ano vs Melhor Ano ({lbl_best_ano}) - Período", [kpi_amy, _fmt_ref_chip(v_best_ano_ytd)], False),
            ],
        ]
    )

    # Eficiência
    n_v_mes = int(df.loc[mask_mes].shape[0])
    n_v_ano = int(df.loc[mask_ano].shape[0])
    tk_mes = (vendas_mes / n_v_mes) if n_v_mes > 0 else 0.0
    tk_ano = (vendas_ano / n_v_ano) if n_v_ano > 0 else 0.0

    render_card_rows(
        "🎯 Eficiência de Vendas",
        [
            [("Ticket médio (mês)", tk_mes, True), ("Ticket médio (ano)", tk_ano, True)],
            [("Nº de vendas (mês)", [str(n_v_mes)], False), ("Nº de vendas (ano)", [str(n_v_ano)], False)],
        ],
    )


def render_endividamento(db_path: str) -> None:
    simplified = bool(st.session_state.get("fd_modo_mobile", False))
    db = cap.DB(db_path)
    df_loans_raw = cap._load_loans_raw(db)
    df_loans_view = cap._build_loans_view(df_loans_raw) if not df_loans_raw.empty else pd.DataFrame()

    if df_loans_view.empty:
        st.info("Nenhum empréstimo encontrado.")
        return

    id_col = _first_existing(df_loans_raw, ["id", "Id", "ID"])
    total_contratado_col = _first_existing(df_loans_raw, ["valor_total", "principal", "valor", "Valor_Total"])
    parcelas_pagas_col = _first_existing(df_loans_raw, ["parcelas_pagas", "parcelas_pag", "qtd_parcelas_pagas"])

    mini_cards = []
    total_pago = 0.0
    total_aberto = 0.0

    for _, linha in df_loans_view.iterrows():
        loan_id = str(linha.get("id"))
        saldo = float(linha.get("Saldo Devedor do Empréstimo", 0.0) or 0.0)
        parcela = float(linha.get("Valor da Parcela Mensal", 0.0) or 0.0)
        raw_row = None
        if id_col:
            # compara tudo como string para garantir correspondência mesmo se o id for int no banco
            mask = df_loans_raw[id_col].astype(str) == loan_id
            if mask.any():
                raw_row = df_loans_raw.loc[mask].iloc[0]

        contratado = float(raw_row[total_contratado_col]) if (raw_row is not None and total_contratado_col) else saldo
        parcelas_pagas = float(raw_row[parcelas_pagas_col]) if (raw_row is not None and parcelas_pagas_col) else 0.0
        pago = contratado - saldo if contratado else parcelas_pagas * parcela
        pago = max(pago, 0.0)

        total_pago += pago
        total_aberto += saldo

        pago_pct = (pago / contratado * 100.0) if contratado else 0.0
        aberto_pct = (saldo / contratado * 100.0) if contratado else 0.0
        mini_cards.append(
            {
                "descricao": linha.get("descricao", "Empréstimo"),
                "contratado": contratado,
                "pago": pago,
                "pago_pct": pago_pct,
                "aberto": saldo,
                "aberto_pct": aberto_pct,
            }
        )

    fig_total = go.Figure(
        data=[
            go.Pie(
                labels=["Pago", "Em aberto"],
                values=[total_pago, total_aberto],
                hole=0.6,
                marker=dict(colors=["#27ae60", "#e74c3c"]),
                text=[_fmt_currency(total_pago), _fmt_currency(total_aberto)],
                textinfo="percent+text",
                showlegend=False,
                hovertemplate="%{label}<br>R$ %{value:,.2f}<extra></extra>",
            )
        ]
    )
    fig_total.update_layout(
        height=520,
        margin=dict(l=10, r=10, t=10, b=10),
    )


    st.header("Endividamento")

    # Implementação do layout de 3 colunas para os gráficos de endividamento
    col1, col2, col3 = st.columns(3)

    # Distribui os cards nas colunas
    cols = [col1, col2, col3]
    
    # Se não houver empréstimos, mostra apenas o total (mas a estrutura pede colunas para os gráficos)
    # O user pediu explicitamente para mover e envolver os comandos que exibem esses três gráficos
    # Assumindo que mini_cards tem os itens na ordem desejada ou que devemos preencher as colunas
    
    # Iterar sobre os cards e colocar cada um em uma coluna
    for i, card in enumerate(mini_cards):
        # Garante que não exceda o número de colunas se houver mais cards (embora o user mencione 3)
        if i < 3:
            with cols[i]:
                st.subheader(card["descricao"])
                # Código do gráfico individual
                fig = go.Figure(
                    data=[
                        go.Pie(
                            labels=["Pago", "Em aberto"],
                            values=[card["pago"], card["aberto"]],
                            hole=0.5,
                            marker=dict(colors=["#2ecc71", "#e74c3c"]),
                            text=[_fmt_currency(card["pago"]), _fmt_currency(card["aberto"])],
                            textinfo="percent+text",
                            showlegend=False,
                            hovertemplate="%{label}<br>R$ %{value:,.2f}<extra></extra>",
                        )
                    ]
                )

                fig.update_traces(
                    textposition="inside",
                    textfont_size=11,
                )

                fig.update_layout(
                    height=180,
                    margin=dict(l=0, r=0, t=0, b=0),
                )

                st.plotly_chart(_apply_simplified_view(fig, simplified), use_container_width=True, config=_plotly_config(simplified=simplified))
                st.markdown(f"**Contratado:** {_fmt_currency(card['contratado'])}")

    st.markdown("---") 
    st.markdown("Dívida total em empréstimos")
    
    # Mostra o gráfico total e valor total abaixo
    st.plotly_chart(_apply_simplified_view(fig_total, simplified), use_container_width=True, config=_plotly_config(simplified=simplified))
    valor_total = (total_pago or 0) + (total_aberto or 0)
    st.markdown(f"**{_fmt_currency(valor_total)}**")


def render_graficos_mensais(metrics: List[Dict], ano: int, df_entrada: pd.DataFrame, df_saida: pd.DataFrame, is_mobile: bool = False) -> None:
    st.subheader("Gráficos Mensais")
    meses = list(range(1, 13))
    lucro_labels = [MESES_LABELS[m - 1] for m in meses]
    lucros_raw = [metrics[m - 1].get("lucro_liq", None) if m - 1 < len(metrics) else None for m in meses]
    lucros_plot = []
    for idx, val in enumerate(lucros_raw, start=1):
        if idx < 10:
            lucros_plot.append(None)
            continue
        try:
            v = float(val) if val is not None else None
        except Exception:
            v = None
        lucros_plot.append(v if v not in (None, 0) else None)
    fig_lucro = None
    show_lucro = False
    font_size = 14 if is_mobile else 10
    title_size = 22 if is_mobile else 18
    height = 550 if is_mobile else 350
    if ano == 2025:
        show_lucro = any(v is not None for v in lucros_plot)
        if show_lucro:
            pos_vals = [v if (v is not None and v > 0) else None for v in lucros_plot]
            neg_vals = [v if (v is not None and v < 0) else None for v in lucros_plot]
            pos_text = [_fmt_currency(v) if v is not None else None for v in pos_vals]
            neg_text = [_fmt_currency(v) if v is not None else None for v in neg_vals]

            fig_lucro = go.Figure()
            fig_lucro.add_trace(
                go.Scatter(
                    x=lucro_labels,
                    y=pos_vals,
                    mode="lines+markers+text",
                    name="Lucro Líquido (+)",
                    line=dict(color="#2ecc71"),
                    marker=dict(color="#2ecc71"),
                    text=pos_text,
                    textposition="top center",
                    connectgaps=False,
                    hovertemplate=_hover_currency(show_x=True),
                )
            )
            fig_lucro.add_trace(
                go.Scatter(
                    x=lucro_labels,
                    y=neg_vals,
                    mode="lines+markers+text",
                    name="Lucro Líquido (-)",
                    line=dict(color="#e74c3c"),
                    marker=dict(color="#e74c3c"),
                    text=neg_text,
                    textposition="top center",
                    connectgaps=False,
                    hovertemplate=_hover_currency(show_x=True),
                )
            )
            fig_lucro = _apply_simplified_view(fig_lucro, is_mobile)
            fig_lucro.update_layout(
                title=dict(text=f"Lucro Líquido – {ano}", font=dict(size=title_size)),
                legend=dict(
                    orientation="h",
                    yanchor="top",
                    y=-0.2,
                    xanchor="center",
                    x=0.5,
                ),
                margin=dict(b=80),
                height=height,
                font=dict(size=font_size),
                dragmode="zoom",
                hovermode="x unified",
                showlegend=not is_mobile,
            )
            if is_mobile:
                fig_lucro.update_traces(text=None, texttemplate=None)

    # --- Balanço Mensal com dados reais de entrada/saída ---
    df_ent_ano = df_entrada[df_entrada["ano"] == ano] if not df_entrada.empty else pd.DataFrame(columns=["mes", "Valor"])
    df_sai_ano = df_saida[df_saida["ano"] == ano] if not df_saida.empty else pd.DataFrame(columns=["mes", "Valor"])
    fat_series = df_ent_ano.groupby("mes")["Valor"].sum().reindex(meses).fillna(0.0)
    sai_series = df_sai_ano.groupby("mes")["Valor"].sum().reindex(meses).fillna(0.0)
    fat = fat_series.tolist()
    saidas = sai_series.tolist()
    resultado = [f - s for f, s in zip(fat, saidas)]
    meses_labels = [MESES_LABELS[m - 1] for m in meses]
    df_balanco = pd.DataFrame({"Mês": meses_labels, "Faturamento": fat, "Saída": saidas, "Resultado": resultado})

    res_pos = [v if v > 0 else None for v in resultado]
    res_neg = [v if v < 0 else None for v in resultado]
    res_zero = [v if v == 0 else None for v in resultado]

    fig_balanco = go.Figure()
    fig_balanco.add_trace(
        go.Bar(
            x=df_balanco["Mês"],
            y=df_balanco["Faturamento"],
            name="Entrada",
            marker_color="#2980b9",
            hovertemplate="Entrada: R$ %{y:,.2f}<extra></extra>",
        )
    )
    fig_balanco.add_trace(
        go.Bar(
            x=df_balanco["Mês"],
            y=df_balanco["Saída"],
            name="Saída",
            marker_color="#e67e22",
            hovertemplate="Saída: R$ %{y:,.2f}<extra></extra>",
        )
    )
    fig_balanco.add_trace(
        go.Bar(
            x=df_balanco["Mês"],
            y=res_pos,
            name="Resultado (+)",
            marker_color="#27ae60",
            hovertemplate="Resultado: R$ %{y:,.2f}<extra></extra>",
        )
    )
    fig_balanco.add_trace(
        go.Bar(
            x=df_balanco["Mês"],
            y=res_neg,
            name="Resultado (-)",
            marker_color="#e74c3c",
            hovertemplate="Resultado: R$ %{y:,.2f}<extra></extra>",
        )
    )
    if any(v is not None for v in res_zero):
        fig_balanco.add_trace(
            go.Bar(
                x=df_balanco["Mês"],
                y=res_zero,
                name="Resultado (0)",
                marker_color="#95a5a6",
                hovertemplate="Resultado: R$ %{y:,.2f}<extra></extra>",
            )
        )
    linha_text = [_fmt_currency(v) for v in resultado]
    fig_balanco.add_trace(
        go.Scatter(
            x=df_balanco["Mês"],
            y=df_balanco["Resultado"],
            name="Linha Resultado",
            mode="lines+markers+text",
            line=dict(color="#9b59b6"),
            marker=dict(color="#9b59b6"),
            text=linha_text,
            textposition="top center",
            textfont=dict(size=14, color="#ffffff"),
            hovertemplate=_hover_currency(show_x=True),
        )
    )
    fig_balanco.update_traces(hovertemplate="R$ %{y:,.2f}<extra></extra>")
    fig_balanco.update_layout(
        barmode="group",
        title=dict(text=f"Balanço Mensal – {ano}", font=dict(size=title_size)),
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.25,
            xanchor="center",
            x=0.5,
        ),
        margin=dict(b=90),
        height=height,
        font=dict(size=font_size),
        dragmode="zoom",
        hovermode="x unified",
        showlegend=not is_mobile,
    )
    if is_mobile:
        fig_balanco.update_traces(text=None, texttemplate=None)

    fig_balanco = _apply_simplified_view(fig_balanco, is_mobile)
    if is_mobile:
        with st.container():
            if ano != 2025:
                st.warning("Lucro líquido só está disponível a partir de outubro de 2025. Não há dados consistentes para anos anteriores.")
            else:
                if show_lucro and fig_lucro is not None:
                    fig_lucro = _apply_simplified_view(fig_lucro, is_mobile)
                    st.plotly_chart(fig_lucro, use_container_width=True, config=_plotly_config(simplified=is_mobile))
                else:
                    st.warning("Não há dados de lucro líquido registrados entre outubro e dezembro de 2025.")
        with st.container():
            st.plotly_chart(fig_balanco, use_container_width=True, config=_plotly_config(simplified=is_mobile))
            tabela_mes = pd.DataFrame(
                [fat, saidas, resultado],
                index=["Entrada", "Saída", "Resultado"],
                columns=meses_labels,
            )
            tabela_fmt = (
                tabela_mes.style.format(_fmt_currency).applymap(
                    lambda v: "color:green"
                    if isinstance(v, (int, float)) and v > 0
                    else ("color:red" if isinstance(v, (int, float)) and v < 0 else ""),
                    subset=pd.IndexSlice["Resultado", :],
                )
            )
            st.markdown("**Valores Mensais**")
            st.dataframe(tabela_fmt, use_container_width=True)
    else:
        col1, col2 = st.columns(2)
        with col1:
            if ano != 2025:
                st.warning("Lucro líquido só está disponível a partir de outubro de 2025. Não há dados consistentes para anos anteriores.")
            else:
                if show_lucro and fig_lucro is not None:
                    fig_lucro = _apply_simplified_view(fig_lucro, is_mobile)
                    st.plotly_chart(fig_lucro, use_container_width=True, config=_plotly_config(simplified=is_mobile))
                else:
                    st.warning("Não há dados de lucro líquido registrados entre outubro e dezembro de 2025.")
        with col2:
            with st.container():
                st.plotly_chart(fig_balanco, use_container_width=True, config=_plotly_config(simplified=is_mobile))
                tabela_mes = pd.DataFrame(
                    [fat, saidas, resultado],
                    index=["Entrada", "Saída", "Resultado"],
                    columns=meses_labels,
                )
                tabela_fmt = (
                    tabela_mes.style.format(_fmt_currency).applymap(
                        lambda v: "color:green"
                        if isinstance(v, (int, float)) and v > 0
                        else ("color:red" if isinstance(v, (int, float)) and v < 0 else ""),
                        subset=pd.IndexSlice["Resultado", :],
                    )
                )
                st.markdown("**Valores Mensais**")
                st.dataframe(tabela_fmt, use_container_width=True)


def render_analise_anual(df_entrada: pd.DataFrame, anos_multiselect: List[int], is_mobile: bool = False) -> None:
    st.subheader("Análise Anual")
    if df_entrada.empty:
        st.info("Sem dados de entradas para análise.")
        return

    df_base = df_entrada[df_entrada["ano"].isin(anos_multiselect)].copy()
    if df_base.empty:
        st.info("Nenhum dado para os anos selecionados.")
        return

    df_base["total"] = pd.to_numeric(df_base["Valor"], errors="coerce").fillna(0.0)
    fat_mensal = df_base.groupby(["ano", "mes"])["total"].sum().reset_index()
    fat_mensal["mes_label"] = fat_mensal["mes"].apply(lambda m: MESES_LABELS[m - 1])

    # faturamento anual: soma dos meses por ano
    fat_anual = fat_mensal.groupby("ano")["total"].sum().reset_index()

    # adiciona coluna formatada em R$ para exibir no gráfico
    fat_anual["total_fmt"] = fat_anual["total"].apply(lambda v: f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

    font_size = 14 if is_mobile else 10
    title_size = 22 if is_mobile else 18
    height = 550 if is_mobile else 350

    fig_fat_ano = px.line(
        fat_anual,
        x="ano",
        y="total",
        markers=True,
        title="Faturamento Anual",
        labels={"ano": "Ano", "total": "Faturamento"},
    )

    fig_fat_ano.update_traces(
        mode="lines+markers+text",
        text=fat_anual["total_fmt"],
        textposition="top center",
        cliponaxis=False,
        line=dict(color="#9b59b6"),
        marker=dict(color="#9b59b6"),
    )
    fig_fat_ano.update_xaxes(dtick=1, tickformat="d")
    if is_mobile:
        fig_fat_ano.update_traces(text=None, texttemplate=None)
    fig_fat_ano.update_layout(
        height=height,
        font=dict(size=font_size),
        title=dict(font=dict(size=title_size)),
        dragmode="zoom",
        hovermode="x unified",
        showlegend=not is_mobile,
        margin=dict(t=80, b=40),
    )

    def _calc_mm(df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values("mes")
        df["mm"] = df["total"].pct_change().fillna(0.0) * 100.0
        return df

    ano_atual = max(anos_multiselect) if anos_multiselect else None
    ano_anterior = ano_atual - 1 if ano_atual and (ano_atual - 1) in anos_multiselect else None

    pivot_vals = (
        fat_mensal.pivot(index="ano", columns="mes", values="total")
        .reindex(columns=range(1, 13), fill_value=0.0)
        .fillna(0.0)
    )
    yoy_labels_full = {}
    yoy_colors_full = {}
    if ano_atual is not None and ano_anterior in pivot_vals.index:
        for m in range(1, 13):
            prev = float(pivot_vals.at[ano_anterior, m]) if m in pivot_vals.columns else 0.0
            cur = float(pivot_vals.at[ano_atual, m]) if m in pivot_vals.columns else 0.0
            if prev > 0:
                yoy = (cur / prev - 1.0) * 100.0
                lbl = f"{yoy:+.1f}%".replace(".", ",")
                if yoy > 0:
                    color = "green"
                elif yoy < 0:
                    color = "red"
                else:
                    color = "#ffffff"
            else:
                lbl = ""
                color = "#ffffff"
            yoy_labels_full[m] = lbl
            yoy_colors_full[m] = color

    palette_outros = [
        "#e67e22",  # laranja
        "#1abc9c",  # verde água
        "#e74c3c",  # vermelho
        "#f1c40f",  # amarelo
        "#8e44ad",  # roxo escuro
        "#16a085",  # verde
        "#d35400",  # laranja escuro
        "#2ecc71",  # verde claro
        "#3498db",  # azul claro
        "#9b59b6",  # roxo extra
    ]

    anos_sorted = sorted(anos_multiselect)
    cores_por_ano = {}

    # reserva cores fixas para ano atual e ano anterior
    if ano_atual is not None and ano_atual in anos_sorted:
        cores_por_ano[ano_atual] = "#9b59b6"  # roxo para ano atual

    if ano_anterior is not None and ano_anterior in anos_sorted and ano_anterior not in cores_por_ano:
        cores_por_ano[ano_anterior] = "#2980b9"  # azul para ano anterior

    # atribui cores únicas para os demais anos
    idx_cor = 0
    for a in anos_sorted:
        if a in cores_por_ano:
            continue
        if idx_cor >= len(palette_outros):
            # se acabar a paleta, recomeça (caso raro de muitos anos)
            idx_cor = 0
        cores_por_ano[a] = palette_outros[idx_cor]
        idx_cor += 1

    fig_mm = go.Figure()
    for ano in anos_multiselect:
        df_ano = fat_mensal[fat_mensal["ano"] == ano].sort_values("mes")
        if df_ano.empty:
            continue
        is_atual = ano == ano_atual
        is_prev = ano_anterior is not None and ano == ano_anterior
        color = cores_por_ano.get(ano, "#bdc3c7")
        width = 3 if is_atual else (2 if is_prev else 1)
        msize = 8 if is_atual else (6 if is_prev else 4)
        opacity = 1.0 if is_atual else (0.7 if is_prev else 0.3)
        text_vals = [yoy_labels_full.get(int(m), "") if is_atual else "" for m in df_ano["mes"]]
        text_colors = [yoy_colors_full.get(int(m), "#ffffff") if is_atual else "#ffffff" for m in df_ano["mes"]]
        fig_mm.add_trace(
            go.Scatter(
                x=df_ano["mes_label"],
                y=df_ano["total"],
                name=str(ano),
                mode="lines+markers+text",
                line=dict(color=color, width=width),
                marker=dict(color=color, size=msize),
                opacity=opacity,
                text=text_vals,
                textposition="top center",
                textfont=dict(color=text_colors, size=12),
                hovertemplate=_hover_currency(show_x=True),
            )
        )
    fig_mm.add_hline(y=0, line_color="#888", line_dash="dash", opacity=0.7)
    fig_mm.update_layout(
        title=dict(text="Faturamento Mensal (por ano)", font=dict(size=title_size)),
        xaxis_title="Mês",
        yaxis_title="Faturamento (R$)",
        height=height,
        font=dict(size=font_size),
        dragmode="zoom",
        hovermode="x unified",
        showlegend=not is_mobile,
    )
    if is_mobile:
        fig_mm.update_traces(text=None, texttemplate=None)

    ranking = fat_mensal.copy()
    ranking["label"] = ranking.apply(lambda r: f"{MESES_LABELS[int(r['mes']) - 1]}/{int(r['ano'])}", axis=1)
    top = ranking.sort_values("total", ascending=False).head(8)
    fig_rank = px.bar(top, x="label", y="total", title="Top meses (Faturamento)", labels={"label": "Mês/Ano", "total": "Faturamento"})
    fig_rank.update_traces(hovertemplate=_hover_currency(show_x=True))
    fig_rank.update_traces(hovertemplate=_hover_currency(show_x=True))
    fig_rank.update_layout(
        height=height,
        font=dict(size=font_size),
        title=dict(font=dict(size=title_size)),
        dragmode="zoom",
        hovermode="x unified",
        showlegend=not is_mobile,
    )

    pivot = (
        fat_mensal
        .pivot(index="mes_label", columns="ano", values="total")
        .reindex(index=list(reversed(MESES_LABELS)))
        .fillna(0.0)
    )
    fig_heat = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=[str(c) for c in pivot.columns],
            y=pivot.index,
            colorscale="Blues",
            hoverongaps=False,
        )
    )
    fig_heat.update_traces(hovertemplate="%{y} - %{x}<br>Faturamento: R$ %{z:,.2f}<extra></extra>")
    fig_heat.update_traces(hovertemplate="%{y} - %{x}<br>Faturamento: R$ %{z:,.2f}<extra></extra>")
    fig_heat.update_traces(hovertemplate="%{x} – %{y}<br>R$ %{z:,.2f}<extra></extra>")
    fig_heat.update_traces(hovertemplate="%{x} – %{y}<br>R$ %{z:,.2f}<extra></extra>")
    fig_heat.update_traces(hovertemplate="%{x} – %{y}<br>R$ %{z:,.2f}<extra></extra>")
    fig_heat.update_layout(
        title=dict(text="Heatmap de Faturamento", font=dict(size=title_size)),
        height=height,
        font=dict(size=font_size),
        dragmode="zoom",
        hovermode="x unified",
        showlegend=not is_mobile,
    )

    if is_mobile:
        with st.container():
            fig_fat_ano = _apply_simplified_view(fig_fat_ano, is_mobile)
            st.plotly_chart(fig_fat_ano, use_container_width=True, config=_plotly_config(simplified=is_mobile))
        with st.container():
            fig_mm = _apply_simplified_view(fig_mm, is_mobile)
            st.plotly_chart(fig_mm, use_container_width=True, config=_plotly_config(simplified=is_mobile))
            tabela_mm = (
                fat_mensal.pivot(index="ano", columns="mes_label", values="total")
                .reindex(columns=MESES_LABELS, fill_value=0.0)
                .fillna(0.0)
            )
            tabela_mm = tabela_mm.reset_index().rename(columns={"ano": "Ano"})
            tabela_mm_styled = tabela_mm.style.format(
                {m: _fmt_currency for m in MESES_LABELS}
            )
            st.markdown("**Faturamento Mês a Mês (R$)**")
            st.dataframe(tabela_mm_styled, use_container_width=True)

        with st.container():
            fig_rank = _apply_simplified_view(fig_rank, is_mobile)
            st.plotly_chart(fig_rank, use_container_width=True, config=_plotly_config(simplified=is_mobile))
        with st.container():
            fig_heat = _apply_simplified_view(fig_heat, is_mobile)
            st.plotly_chart(fig_heat, use_container_width=True, config=_plotly_config(simplified=is_mobile))
    else:
        col1, col2 = st.columns(2)
        with col1:
            fig_fat_ano = _apply_simplified_view(fig_fat_ano, is_mobile)
            st.plotly_chart(fig_fat_ano, use_container_width=True, config=_plotly_config(simplified=is_mobile))
        with col2:
            fig_mm = _apply_simplified_view(fig_mm, is_mobile)
            st.plotly_chart(fig_mm, use_container_width=True, config=_plotly_config(simplified=is_mobile))
            tabela_mm = (
                fat_mensal.pivot(index="ano", columns="mes_label", values="total")
                .reindex(columns=MESES_LABELS, fill_value=0.0)
                .fillna(0.0)
            )
            tabela_mm = tabela_mm.reset_index().rename(columns={"ano": "Ano"})
            tabela_mm_styled = tabela_mm.style.format(
                {m: _fmt_currency for m in MESES_LABELS}
            )
            st.markdown("**Faturamento Mês a Mês (R$)**")
            st.dataframe(tabela_mm_styled, use_container_width=True)

        col3, col4 = st.columns(2)
        with col3:
            fig_rank = _apply_simplified_view(fig_rank, is_mobile)
            st.plotly_chart(fig_rank, use_container_width=True, config=_plotly_config(simplified=is_mobile))
        with col4:
            fig_heat = _apply_simplified_view(fig_heat, is_mobile)
            st.plotly_chart(fig_heat, use_container_width=True, config=_plotly_config(simplified=is_mobile))


def _prepare_fat_mensal(df_entrada: pd.DataFrame, anos_multiselect: List[int]) -> Optional[pd.DataFrame]:
    if df_entrada.empty:
        return None
    df_base = df_entrada[df_entrada["ano"].isin(anos_multiselect)].copy()
    if df_base.empty:
        return None
    df_base["total"] = pd.to_numeric(df_base["Valor"], errors="coerce").fillna(0.0)
    fat_mensal = df_base.groupby(["ano", "mes"])["total"].sum().reset_index()
    fat_mensal["mes_label"] = fat_mensal["mes"].apply(lambda m: MESES_LABELS[m - 1])
    return fat_mensal


def render_bloco_faturamento_anual(df_entrada: pd.DataFrame, anos_multiselect: List[int], is_mobile: bool = False) -> None:
    st.subheader("Faturamento Anual")
    fat_mensal = _prepare_fat_mensal(df_entrada, anos_multiselect)
    if fat_mensal is None or fat_mensal.empty:
        st.info("Sem dados para os anos selecionados.")
        return

    fat_anual = fat_mensal.groupby("ano")["total"].sum().reset_index()
    fat_anual["total_fmt"] = fat_anual["total"].apply(lambda v: f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

    fig_fat_ano = px.line(
        fat_anual,
        x="ano",
        y="total",
        markers=True,
        title="Faturamento Anual",
        labels={"ano": "Ano", "total": "Faturamento"},
    )
    fig_fat_ano.update_traces(
        mode="lines+markers+text",
        text=fat_anual["total_fmt"],
        textposition="top center",
        cliponaxis=False,
        line=dict(color="#9b59b6"),
        marker=dict(color="#9b59b6"),
        hovertemplate=_hover_currency(show_x=True),
    )
    fig_fat_ano.update_xaxes(dtick=1, tickformat="d")
    if is_mobile:
        fig_fat_ano.update_traces(text=None, texttemplate=None)
    font_size = 14 if is_mobile else 10
    title_size = 22 if is_mobile else 18
    height = 550 if is_mobile else 350
    fig_fat_ano.update_layout(
        height=height,
        font=dict(size=font_size),
        title=dict(font=dict(size=title_size)),
        dragmode="zoom",
        hovermode="x unified",
        showlegend=not is_mobile,
        margin=dict(t=80, b=40),
    )
    fig_fat_ano = _apply_simplified_view(fig_fat_ano, is_mobile)
    st.plotly_chart(fig_fat_ano, use_container_width=True, config=_plotly_config(simplified=is_mobile))


def render_bloco_faturamento_mensal(df_entrada: pd.DataFrame, anos_multiselect: List[int], is_mobile: bool = False) -> None:
    st.subheader("Faturamento Mensal por Ano")
    fat_mensal = _prepare_fat_mensal(df_entrada, anos_multiselect)
    if fat_mensal is None or fat_mensal.empty:
        st.info("Sem dados para os anos selecionados.")
        return

    ano_atual = max(anos_multiselect) if anos_multiselect else None
    ano_anterior = ano_atual - 1 if ano_atual and (ano_atual - 1) in anos_multiselect else None

    pivot_vals = (
        fat_mensal.pivot(index="ano", columns="mes", values="total")
        .reindex(columns=range(1, 13), fill_value=0.0)
        .fillna(0.0)
    )
    yoy_labels_full: Dict[int, str] = {}
    yoy_colors_full: Dict[int, str] = {}
    if ano_atual is not None and ano_anterior in pivot_vals.index:
        for m in range(1, 13):
            prev = float(pivot_vals.at[ano_anterior, m]) if m in pivot_vals.columns else 0.0
            cur = float(pivot_vals.at[ano_atual, m]) if m in pivot_vals.columns else 0.0
            if prev > 0:
                yoy = (cur / prev - 1.0) * 100.0
                lbl = f"{yoy:+.1f}%".replace(".", ",")
                if yoy > 0:
                    color = "green"
                elif yoy < 0:
                    color = "red"
                else:
                    color = "#ffffff"
            else:
                lbl = ""
                color = "#ffffff"
            yoy_labels_full[m] = lbl
            yoy_colors_full[m] = color

    palette_outros = [
        "#e67e22",  # laranja
        "#1abc9c",  # verde água
        "#e74c3c",  # vermelho
        "#f1c40f",  # amarelo
        "#8e44ad",  # roxo escuro
        "#16a085",  # verde
        "#d35400",  # laranja escuro
        "#2ecc71",  # verde claro
        "#3498db",  # azul claro
        "#9b59b6",  # roxo extra
    ]

    anos_sorted = sorted(anos_multiselect)
    cores_por_ano = {}

    # reserva cores fixas para ano atual e ano anterior
    if ano_atual is not None and ano_atual in anos_sorted:
        cores_por_ano[ano_atual] = "#9b59b6"  # roxo para ano atual

    if ano_anterior is not None and ano_anterior in anos_sorted and ano_anterior not in cores_por_ano:
        cores_por_ano[ano_anterior] = "#2980b9"  # azul para ano anterior

    # atribui cores únicas para os demais anos
    idx_cor = 0
    for a in anos_sorted:
        if a in cores_por_ano:
            continue
        if idx_cor >= len(palette_outros):
            # se acabar a paleta, recomeça (caso raro de muitos anos)
            idx_cor = 0
        cores_por_ano[a] = palette_outros[idx_cor]
        idx_cor += 1

    fig_mm = go.Figure()
    for ano in anos_multiselect:
        df_ano = fat_mensal[fat_mensal["ano"] == ano].sort_values("mes")
        if df_ano.empty:
            continue
        is_atual = ano == ano_atual
        is_prev = ano_anterior is not None and ano == ano_anterior
        color = cores_por_ano.get(ano, "#bdc3c7")
        width = 3 if is_atual else (2 if is_prev else 1)
        msize = 8 if is_atual else (6 if is_prev else 4)
        opacity = 1.0 if is_atual else (0.7 if is_prev else 0.3)
        text_vals = [yoy_labels_full.get(int(m), "") if is_atual else "" for m in df_ano["mes"]]
        text_colors = [yoy_colors_full.get(int(m), "#ffffff") if is_atual else "#ffffff" for m in df_ano["mes"]]
        fig_mm.add_trace(
            go.Scatter(
                x=df_ano["mes_label"],
                y=df_ano["total"],
                name=str(ano),
                mode="lines+markers+text",
                line=dict(color=color, width=width),
                marker=dict(color=color, size=msize),
                opacity=opacity,
                text=text_vals,
                textposition="top center",
                textfont=dict(color=text_colors, size=12),
                hovertemplate=_hover_currency(show_x=True),
            )
        )
    fig_mm.add_hline(y=0, line_color="#888", line_dash="dash", opacity=0.7)
    font_size = 14 if is_mobile else 10
    title_size = 22 if is_mobile else 18
    height = 550 if is_mobile else 350
    fig_mm.update_layout(
        title=dict(text="Faturamento Mensal (por ano)", font=dict(size=title_size)),
        xaxis_title="Mês",
        yaxis_title="Faturamento (R$)",
        height=height,
        font=dict(size=font_size),
        dragmode="zoom",
        hovermode="x unified",
        showlegend=not is_mobile,
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.25,
            xanchor="center",
            x=0.5,
        ),
        margin=dict(b=90),
    )
    if is_mobile:
        fig_mm.update_traces(text=None, texttemplate=None)

    tabela_mm = (
        fat_mensal.pivot(index="ano", columns="mes_label", values="total")
        .reindex(columns=MESES_LABELS, fill_value=0.0)
        .fillna(0.0)
    )
    tabela_mm = tabela_mm.reset_index().rename(columns={"ano": "Ano"})
    tabela_mm_styled = tabela_mm.style.format(
        {m: _fmt_currency for m in MESES_LABELS}
    )

    fig_mm = _apply_simplified_view(fig_mm, is_mobile)
    st.plotly_chart(fig_mm, use_container_width=True, config=_plotly_config(simplified=is_mobile))
    st.markdown("**Faturamento Mês a Mês (R$)**")
    st.dataframe(tabela_mm_styled, use_container_width=True)


def render_bloco_top_meses(df_entrada: pd.DataFrame, anos_multiselect: List[int], is_mobile: bool = False) -> None:
    st.subheader("Top meses (Faturamento)")
    fat_mensal = _prepare_fat_mensal(df_entrada, anos_multiselect)
    if fat_mensal is None or fat_mensal.empty:
        st.info("Sem dados para os anos selecionados.")
        return
    ranking = fat_mensal.copy()
    ranking["label"] = ranking.apply(lambda r: f"{MESES_LABELS[int(r['mes']) - 1]}/{int(r['ano'])}", axis=1)
    top = ranking.sort_values("total", ascending=False).head(8)
    fig_rank = px.bar(top, x="label", y="total", title="Top meses (Faturamento)", labels={"label": "Mês/Ano", "total": "Faturamento"})
    fig_rank.update_traces(hovertemplate=_hover_currency(show_x=True), marker_color="#9b59b6")
    font_size = 14 if is_mobile else 10
    title_size = 22 if is_mobile else 18
    height = 550 if is_mobile else 350
    fig_rank.update_layout(
        height=height,
        font=dict(size=font_size),
        title=dict(font=dict(size=title_size)),
        dragmode="zoom",
        hovermode="x unified",
        showlegend=not is_mobile,
    )
    fig_rank = _apply_simplified_view(fig_rank, is_mobile)
    st.plotly_chart(fig_rank, use_container_width=True, config=_plotly_config(simplified=is_mobile))


def render_bloco_heatmap(df_entrada: pd.DataFrame, anos_multiselect: List[int], is_mobile: bool = False) -> None:
    st.subheader("Heatmap de Faturamento")
    fat_mensal = _prepare_fat_mensal(df_entrada, anos_multiselect)
    if fat_mensal is None or fat_mensal.empty:
        st.info("Sem dados para os anos selecionados.")
        return
    pivot = (
        fat_mensal
        .pivot(index="mes_label", columns="ano", values="total")
        .reindex(index=list(reversed(MESES_LABELS)))
        .fillna(0.0)
    )
    fig_heat = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=[str(c) for c in pivot.columns],
            y=pivot.index,
            colorscale="Purples",
            hoverongaps=False,
        )
    )
    fig_heat = _apply_simplified_view(fig_heat, is_mobile)
    font_size = 14 if is_mobile else 10
    title_size = 22 if is_mobile else 18
    height = 550 if is_mobile else 350
    fig_heat.update_layout(
        title=dict(text="Heatmap de Faturamento", font=dict(size=title_size)),
        height=height,
        font=dict(size=font_size),
        dragmode="zoom",
        hovermode="x unified",
        showlegend=not is_mobile,
    )
    st.plotly_chart(fig_heat, use_container_width=True, config=_plotly_config(simplified=is_mobile))


def render_bloco_lucro_liquido(metrics: List[Dict], ano: int, vars_dre, db_path: str, is_mobile: bool = False) -> None:
    st.subheader("Lucro Líquido")
    meses = list(range(1, 13))
    lucro_labels = [MESES_LABELS[m - 1] for m in meses]
    lucros_raw = [metrics[m - 1].get("lucro_liq", None) if m - 1 < len(metrics) else None for m in meses]

    # Prepara a série principal de Lucro Líquido
    lucros_plot = []
    for idx, val in enumerate(lucros_raw, start=1):
        if idx < 10:
            lucros_plot.append(None)
            continue
        try:
            v = float(val) if val is not None else None
        except Exception:
            v = None
        lucros_plot.append(v if v not in (None, 0) else None)

    # 1) Tenta ler depreciação das variáveis já carregadas
    deprec_mensal = 0.0
    if isinstance(vars_dre, dict):
        for key in ("_dashboard_deprec_mensal", "depreciacao_mensal_padrao"):
            if key in vars_dre:
                try:
                    deprec_mensal = float(vars_dre.get(key) or 0.0)
                except Exception:
                    deprec_mensal = 0.0
                break

    # 2) Se ainda for 0, busca diretamente no banco de forma robusta
    if not deprec_mensal and db_path:
        try:
            with get_conn(db_path) as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT valor_num
                    FROM dre_variaveis
                    WHERE chave = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    ("depreciacao_mensal_padrao",),
                )
                row = cur.fetchone()
                if row and row[0] is not None:
                    deprec_mensal = float(row[0])
        except Exception:
            pass

    serie_lucro_liquido = lucros_plot

    # Cria a série de depreciação mensal (constante ou 0)
    serie_deprec_mensal = [deprec_mensal for _ in serie_lucro_liquido]

    # 3) Calcula a série "Antes da Depreciação" somando (Lucro + Depreciação)
    serie_lucro_antes_deprec = []
    for ll, dep in zip(serie_lucro_liquido, serie_deprec_mensal):
        if ll is None:
            serie_lucro_antes_deprec.append(None)
        else:
            serie_lucro_antes_deprec.append((ll or 0.0) + (dep or 0.0))

    text_liq = [_fmt_currency(v) if v is not None else None for v in serie_lucro_liquido]
    text_antes = [_fmt_currency(v) if v is not None else None for v in serie_lucro_antes_deprec]
    colors_liq = ["#2ecc71" if (v is not None and v >= 0) else "#e74c3c" for v in serie_lucro_liquido]
    colors_antes = ["#2ecc71" if (v is not None and v >= 0) else "#e74c3c" for v in serie_lucro_antes_deprec]

    fig_lucro = None
    show_lucro = False
    font_size = 14 if is_mobile else 10
    title_size = 22 if is_mobile else 18
    height = 550 if is_mobile else 350

    if ano == 2025:
        show_lucro = any(v is not None for v in serie_lucro_liquido)
        if show_lucro:
            # Calcula limites para o eixo Y para evitar corte dos rótulos
            all_vals = [v for v in serie_lucro_liquido + serie_lucro_antes_deprec if v is not None]
            range_y_args = {}
            if all_vals:
                y_max = max(all_vals)
                y_min = min(all_vals)
                # Adiciona 20% de margem no topo para os labels
                amplitude = y_max - y_min if y_max != y_min else abs(y_max) or 100
                range_max = y_max + (amplitude * 0.25)
                # Mantém o zero visível ou margem inferior se houver negativos
                range_min = y_min - (amplitude * 0.1) if y_min < 0 else 0
                range_y_args["range"] = [range_min, range_max]

            fig_lucro = go.Figure()

            # 1. Barras para o Lucro Líquido (Verde para Positivo, Vermelho para Negativo)
            # Define as cores baseadas no valor: Verde (#2ecc71) se > 0, Vermelho (#e74c3c) se < 0
            colors_liq_condicional = ["#2ecc71" if (v is not None and v >= 0) else "#e74c3c" for v in serie_lucro_liquido]
            
            fig_lucro.add_trace(
                go.Bar(
                    x=lucro_labels,
                    y=serie_lucro_liquido,
                    name="Lucro Líquido",
                    marker_color=colors_liq_condicional, 
                    text=text_liq,
                    textposition="auto",
                    hovertemplate="Lucro Líquido: %{y:,.2f}<extra></extra>"
                )
            )

            # 2. Linha para o Lucro Antes da Depreciação (Roxo e Destacada)
            fig_lucro.add_trace(
                go.Scatter(
                    x=lucro_labels,
                    y=serie_lucro_antes_deprec,
                    name="Antes da Deprec.",
                    mode="lines+markers+text", # Adiciona texto na linha
                    line=dict(color="#9b59b6", width=4), # Roxo e mais espessa
                    marker=dict(size=8, color="#ffffff", line=dict(width=2, color="#9b59b6")),
                    text=text_antes, # Valores da linha
                    textposition="top center",
                    textfont=dict(color="white", size=11, family="Arial Black"), # Texto branco para contraste
                    cliponaxis=False, # Permite que o texto saia da área de plotagem se necessário
                    hovertemplate="Antes Deprec.: %{y:,.2f}<extra></extra>"
                )
            )

            fig_lucro.update_layout(
                title=dict(text=f"Lucro Líquido vs. Operacional – {ano}", font=dict(size=title_size)),
                legend=dict(
                    orientation="h",
                    yanchor="top",
                    y=-0.2,
                    xanchor="center",
                    x=0.5,
                ),
                margin=dict(t=80, b=90),
                height=height,
                font=dict(size=font_size),
                dragmode="zoom",
                hovermode="x unified",
                showlegend=not is_mobile,
                yaxis=range_y_args,
            )
            
            if is_mobile:
                fig_lucro.update_traces(text=None, texttemplate=None)

    if ano != 2025:
        st.warning("Lucro líquido só está disponível a partir de outubro de 2025. Não há dados consistentes para anos anteriores.")
    else:
        if show_lucro and fig_lucro is not None:
            fig_lucro = _apply_simplified_view(fig_lucro, is_mobile)
            st.plotly_chart(fig_lucro, use_container_width=True, config=_plotly_config(simplified=is_mobile))
        else:
            st.warning("Não há dados de lucro líquido registrados entre outubro e dezembro de 2025.")


def render_bloco_balanco_mensal(df_entrada: pd.DataFrame, df_saida: pd.DataFrame, ano: int, is_mobile: bool = False) -> None:
    st.subheader("Balanço Mensal")
    meses = list(range(1, 13))
    meses_labels = [MESES_LABELS[m - 1] for m in meses]
    df_ent_ano = df_entrada[df_entrada["ano"] == ano] if not df_entrada.empty else pd.DataFrame(columns=["mes", "Valor"])
    df_sai_ano = df_saida[df_saida["ano"] == ano] if not df_saida.empty else pd.DataFrame(columns=["mes", "Valor"])
    fat_series = df_ent_ano.groupby("mes")["Valor"].sum().reindex(meses).fillna(0.0)
    sai_series = df_sai_ano.groupby("mes")["Valor"].sum().reindex(meses).fillna(0.0)
    fat = fat_series.tolist()
    saidas = sai_series.tolist()
    resultado = [f - s for f, s in zip(fat, saidas)]
    df_balanco = pd.DataFrame({"Mês": meses_labels, "Faturamento": fat, "Saída": saidas, "Resultado": resultado})

    res_pos = [v if v > 0 else None for v in resultado]
    res_neg = [v if v < 0 else None for v in resultado]
    res_zero = [v if v == 0 else None for v in resultado]

    fig_balanco = go.Figure()
    fig_balanco.add_trace(go.Bar(x=df_balanco["Mês"], y=df_balanco["Faturamento"], name="Entrada", marker_color="#2980b9"))
    fig_balanco.add_trace(go.Bar(x=df_balanco["Mês"], y=df_balanco["Saída"], name="Saída", marker_color="#e67e22"))
    fig_balanco.add_trace(
        go.Bar(
            x=df_balanco["Mês"],
            y=res_pos,
            name="Resultado (+)",
            marker_color="#27ae60",
        )
    )
    fig_balanco.add_trace(
        go.Bar(
            x=df_balanco["Mês"],
            y=res_neg,
            name="Resultado (-)",
            marker_color="#e74c3c",
        )
    )
    if any(v is not None for v in res_zero):
        fig_balanco.add_trace(
            go.Bar(
                x=df_balanco["Mês"],
                y=res_zero,
                name="Resultado (0)",
                marker_color="#95a5a6",
            )
        )
    linha_text = [_fmt_currency(v) for v in resultado]
    fig_balanco.add_trace(
        go.Scatter(
            x=df_balanco["Mês"],
            y=df_balanco["Resultado"],
            name="Linha Resultado",
            mode="lines+markers+text",
            line=dict(color="#9b59b6"),
            marker=dict(color="#9b59b6"),
            text=linha_text,
            textposition="top center",
            textfont=dict(size=14, color="#ffffff"),
            hovertemplate="Mês: %{x}<br>Balanço: R$ %{y:,.2f}<extra></extra>",
        )
    )
    font_size = 14 if is_mobile else 10
    title_size = 22 if is_mobile else 18
    height = 550 if is_mobile else 350
    fig_balanco.update_layout(
        barmode="group",
        title=dict(text=f"Balanço Mensal – {ano}", font=dict(size=title_size)),
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.25,
            xanchor="center",
            x=0.5,
        ),
        margin=dict(b=90),
        height=height,
        font=dict(size=font_size),
        dragmode="zoom",
        hovermode="x unified",
        showlegend=not is_mobile,
    )
    if is_mobile:
        fig_balanco.update_traces(text=None, texttemplate=None)

    with st.container():
        fig_balanco = _apply_simplified_view(fig_balanco, is_mobile)
        st.plotly_chart(fig_balanco, use_container_width=True, config=_plotly_config(simplified=is_mobile))
        tabela_mes = pd.DataFrame(
            [fat, saidas, resultado],
            index=["Entrada", "Saída", "Resultado"],
            columns=meses_labels,
        )
        tabela_fmt = (
            tabela_mes.style.format(_fmt_currency).map(
                lambda v: "color:green"
                if isinstance(v, (int, float)) and v > 0
                else ("color:red" if isinstance(v, (int, float)) and v < 0 else ""),
                subset=pd.IndexSlice["Resultado", :],
            )
        )
        st.markdown("**Valores Mensais**")
        st.dataframe(tabela_fmt, use_container_width=True)

def render_reposicao(df_mercadorias: pd.DataFrame, metrics: List[Dict]) -> None:
    simplified = bool(st.session_state.get("fd_modo_mobile", False))
    st.subheader("Reposição / Estoque")
    if df_mercadorias.empty:
        st.info("Sem dados de mercadorias.")
        return
    anos_disp = sorted(df_mercadorias["ano"].dropna().unique())
    if not anos_disp:
        st.info("Nenhum ano disponível para reposição.")
        return
    ano_reposicao = st.selectbox("Ano – Reposição", options=anos_disp, index=len(anos_disp) - 1 if anos_disp else 0)
    df_ano = df_mercadorias[df_mercadorias["ano"] == ano_reposicao]
    reposicao = df_ano.groupby("mes")["Valor"].sum().reindex(range(1, 13)).fillna(0.0)
    cmv = [metrics[m - 1].get("cmv", 0.0) if m - 1 < len(metrics) else 0.0 for m in range(1, 13)]

    # Cores da paleta roxa
    cor_reposto = "#9b59b6"  # Roxo mais claro
    cor_cmv = "#6c3483"      # Roxo mais escuro

    # Gráfico Mensal
    df_rep = pd.DataFrame(
        {
            "Mês": [MESES_LABELS[i] for i in range(12)],
            "Valor Reposto": reposicao.values,
            "CMV": cmv,
        }
    )
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df_rep["Mês"], y=df_rep["Valor Reposto"], name="Valor reposto", marker_color=cor_reposto, hovertemplate=_hover_currency(show_x=True)))
    fig.add_trace(go.Bar(x=df_rep["Mês"], y=df_rep["CMV"], name="CMV", marker_color=cor_cmv, hovertemplate=_hover_currency(show_x=True)))
    fig.update_layout(
        barmode="group",
        title=f"Reposição x Custo Mercadoria – {ano_reposicao} (Mensal)",
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.2,
            xanchor="center",
            x=0.5,
        ),
        margin=dict(b=80),
        hovermode="x unified",
    )
    fig = _apply_simplified_view(fig, simplified)

    # Gráfico Anual (Resumo)
    total_reposicao = reposicao.sum()
    total_cmv = sum(cmv)
    
    fig_anual = go.Figure()
    fig_anual.add_trace(go.Bar(x=["Valor Reposto"], y=[total_reposicao], name="Valor Reposto", marker_color=cor_reposto, text=[_fmt_currency(total_reposicao)], textposition="auto"))
    fig_anual.add_trace(go.Bar(x=["CMV"], y=[total_cmv], name="CMV", marker_color=cor_cmv, text=[_fmt_currency(total_cmv)], textposition="auto"))
    
    fig_anual.update_layout(
        title=f"Resumo Anual – {ano_reposicao}",
        showlegend=False,
        margin=dict(b=40),
        hovermode="x unified",
    )
    fig_anual.update_traces(hovertemplate="%{x}<br>%{text}<extra></extra>")
    fig_anual = _apply_simplified_view(fig_anual, simplified)

    if simplified:
        st.plotly_chart(fig, use_container_width=True, config=_plotly_config(simplified=simplified))
        st.plotly_chart(fig_anual, use_container_width=True, config=_plotly_config(simplified=simplified))
    else:
        c1, c2 = st.columns([2, 1])
        with c1:
            st.plotly_chart(fig, use_container_width=True, config=_plotly_config(simplified=simplified))
        with c2:
            st.plotly_chart(fig_anual, use_container_width=True, config=_plotly_config(simplified=simplified))


# ========================= Entrada principal =========================
def render_dashboard(caminho_banco: Optional[str]):
    """
    Dashboard principal FlowDash.
    """
    db_path = _resolve_db_path(caminho_banco)
    vars_dre = _load_vars_runtime(db_path)
    # Garante depreciação mensal padrão disponível para o dashboard
    deprec_mensal_dashboard = 0.0
    try:
        with get_conn(db_path) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT valor_num
                FROM dre_variaveis
                WHERE chave = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                ("depreciacao_mensal_padrao",),
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                try:
                    deprec_mensal_dashboard = float(row[0])
                except Exception:
                    deprec_mensal_dashboard = 0.0
    except Exception:
        deprec_mensal_dashboard = 0.0

    if isinstance(vars_dre, dict):
        vars_dre["_dashboard_deprec_mensal"] = deprec_mensal_dashboard
    df_entrada, df_saida = _load_entradas_saidas(db_path)
    df_mercadorias = _load_mercadorias(db_path)

    if df_entrada.empty:
        st.error("Não há dados de entrada para montar o dashboard.")
        return

    anos_disponiveis = sorted(df_entrada["ano"].dropna().unique())
    if not anos_disponiveis:
        st.error("Não foi possível identificar anos em df_entrada.")
        return
    ano_selecionado = st.selectbox(
        "Ano (gráficos mensais)",
        anos_disponiveis,
        index=len(anos_disponiveis) - 1 if anos_disponiveis else 0,
    )
    anos_multiselect = st.multiselect(
        "Anos para comparação (gráficos anuais / M/M)",
        options=anos_disponiveis,
        default=anos_disponiveis,
    )

    metrics = _calc_monthly_metrics(db_path, int(ano_selecionado), vars_dre)

    modo_mobile = st.toggle(
        "Visualização simplificada (mobile)",
        key="fd_modo_mobile",
        value=st.session_state.get("fd_modo_mobile", False),
        help="Reduz textos e poluição visual para facilitar a leitura no celular.",
    )
    IS_MOBILE = bool(modo_mobile)

    with st.container():
        render_chips_principais(df_entrada, db_path, int(ano_selecionado), vars_dre)

    # Bloco de Metas da Loja – continua no topo, ocupando a largura toda
    with st.container():
        st.markdown("### Metas da Loja")
        render_metas_resumo_dashboard(db_path)

    # ========================= Bloco de Previsão (Prophet) =========================
    with st.container():
        st.markdown("---")
        st.header("Previsão de Faturamento (IA)")
        
        # Slider para escolher meses futuros
        meses_futuro = st.slider("Meses para prever", min_value=1, max_value=24, value=12)
        
        # Apenas chama a função. O "engine" se vira para buscar IPCA, Selic, tratar dados, etc.
        if not df_entrada.empty:
            with st.spinner("O Oráculo está consultando o Banco Central e prevendo o futuro..."):
                # Passa o df_entrada BRUTO
                fig_previsao, dados_futuros, metricas_atual = criar_grafico_previsao(df_entrada, meses_futuro)
            
            # --- NOVO: Exibir Comparativo do Mês Atual (Realizado vs Meta IA) ---
            if metricas_atual and metricas_atual.get('previsto', 0) > 0:
                mes_nome = MESES_LABELS[metricas_atual['data'].month - 1]
                ano_atual = metricas_atual['data'].year
                
                st.markdown(f"##### Acompanhamento: {mes_nome}/{ano_atual}")
                
                c1, c2, c3, c4 = st.columns(4)
                
                v_real = metricas_atual.get('realizado', 0.0)
                v_prev = metricas_atual.get('previsto', 0.0)
                v_pess = metricas_atual.get('pessimista', 0.0)
                v_otim = metricas_atual.get('otimista', 0.0)
                
                # Helper para calcular % e formatar delta composto
                def _calc_delta_composto(real, meta):
                    if meta <= 0: return "off", "0.0%"
                    
                    diff = real - meta
                    pct = (real / meta) * 100.0
                    
                    # Formatação do valor da diferença (absoluto)
                    diff_fmt = _fmt_currency(abs(diff))
                    
                    if diff >= 0:
                        # Meta batida (Verde)
                        return "normal", f"{pct:.1f}% (Superou {diff_fmt})"
                    else:
                        # Falta bater (Cinza/Padrão)
                        return "off", f"{pct:.1f}% (Falta {diff_fmt})"

                # Col 1: Realizado
                c1.metric("Realizado", _fmt_currency(v_real))
                
                # Col 2: Pessimista
                cor_pess, delta_pess = _calc_delta_composto(v_real, v_pess)
                c2.metric("Meta Mínima (Pessimista)", _fmt_currency(v_pess), delta=delta_pess, delta_color=cor_pess)
                
                # Col 3: Realista
                cor_prev, delta_prev = _calc_delta_composto(v_real, v_prev)
                c3.metric("Meta Realista (IA)", _fmt_currency(v_prev), delta=delta_prev, delta_color=cor_prev)
                
                # Col 4: Otimista
                cor_otim, delta_otim = _calc_delta_composto(v_real, v_otim)
                c4.metric("Meta Otimista", _fmt_currency(v_otim), delta=delta_otim, delta_color=cor_otim)
                
                st.divider() # Separador visual antes do gráfico
            
            # Exibe Cards com o próximo mês
            if not dados_futuros.empty:
                prox_mes = dados_futuros.iloc[0]
                data_ref = prox_mes['ds'].strftime('%B/%Y') # Ex: Janeiro/2024 (depende do locale, mas ok)
                
                st.markdown(f"**Previsão para o próximo mês ({data_ref})**")
                c_pess, c_real, c_otim = st.columns(3)
                
                c_pess.metric("🔻 Cenário Pessimista", _fmt_currency(prox_mes['yhat_lower']))
                c_real.metric("🎯 Previsão Realista", _fmt_currency(prox_mes['yhat']))
                c_otim.metric("🚀 Cenário Otimista", _fmt_currency(prox_mes['yhat_upper']))

            fig_previsao = _apply_simplified_view(fig_previsao, IS_MOBILE)
            st.plotly_chart(fig_previsao, use_container_width=True, config=_plotly_config(simplified=IS_MOBILE))

            # Tabela Detalhada (Transposta)
            st.markdown("##### Detalhamento da Previsão")
            
            if not dados_futuros.empty:
                # Seleciona e ordena: Otimista -> Realista -> Pessimista
                df_t = dados_futuros[['ds', 'yhat_upper', 'yhat', 'yhat_lower']].copy()
                
                # Helper para formatar data usando MESES_LABELS (Ex: Nov/2025)
                def _fmt_data_pt(d):
                    try:
                        return f"{MESES_LABELS[d.month-1]}/{d.year}"
                    except:
                        return str(d)
                
                df_t['ds_label'] = df_t['ds'].apply(_fmt_data_pt)
                
                # Define a coluna de data como índice e transpõe
                # Transpõe apenas as colunas de valor
                df_t = df_t.set_index('ds_label')[['yhat_upper', 'yhat', 'yhat_lower']].T
                
                # Renomeia as linhas
                df_t = df_t.rename(index={
                    'yhat_upper': 'Máximo (Otimista)',
                    'yhat': 'Previsão Realista',
                    'yhat_lower': 'Mínimo (Pessimista)'
                })
                
                # Formata valores para R$ (usando map em vez de applymap para Pandas 2.x)
                df_t = df_t.map(_fmt_currency)
                
                st.dataframe(df_t, use_container_width=True)
            elif metricas_atual and "error" in metricas_atual:
                 st.error(f"Não foi possível gerar a previsão: {metricas_atual['error']}")
            else:
                 st.warning("Dados insuficientes para gerar previsão (mínimo 6 meses de histórico).")
        else:
            st.info("Sem dados de entrada para previsão.")

    with st.container():
        if IS_MOBILE:
            with st.container():
                render_bloco_faturamento_anual(df_entrada, [int(a) for a in anos_multiselect], is_mobile=IS_MOBILE)
            with st.container():
                render_bloco_lucro_liquido(metrics, int(ano_selecionado), vars_dre, db_path, is_mobile=IS_MOBILE)
        else:
            col1, col2 = st.columns(2)
            with col1:
                render_bloco_faturamento_anual(df_entrada, [int(a) for a in anos_multiselect], is_mobile=IS_MOBILE)
            with col2:
                render_bloco_lucro_liquido(metrics, int(ano_selecionado), vars_dre, db_path, is_mobile=IS_MOBILE)

    with st.container():
        if IS_MOBILE:
            with st.container():
                render_bloco_faturamento_mensal(df_entrada, [int(a) for a in anos_multiselect], is_mobile=IS_MOBILE)
            with st.container():
                render_bloco_balanco_mensal(df_entrada, df_saida, int(ano_selecionado), is_mobile=IS_MOBILE)
        else:
            col1, col2 = st.columns(2)
            with col1:
                render_bloco_faturamento_mensal(df_entrada, [int(a) for a in anos_multiselect], is_mobile=IS_MOBILE)
            with col2:
                render_bloco_balanco_mensal(df_entrada, df_saida, int(ano_selecionado), is_mobile=IS_MOBILE)

    with st.container():
        render_endividamento(db_path)

    with st.container():
        if IS_MOBILE:
            with st.container():
                render_bloco_top_meses(df_entrada, [int(a) for a in anos_multiselect], is_mobile=IS_MOBILE)
            with st.container():
                render_bloco_heatmap(df_entrada, [int(a) for a in anos_multiselect], is_mobile=IS_MOBILE)
        else:
            col1, col2 = st.columns(2)
            with col1:
                render_bloco_top_meses(df_entrada, [int(a) for a in anos_multiselect], is_mobile=IS_MOBILE)
            with col2:
                render_bloco_heatmap(df_entrada, [int(a) for a in anos_multiselect], is_mobile=IS_MOBILE)

    with st.container():
        render_reposicao(df_mercadorias, metrics)




def render(caminho_banco: Optional[str] = None):
    """
    Wrapper para compatibilidade com o carregador de páginas do FlowDash.
    Apenas delega para render_dashboard.
    """
    return render_dashboard(caminho_banco)

def main():
    render_dashboard(None)

def app():
    main()