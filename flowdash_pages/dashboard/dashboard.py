from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

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
from flowdash_pages.fechamento.fechamento import _somar_bancos_totais, _ultimo_caixas_ate


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
    hoje = date.today()
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
    hoje = date.today()
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
    ref_day = date.today()
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
    Bloco de KPIs principais do dashboard (topo da página).

    Indicadores calculados sempre com base na data de hoje (ano/mês/dia atuais),
    independentemente do ano selecionado para os gráficos abaixo.
    """
    hoje = date.today()
    ano_atual = hoje.year
    mes_atual = hoje.month
    dia_atual = hoje.day

    if df_entrada is None or df_entrada.empty:
        st.info("Sem dados de entrada para calcular os indicadores principais.")
        return

    df = df_entrada.copy()
    # Garante colunas de apoio
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
    df["Data_date"] = df["Data"].dt.date
    df["ano"] = df["Data"].dt.year
    df["mes"] = df["Data"].dt.month

    # -------------------------
    # Bloco 1 – Vendas & Saldo
    # -------------------------
    # Vendas do dia
    mask_dia = df["Data_date"] == hoje
    vendas_dia = float(df.loc[mask_dia, "Valor"].sum())

    # Vendas do mês (ano atual + mês atual)
    mask_mes_atual = (df["ano"] == ano_atual) & (df["mes"] == mes_atual)
    vendas_mes = float(df.loc[mask_mes_atual, "Valor"].sum())

    # Vendas do ano (ano atual)
    mask_ano_atual = df["ano"] == ano_atual
    vendas_ano = float(df.loc[mask_ano_atual, "Valor"].sum())

    # Saldo disponível (bancos + caixa + caixa 2) usando helpers do fechamento
    bancos_dict = _somar_bancos_totais(db_path, hoje)
    disp_caixa, disp_caixa2, _ = _ultimo_caixas_ate(db_path, hoje)
    saldo_disponivel = float(disp_caixa + disp_caixa2 + sum(bancos_dict.values()))

    # ------------------------------
    # Bloco 2 – Crescimento & Comparações
    # ------------------------------
    # Crescimento m/m: mês atual vs mês anterior (ano atual)
    fat_por_mes_ano = (
        df[df["ano"] == ano_atual]
        .groupby("mes")["Valor"]
        .sum()
    )

    valor_mes_atual = float(fat_por_mes_ano.get(mes_atual, 0.0))
    valor_mes_anterior = float(fat_por_mes_ano.get(mes_atual - 1, 0.0)) if mes_atual > 1 else 0.0
    if valor_mes_anterior > 0:
        crescimento_mm_pct = (valor_mes_atual - valor_mes_anterior) / valor_mes_anterior * 100.0
        delta_mm_valor = valor_mes_atual - valor_mes_anterior
    else:
        crescimento_mm_pct = 0.0
        delta_mm_valor = 0.0

    # Comparação do mesmo período do mês x ano anterior
    ano_anterior = ano_atual - 1
    comp_mes_anoant_pct = 0.0
    delta_mes_anoant_valor = 0.0
    valor_periodo_atual = 0.0
    valor_periodo_prev = 0.0

    if ano_anterior in df["ano"].unique():
        # Ajusta o dia final do mês anterior para não estourar (ex.: 31 vs fevereiro)
        dia_limite_prev = monthrange(ano_anterior, mes_atual)[1]
        dia_prev = min(dia_atual, dia_limite_prev)

        atual_ini = date(ano_atual, mes_atual, 1)
        atual_fim = hoje
        prev_ini = date(ano_anterior, mes_atual, 1)
        prev_fim = date(ano_anterior, mes_atual, dia_prev)

        mask_periodo_atual = (df["Data_date"] >= atual_ini) & (df["Data_date"] <= atual_fim)
        mask_periodo_prev = (df["Data_date"] >= prev_ini) & (df["Data_date"] <= prev_fim)

        valor_periodo_atual = float(df.loc[mask_periodo_atual, "Valor"].sum())
        valor_periodo_prev = float(df.loc[mask_periodo_prev, "Valor"].sum())

        if valor_periodo_prev > 0:
            comp_mes_anoant_pct = (valor_periodo_atual - valor_periodo_prev) / valor_periodo_prev * 100.0
            delta_mes_anoant_valor = valor_periodo_atual - valor_periodo_prev

    # Comparação do mês atual com o melhor mesmo mês dos anos anteriores
    df_mes_hist = df[df["mes"] == mes_atual].groupby("ano")["Valor"].sum()
    historico_mes_anteriores = df_mes_hist[df_mes_hist.index < ano_atual]
    melhor_ano_mes = None
    melhor_val_mes = 0.0
    comp_mes_melhor_pct = 0.0
    delta_mes_melhor_valor = 0.0

    if not historico_mes_anteriores.empty:
        melhor_ano_mes = int(historico_mes_anteriores.idxmax())
        melhor_val_mes = float(historico_mes_anteriores.max())
        if melhor_val_mes > 0:
            comp_mes_melhor_pct = (valor_mes_atual - melhor_val_mes) / melhor_val_mes * 100.0
            delta_mes_melhor_valor = valor_mes_atual - melhor_val_mes

    # Comparação do ano atual com o ano anterior (total anual)
    vendas_ano_anterior = float(df.loc[df["ano"] == ano_anterior, "Valor"].sum()) if ano_anterior in df["ano"].unique() else 0.0
    if vendas_ano_anterior > 0:
        comp_ano_anterior_pct = (vendas_ano - vendas_ano_anterior) / vendas_ano_anterior * 100.0
        delta_ano_anterior_valor = vendas_ano - vendas_ano_anterior
    else:
        comp_ano_anterior_pct = 0.0
        delta_ano_anterior_valor = 0.0

    # Comparação do ano atual com o melhor ano de vendas (histórico)
    vendas_por_ano = df.groupby("ano")["Valor"].sum()
    historico_anos_anteriores = vendas_por_ano[vendas_por_ano.index < ano_atual]
    melhor_ano_global = None
    melhor_val_ano_global = 0.0
    comp_ano_melhor_pct = 0.0
    delta_ano_melhor_valor = 0.0

    if not historico_anos_anteriores.empty:
        melhor_ano_global = int(historico_anos_anteriores.idxmax())
        melhor_val_ano_global = float(historico_anos_anteriores.max())
        if melhor_val_ano_global > 0:
            comp_ano_melhor_pct = (vendas_ano - melhor_val_ano_global) / melhor_val_ano_global * 100.0
            delta_ano_melhor_valor = vendas_ano - melhor_val_ano_global

    # -------------------------
    # Bloco 3 – Eficiência de Vendas
    # -------------------------
    # Cálculo direto a partir da tabela entrada (cada linha = 1 venda)

    # Número de vendas no mês atual (linhas da tabela entrada)
    n_vendas_mes = int(df.loc[mask_mes_atual].shape[0])

    # Número de vendas no ano atual
    n_vendas_ano = int(df.loc[mask_ano_atual].shape[0])

    # Ticket médio do mês: vendas_mes / n_vendas_mes
    ticket_medio_mes = (vendas_mes / n_vendas_mes) if n_vendas_mes > 0 else 0.0

    # Ticket médio do ano: vendas_ano / n_vendas_ano
    ticket_medio_ano = (vendas_ano / n_vendas_ano) if n_vendas_ano > 0 else 0.0

    # =======================
    # Renderização dos blocos (cards estilo Lançamentos)
    # =======================
    st.markdown("## Indicadores de Vendas")

    # --- Bloco 1 – Vendas & Saldo (2 linhas no mesmo card) ---
    render_card_rows(
        "📊 Vendas & Saldo",
        [
            # Linha 1: 3 indicadores lado a lado
            [
                ("Vendas do dia", vendas_dia, True),
                ("Vendas do mês", vendas_mes, True),
                ("Vendas do ano", vendas_ano, True),
            ],
            # Linha 2: Saldo disponível ocupando a linha toda
            [
                ("Saldo disponível", saldo_disponivel, True),
            ],
        ],
    )

    # Monta labels dinâmicos reaproveitando as variáveis já calculadas
    label_mes_melhor = (
        f"Mês atual vs melhor {MESES_LABELS[mes_atual - 1]}/{melhor_ano_mes}"
        if melhor_ano_mes is not None
        else f"Mês atual vs melhor {MESES_LABELS[mes_atual - 1]} histórico"
    )
    label_ano_melhor = (
        f"Ano atual vs melhor ano ({melhor_ano_global})"
        if melhor_ano_global is not None
        else "Ano atual vs melhor ano"
    )

    # monta valores com % e delta em R$ e aplica cor por sinal
    def _colorize_val(texto: str, base_pct: float) -> str:
        if base_pct > 0:
            color = "#2ecc71"
        elif base_pct < 0:
            color = "#e74c3c"
        else:
            color = "#ffffff"
        return f"<span style='color:{color};font-size:18px;font-weight:600;'>{texto}</span>"

    val_cres_mm_txt = _fmt_percent(crescimento_mm_pct)
    if delta_mm_valor:
        val_cres_mm_txt = f"{val_cres_mm_txt} ({_fmt_currency(delta_mm_valor)})"
    val_cres_mm = _colorize_val(val_cres_mm_txt, crescimento_mm_pct)

    val_comp_mes_anoant_txt = _fmt_percent(comp_mes_anoant_pct)
    if delta_mes_anoant_valor:
        val_comp_mes_anoant_txt = f"{val_comp_mes_anoant_txt} ({_fmt_currency(delta_mes_anoant_valor)})"
    val_comp_mes_anoant = _colorize_val(val_comp_mes_anoant_txt, comp_mes_anoant_pct)

    val_comp_mes_melhor_txt = _fmt_percent(comp_mes_melhor_pct)
    if delta_mes_melhor_valor:
        val_comp_mes_melhor_txt = f"{val_comp_mes_melhor_txt} ({_fmt_currency(delta_mes_melhor_valor)})"
    val_comp_mes_melhor = _colorize_val(val_comp_mes_melhor_txt, comp_mes_melhor_pct)

    val_comp_ano_ant_txt = _fmt_percent(comp_ano_anterior_pct)
    if delta_ano_anterior_valor:
        val_comp_ano_ant_txt = f"{val_comp_ano_ant_txt} ({_fmt_currency(delta_ano_anterior_valor)})"
    val_comp_ano_ant = _colorize_val(val_comp_ano_ant_txt, comp_ano_anterior_pct)

    val_comp_ano_melhor_txt = _fmt_percent(comp_ano_melhor_pct)
    if delta_ano_melhor_valor:
        val_comp_ano_melhor_txt = f"{val_comp_ano_melhor_txt} ({_fmt_currency(delta_ano_melhor_valor)})"
    val_comp_ano_melhor = _colorize_val(val_comp_ano_melhor_txt, comp_ano_melhor_pct)

    # --- Bloco 2 – Crescimento & Comparações (2 linhas no mesmo card) ---
    render_card_rows(
        "📈 Crescimento & Comparações",
        [
            # Linha 1: 3 indicadores
            [
                ("Crescimento m/m", [val_cres_mm], False),
                ("Mês vs mesmo período ano anterior", [val_comp_mes_anoant], False),
                (label_mes_melhor, [val_comp_mes_melhor], False),
            ],
            # Linha 2: 2 indicadores do ano
            [
                ("Ano atual vs ano anterior", [val_comp_ano_ant], False),
                (label_ano_melhor, [val_comp_ano_melhor], False),
            ],
        ],
    )

    # --- Bloco 3 – Eficiência de Vendas (2 linhas no mesmo card) ---
    render_card_rows(
        "🎯 Eficiência de Vendas",
        [
            # Linha 1: Ticket médio mês/ano
            [
                ("Ticket médio (mês)", ticket_medio_mes, True),
                ("Ticket médio (ano)", ticket_medio_ano, True),
            ],
            # Linha 2: Nº de vendas mês/ano
            [
                ("Nº de vendas (mês)", [str(int(n_vendas_mes))], False),
                ("Nº de vendas (ano)", [str(int(n_vendas_ano))], False),
            ],
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
    fig_rank.update_traces(hovertemplate=_hover_currency(show_x=True))
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
            colorscale="Blues",
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
            fig_lucro = go.Figure()
            # Linha 1: Lucro Líquido
            fig_lucro.add_trace(
                go.Scatter(
                    x=lucro_labels,
                    y=serie_lucro_liquido,
                    mode="lines+markers+text",
                    name="Lucro Líquido",
                    line=dict(color="#34495e", width=3),
                    marker=dict(color=colors_liq, size=8, line=dict(width=2, color=colors_liq)),
                    text=text_liq,
                    textposition="top center",
                    textfont=dict(color=colors_liq, size=12, family="Arial Black"),
                    connectgaps=False,
                    hovertemplate=_hover_currency(show_x=True),
                )
            )
            # Linha 2: Lucro Líquido antes da Depreciação
            fig_lucro.add_trace(
                go.Scatter(
                    x=lucro_labels,
                    y=serie_lucro_antes_deprec,
                    mode="lines+markers+text",
                    name="Lucro Líquido antes da Depreciação",
                    line=dict(color="#95a5a6", width=2, dash="dot"),
                    marker=dict(color=colors_antes, size=6, symbol="circle-open", line=dict(width=2, color=colors_antes)),
                    text=text_antes,
                    textposition="bottom center",
                    textfont=dict(color=colors_antes, size=10),
                    connectgaps=False,
                    hovertemplate=_hover_currency(show_x=True),
                )
            )

            fig_lucro.update_layout(
                title=dict(text=f"Lucro Líquido – {ano}", font=dict(size=title_size)),
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
                margin=dict(t=80, b=90),
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
            tabela_mes.style.format(_fmt_currency).applymap(
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

    df_rep = pd.DataFrame(
        {
            "Mês": [MESES_LABELS[i] for i in range(12)],
            "Valor Reposto": reposicao.values,
            "CMV": cmv,
        }
    )
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df_rep["Mês"], y=df_rep["Valor Reposto"], name="Valor reposto", hovertemplate=_hover_currency(show_x=True)))
    fig.add_trace(go.Bar(x=df_rep["Mês"], y=df_rep["CMV"], name="CMV", hovertemplate=_hover_currency(show_x=True)))
    fig.update_layout(
        barmode="group",
        title=f"Reposição x Custo Mercadoria – {ano_reposicao}",
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
    st.plotly_chart(fig, use_container_width=True, config=_plotly_config(simplified=simplified))


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
