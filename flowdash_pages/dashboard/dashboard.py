from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from shared.db import ensure_db_path_or_raise, get_conn
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
        build_meta_gauge_dashboard("Meta do Dia", perc_dia, bronze_pct_calc, prata_pct_calc, _fmt_currency(valor_dia)),
        use_container_width=True,
    )
    c2.plotly_chart(
        build_meta_gauge_dashboard("Meta da Semana", perc_sem, bronze_pct_calc, prata_pct_calc, _fmt_currency(valor_sem)),
        use_container_width=True,
    )
    c3.plotly_chart(
        build_meta_gauge_dashboard("Meta do Mês", perc_mes, bronze_pct_calc, prata_pct_calc, _fmt_currency(valor_mes)),
        use_container_width=True,
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
    st.subheader("Visão Geral")
    metrics = _calc_monthly_metrics(db_path, ano_selecionado, vars_dre)
    df_ano = df_entrada[df_entrada["ano"] == ano_selecionado]
    ultimo_mes = int(df_ano["mes"].max()) if not df_ano.empty else datetime.today().month

    receita_bruta = sum(m.get("fat", 0.0) or 0.0 for m in metrics)
    receita_liq = sum(m.get("receita_liq", 0.0) or 0.0 for m in metrics)
    lucro_bruto = sum(m.get("lucro_bruto", 0.0) or 0.0 for m in metrics)
    margem_bruta_pct = (lucro_bruto / receita_liq * 100.0) if receita_liq else 0.0
    margem_contrib = sum(m.get("margem_contrib", 0.0) or 0.0 for m in metrics)
    margem_contrib_pct = (margem_contrib / receita_liq * 100.0) if receita_liq else 0.0
    ebitda_total = sum(m.get("ebitda", 0.0) or 0.0 for m in metrics)
    lucro_liq_total = sum(m.get("lucro_liq", 0.0) or 0.0 for m in metrics)
    n_vendas = sum(m.get("n_vendas", 0) or 0 for m in metrics)
    ticket_medio = (receita_bruta / n_vendas) if n_vendas else 0.0
    crescimento = _growth_mm(metrics, ultimo_mes)

    ref_mes = metrics[ultimo_mes - 1] if ultimo_mes - 1 < len(metrics) else {}
    divida_estoque = ref_mes.get("divida_estoque", 0.0) or 0.0
    indice_endividamento = ref_mes.get("indice_endividamento_pct", 0.0) or 0.0

    hoje = date.today()
    bancos_dict = _somar_bancos_totais(db_path, hoje)
    disp_caixa, disp_caixa2, _ = _ultimo_caixas_ate(db_path, hoje)
    saldo_total = float(disp_caixa + disp_caixa2 + sum(bancos_dict.values()))

    linhas: List[Tuple[str, str, Optional[str]]] = [
        ("Crescimento m/m", _fmt_percent(crescimento), None),
        ("Ticket médio", _fmt_currency(ticket_medio), None),
        ("Nº de vendas", str(n_vendas), None),
        ("Saldo disponível", _fmt_currency(saldo_total), None),
    ]
    _cards_row(linhas, cols_per_row=3)

    bancos_items = []
    for nome, valor in bancos_dict.items():
        bancos_items.append((nome, _fmt_currency(valor), None))
    bancos_items.append(("Caixa", _fmt_currency(disp_caixa), None))
    bancos_items.append(("Caixa 2", _fmt_currency(disp_caixa2), None))
    if bancos_items:
        st.markdown("**Saldos por conta**")
        _cards_row(bancos_items, cols_per_row=4)


def render_endividamento(db_path: str) -> None:
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
                marker=dict(colors=["#27ae60", "#e67e22"]),
                text=[_fmt_currency(total_pago), _fmt_currency(total_aberto)],
                textinfo="percent+text",
                showlegend=False,
            )
        ]
    )
    fig_total.update_layout(
        height=520,
        margin=dict(l=10, r=10, t=10, b=10),
    )

    # duas colunas principais: Endividamento (esquerda) e Metas (direita)
    col_endiv, col_meta = st.columns(2)

    # --- coluna de Endividamento (donut total + donuts por empréstimo) ---
    with col_endiv:
        st.markdown("### Endividamento")

        # coluna do donut total e coluna dos donuts por empréstimo
        col_geral, col_emp = st.columns([3, 2])

        # donut total
        with col_geral:
            st.plotly_chart(fig_total, use_container_width=True)
            st.caption("Dívida total em empréstimos")
            valor_total = (total_pago or 0) + (total_aberto or 0)
            st.markdown(f"**{_fmt_currency(valor_total)}**")

        # donuts individuais por empréstimo (empilhados na vertical)
        with col_emp:
            for card in mini_cards[:3]:
                st.caption(card["descricao"])
                st.write(f"Contratado: {_fmt_currency(card['contratado'])}")

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
                        )
                    ]
                )

                fig.update_traces(
                    textposition="inside",
                    textfont_size=11,
                )

                fig.update_layout(
                    height=210,
                    margin=dict(l=0, r=0, t=10, b=0),
                )

                st.plotly_chart(fig, use_container_width=True)

    # --- coluna de Metas da Loja (fica à direita) ---
    with col_meta:
        st.markdown("### Metas da Loja")
        render_metas_resumo_dashboard(db_path)


def render_graficos_mensais(metrics: List[Dict], ano: int, df_entrada: pd.DataFrame, df_saida: pd.DataFrame) -> None:
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
                )
            )
            fig_lucro.update_layout(
                title=f"Lucro Líquido – {ano}",
                legend=dict(
                    orientation="h",
                    yanchor="top",
                    y=-0.2,
                    xanchor="center",
                    x=0.5,
                ),
                margin=dict(b=80),
            )

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
        )
    )
    fig_balanco.update_layout(
        barmode="group",
        title=f"Balanço Mensal – {ano}",
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.25,
            xanchor="center",
            x=0.5,
        ),
        margin=dict(b=90),
    )

    col1, col2 = st.columns(2)
    with col1:
        if ano != 2025:
            st.warning("Lucro líquido só está disponível a partir de outubro de 2025. Não há dados consistentes para anos anteriores.")
        else:
            if show_lucro and fig_lucro is not None:
                st.plotly_chart(fig_lucro, use_container_width=True)
            else:
                st.warning("Não há dados de lucro líquido registrados entre outubro e dezembro de 2025.")
    with col2:
        with st.container():
            st.plotly_chart(fig_balanco, use_container_width=True)
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


def render_analise_anual(df_entrada: pd.DataFrame, anos_multiselect: List[int]) -> None:
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

    fig_fat_ano = px.line(
        fat_anual,
        x="ano",
        y="total",
        markers=True,
        title="Faturamento Anual",
        labels={"ano": "Ano", "total": "Faturamento"},
        text="total_fmt",  # rótulo em R$ em cada ponto
    )

    # posiciona os rótulos acima dos pontos
    fig_fat_ano.update_traces(textposition="top center")

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

    fig_mm = go.Figure()
    for ano in anos_multiselect:
        df_ano = fat_mensal[fat_mensal["ano"] == ano].sort_values("mes")
        if df_ano.empty:
            continue
        is_atual = ano == ano_atual
        is_prev = ano_anterior is not None and ano == ano_anterior
        color = "#9b59b6" if is_atual else ("#2980b9" if is_prev else "#bdc3c7")
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
            )
        )
    fig_mm.add_hline(y=0, line_color="#888", line_dash="dash", opacity=0.7)
    fig_mm.update_layout(
        title="Faturamento Mensal (por ano)",
        xaxis_title="Mês",
        yaxis_title="Faturamento (R$)",
    )

    ranking = fat_mensal.copy()
    ranking["label"] = ranking.apply(lambda r: f"{MESES_LABELS[int(r['mes']) - 1]}/{int(r['ano'])}", axis=1)
    top = ranking.sort_values("total", ascending=False).head(8)
    fig_rank = px.bar(top, x="label", y="total", title="Top meses (Faturamento)", labels={"label": "Mês/Ano", "total": "Faturamento"})

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
    fig_heat.update_layout(title="Heatmap de Faturamento")

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(fig_fat_ano, use_container_width=True)
    with col2:
        st.plotly_chart(fig_mm, use_container_width=True)
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
        st.plotly_chart(fig_rank, use_container_width=True)
    with col4:
        st.plotly_chart(fig_heat, use_container_width=True)


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


def render_bloco_faturamento_anual(df_entrada: pd.DataFrame, anos_multiselect: List[int]) -> None:
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
        text="total_fmt",
    )
    fig_fat_ano.update_traces(textposition="top center")
    st.plotly_chart(fig_fat_ano, use_container_width=True)


def render_bloco_faturamento_mensal(df_entrada: pd.DataFrame, anos_multiselect: List[int]) -> None:
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

    fig_mm = go.Figure()
    for ano in anos_multiselect:
        df_ano = fat_mensal[fat_mensal["ano"] == ano].sort_values("mes")
        if df_ano.empty:
            continue
        is_atual = ano == ano_atual
        is_prev = ano_anterior is not None and ano == ano_anterior
        color = "#9b59b6" if is_atual else ("#2980b9" if is_prev else "#bdc3c7")
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
            )
        )
    fig_mm.add_hline(y=0, line_color="#888", line_dash="dash", opacity=0.7)
    fig_mm.update_layout(
        title="Faturamento Mensal (por ano)",
        xaxis_title="Mês",
        yaxis_title="Faturamento (R$)",
        height=480,
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.25,
            xanchor="center",
            x=0.5,
        ),
        margin=dict(b=90),
    )

    tabela_mm = (
        fat_mensal.pivot(index="ano", columns="mes_label", values="total")
        .reindex(columns=MESES_LABELS, fill_value=0.0)
        .fillna(0.0)
    )
    tabela_mm = tabela_mm.reset_index().rename(columns={"ano": "Ano"})
    tabela_mm_styled = tabela_mm.style.format(
        {m: _fmt_currency for m in MESES_LABELS}
    )

    st.plotly_chart(fig_mm, use_container_width=True)
    st.markdown("**Faturamento Mês a Mês (R$)**")
    st.dataframe(tabela_mm_styled, use_container_width=True)


def render_bloco_top_meses(df_entrada: pd.DataFrame, anos_multiselect: List[int]) -> None:
    st.subheader("Top meses (Faturamento)")
    fat_mensal = _prepare_fat_mensal(df_entrada, anos_multiselect)
    if fat_mensal is None or fat_mensal.empty:
        st.info("Sem dados para os anos selecionados.")
        return
    ranking = fat_mensal.copy()
    ranking["label"] = ranking.apply(lambda r: f"{MESES_LABELS[int(r['mes']) - 1]}/{int(r['ano'])}", axis=1)
    top = ranking.sort_values("total", ascending=False).head(8)
    fig_rank = px.bar(top, x="label", y="total", title="Top meses (Faturamento)", labels={"label": "Mês/Ano", "total": "Faturamento"})
    st.plotly_chart(fig_rank, use_container_width=True)


def render_bloco_heatmap(df_entrada: pd.DataFrame, anos_multiselect: List[int]) -> None:
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
    fig_heat.update_layout(title="Heatmap de Faturamento")
    st.plotly_chart(fig_heat, use_container_width=True)


def render_bloco_lucro_liquido(metrics: List[Dict], ano: int) -> None:
    st.subheader("Lucro Líquido")
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
                )
            )
            fig_lucro.update_layout(
                title=f"Lucro Líquido – {ano}",
                legend=dict(
                    orientation="h",
                    yanchor="top",
                    y=-0.25,
                    xanchor="center",
                    x=0.5,
                ),
                margin=dict(b=90),
            )

    if ano != 2025:
        st.warning("Lucro líquido só está disponível a partir de outubro de 2025. Não há dados consistentes para anos anteriores.")
    else:
        if show_lucro and fig_lucro is not None:
            st.plotly_chart(fig_lucro, use_container_width=True)
        else:
            st.warning("Não há dados de lucro líquido registrados entre outubro e dezembro de 2025.")


def render_bloco_balanco_mensal(df_entrada: pd.DataFrame, df_saida: pd.DataFrame, ano: int) -> None:
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
        )
    )
    fig_balanco.update_layout(
        barmode="group",
        title=f"Balanço Mensal – {ano}",
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.25,
            xanchor="center",
            x=0.5,
        ),
        margin=dict(b=90),
    )

    with st.container():
        st.plotly_chart(fig_balanco, use_container_width=True)
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
    fig.add_trace(go.Bar(x=df_rep["Mês"], y=df_rep["Valor Reposto"], name="Valor reposto"))
    fig.add_trace(go.Bar(x=df_rep["Mês"], y=df_rep["CMV"], name="CMV"))
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
    )
    st.plotly_chart(fig, use_container_width=True)


# ========================= Entrada principal =========================
def render_dashboard(caminho_banco: Optional[str]):
    """
    Dashboard principal FlowDash.
    """
    db_path = _resolve_db_path(caminho_banco)
    vars_dre = _load_vars_runtime(db_path)
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

    with st.container():
        render_chips_principais(df_entrada, db_path, int(ano_selecionado), vars_dre)

    with st.container():
        render_endividamento(db_path)

    with st.container():
        col1, col2 = st.columns(2)
        with col1:
            render_bloco_faturamento_anual(df_entrada, [int(a) for a in anos_multiselect])
        with col2:
            render_bloco_lucro_liquido(metrics, int(ano_selecionado))

    with st.container():
        col1, col2 = st.columns(2)
        with col1:
            render_bloco_faturamento_mensal(df_entrada, [int(a) for a in anos_multiselect])
        with col2:
            render_bloco_balanco_mensal(df_entrada, df_saida, int(ano_selecionado))

    with st.container():
        col1, col2 = st.columns(2)
        with col1:
            render_bloco_top_meses(df_entrada, [int(a) for a in anos_multiselect])
        with col2:
            render_bloco_heatmap(df_entrada, [int(a) for a in anos_multiselect])

    with st.container():
        render_reposicao(df_mercadorias, metrics)
