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
        ("Receita Bruta", _fmt_currency(receita_bruta), None),
        ("Receita Líquida", _fmt_currency(receita_liq), None),
        ("Lucro Bruto", f"{_fmt_currency(lucro_bruto)} · {_fmt_percent(margem_bruta_pct)}", None),
        ("Margem Bruta (%)", _fmt_percent(margem_bruta_pct), None),
        ("Margem de Contribuição", f"{_fmt_currency(margem_contrib)} · {_fmt_percent(margem_contrib_pct)}", None),
        ("EBITDA", _fmt_currency(ebitda_total), None),
        ("Lucro Líquido (ano)", _fmt_currency(lucro_liq_total), None),
        ("Crescimento m/m", _fmt_percent(crescimento), None),
        ("Ticket médio", _fmt_currency(ticket_medio), None),
        ("Nº de vendas", str(n_vendas), None),
        ("Dívida (Estoque)", f"{_fmt_currency(divida_estoque)} · {_fmt_percent(indice_endividamento)}", None),
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
    st.subheader("Endividamento")
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
                textinfo="label+percent",
            )
        ]
    )
    col1, col2 = st.columns([2, 1])
    with col1:
        st.plotly_chart(fig_total, use_container_width=True)
    with col2:
        st.metric("Dívida total em empréstimos", _fmt_currency(total_pago + total_aberto))
        st.metric("Já pago", f"{_fmt_currency(total_pago)} ({_fmt_percent((total_pago/(total_pago+total_aberto))*100 if (total_pago+total_aberto) else 0)})")
        st.metric("Em aberto", f"{_fmt_currency(total_aberto)} ({_fmt_percent((total_aberto/(total_pago+total_aberto))*100 if (total_pago+total_aberto) else 0)})")

    st.markdown("**Por empréstimo**")
    for i in range(0, len(mini_cards), 3):
        cols = st.columns(3)
        for col, card in zip(cols, mini_cards[i : i + 3]):
            with col:
                fig = go.Figure(
                    data=[
                        go.Pie(
                            labels=["Pago", "Em aberto"],
                            values=[card["pago"], card["aberto"]],
                            hole=0.55,
                            marker=dict(colors=["#2ecc71", "#e74c3c"]),
                            textinfo="percent",
                            showlegend=False,
                        )
                    ]
                )
                st.plotly_chart(fig, use_container_width=True)
                st.caption(card["descricao"])
                st.write(f"Contratado: {_fmt_currency(card['contratado'])}")
                st.write(f"Pago: {_fmt_currency(card['pago'])} ({_fmt_percent(card['pago_pct'])})")
                st.write(f"Em aberto: {_fmt_currency(card['aberto'])} ({_fmt_percent(card['aberto_pct'])})")


def render_graficos_mensais(metrics: List[Dict], ano: int) -> None:
    st.subheader("Gráficos Mensais")
    meses = list(range(1, 13))
    df_lucro = pd.DataFrame(
        {
            "Mês": [MESES_LABELS[m - 1] for m in meses],
            "Lucro Líquido": [metrics[m - 1].get("lucro_liq", 0.0) if m - 1 < len(metrics) else 0.0 for m in meses],
        }
    )
    fig_lucro = px.line(df_lucro, x="Mês", y="Lucro Líquido", markers=True, title=f"Lucro Líquido – {ano}")

    fat = [metrics[m - 1].get("fat", 0.0) if m - 1 < len(metrics) else 0.0 for m in meses]
    saidas = [metrics[m - 1].get("total_saida_oper", 0.0) if m - 1 < len(metrics) else 0.0 for m in meses]
    resultado = [f - s for f, s in zip(fat, saidas)]
    df_balanco = pd.DataFrame(
        {
            "Mês": [MESES_LABELS[m - 1] for m in meses],
            "Faturamento": fat,
            "Saída": saidas,
            "Resultado": resultado,
        }
    )

    fig_balanco = go.Figure()
    fig_balanco.add_trace(go.Bar(x=df_balanco["Mês"], y=df_balanco["Faturamento"], name="Faturamento"))
    fig_balanco.add_trace(go.Bar(x=df_balanco["Mês"], y=df_balanco["Saída"], name="Saída"))
    fig_balanco.add_trace(go.Bar(x=df_balanco["Mês"], y=df_balanco["Resultado"], name="Resultado"))
    fig_balanco.add_trace(go.Scatter(x=df_balanco["Mês"], y=df_balanco["Resultado"], name="Linha Resultado", mode="lines+markers"))
    fig_balanco.update_layout(barmode="group", title=f"Balanço Mensal – {ano}")

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(fig_lucro, use_container_width=True)
    with col2:
        st.plotly_chart(fig_balanco, use_container_width=True)


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

    fig_fat_ano = px.line(
        fat_mensal,
        x="mes_label",
        y="total",
        color="ano",
        markers=True,
        title="Faturamento por Ano",
        labels={"mes_label": "Mês", "total": "Faturamento"},
    )

    def _calc_mm(df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values("mes")
        df["mm"] = df["total"].pct_change().fillna(0.0) * 100.0
        return df

    fat_mm = fat_mensal.groupby("ano", group_keys=False).apply(_calc_mm)
    fig_mm = px.line(
        fat_mm,
        x="mes_label",
        y="mm",
        color="ano",
        markers=True,
        title="Faturamento M/M",
        labels={"mes_label": "Mês", "mm": "% vs mês anterior"},
    )

    ranking = fat_mensal.copy()
    ranking["label"] = ranking.apply(lambda r: f"{MESES_LABELS[int(r['mes']) - 1]}/{int(r['ano'])}", axis=1)
    top = ranking.sort_values("total", ascending=False).head(8)
    fig_rank = px.bar(top, x="label", y="total", title="Top meses (Faturamento)", labels={"label": "Mês/Ano", "total": "Faturamento"})

    pivot = fat_mensal.pivot(index="mes_label", columns="ano", values="total").fillna(0.0)
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

    col3, col4 = st.columns(2)
    with col3:
        st.plotly_chart(fig_rank, use_container_width=True)
    with col4:
        st.plotly_chart(fig_heat, use_container_width=True)


def render_entradas(df_entrada: pd.DataFrame, ano: int) -> None:
    st.subheader("Entradas")
    formas = _formas_pagamento(df_entrada, ano)
    col1, col2 = st.columns(2)
    with col1:
        if formas.empty:
            st.info("Formas de pagamento não encontradas.")
        else:
            fig_formas = px.bar(
                formas,
                x="Total",
                y="Forma",
                orientation="h",
                title=f"Formas de Pagamento – {ano}",
                labels={"Total": "Total", "Forma": "Forma"},
            )
            st.plotly_chart(fig_formas, use_container_width=True)
    with col2:
        previstos = _previsto_dx(df_entrada)
        st.metric("Previsto amanhã (D+1)", _fmt_currency(previstos.get("d1", 0.0)))
        st.metric("Previsto em 7 dias (D+7)", _fmt_currency(previstos.get("d7", 0.0)))


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
    fig.update_layout(barmode="group", title=f"Reposição x Custo Mercadoria – {ano_reposicao}")
    st.plotly_chart(fig, use_container_width=True)


# ========================= Entrada principal =========================
def render_dashboard(caminho_banco: Optional[str]):
    """
    Dashboard principal FlowDash.
    """
    db_path = _resolve_db_path(caminho_banco)
    vars_dre = _load_vars_runtime(db_path)
    df_entrada, _ = _load_entradas_saidas(db_path)
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
        render_graficos_mensais(metrics, int(ano_selecionado))

    with st.container():
        render_analise_anual(df_entrada, [int(a) for a in anos_multiselect])

    with st.container():
        render_entradas(df_entrada, int(ano_selecionado))

    with st.container():
        render_reposicao(df_mercadorias, metrics)
