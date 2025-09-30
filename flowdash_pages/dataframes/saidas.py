# flowdash_pages/dataframes/saidas.py
from __future__ import annotations
import pandas as pd
import streamlit as st

from flowdash_pages.dataframes.filtros import (
    selecionar_ano,
    selecionar_mes,
    resumo_por_mes,
)

# Helpers (alinhados com Entradas)
def _fmt_moeda(v) -> str:
    try:
        return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v)

def _auto_df_height(df: pd.DataFrame, row_px: int = 30, header_px: int = 44, pad_px: int = 14, max_px: int = 10_000) -> int:
    """Altura base (usada na 3ª tabela e como fallback)."""
    n = int(len(df))
    h = header_px + (n * row_px) + pad_px
    return min(h, max_px)

def _safe_to_datetime(s: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(s):
        return s
    return pd.to_datetime(s, errors="coerce")

def _zebra(df: pd.DataFrame, dark: str = "#12161d", light: str = "#1b212b") -> pd.io.formats.style.Styler:
    """Zebra somente no fundo (não altera fonte/cor do texto)."""
    ncols = df.shape[1]
    def _row_style(row: pd.Series):
        bg = light if (row.name % 2) else dark
        return [f"background-color: {bg}"] * ncols
    return df.style.apply(_row_style, axis=1)

# Mapeamento fixo de meses (12 linhas garantidas)
_MESES_PT = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
}

def render(df_saidas: pd.DataFrame) -> None:
    """
    📤 Saídas — visão simples com:
      - 1ª tabela (Total por mês) com **exatamente 12 linhas** (Jan..Dez) e **sem scroll**.
      - 2ª tabela (Detalhe diário) com a **mesma altura** da 1ª (scroll quando necessário).
      - 3ª tabela: **todas as colunas** do mês selecionado (id oculto).
      - Zebra nas tabelas e cabeçalho destacado (valor em verde).
    """
    if not isinstance(df_saidas, pd.DataFrame) or df_saidas.empty:
        st.info("Nenhuma saída encontrada (ou DataFrame inválido/vazio).")
        return

    st.session_state["df_saidas"] = df_saidas

    # 1) Filtro por Ano
    ano, df_ano = selecionar_ano(df_saidas, key="saidas", label="Ano (Saídas)")
    if df_ano.empty:
        st.warning("Não há dados para o ano selecionado.")
        return

    # Cabeçalho destacado (maior, negrito e total em verde)
    total_ano_num = float(pd.to_numeric(df_ano["Valor"], errors="coerce").sum())
    st.markdown(
        f"""
        <div style="font-size:1.25rem;font-weight:700;margin:6px 0 10px;">
            Ano selecionado: {ano} • Total no ano:
            <span style="color:#00C853;">{_fmt_moeda(total_ano_num)}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 2) Grade de meses (define o mês ativo)
    mes, df_mes = selecionar_mes(df_ano, key="saidas", label="Escolha um mês")

    # 3) Layout em 2 colunas
    col_esq, col_dir = st.columns(2)

    # 3.1) ESQUERDA — Total por mês (12 linhas, SEM SCROLL)
    with col_esq:
        st.markdown("**Total por mês (ano selecionado)**")

        # Resumo original
        resumo = resumo_por_mes(df_ano, valor_col="Valor")  # Esperado: Mes, MesNome, Total

        # Base fixa de 12 meses (garante 12 linhas e ordem Jan..Dez)
        base = pd.DataFrame({"Mes": list(range(1, 13))})
        base["Mês"] = base["Mes"].map(_MESES_PT)

        # Usa 'Mes' do resumo; se não existir, recalcula a partir de df_ano
        if "Mes" in resumo.columns and "Total" in resumo.columns:
            tot = resumo[["Mes", "Total"]].copy()
        else:
            tmp = df_ano.copy()
            if not pd.api.types.is_datetime64_any_dtype(tmp["Data"]):
                tmp["Data"] = _safe_to_datetime(tmp["Data"])
            tmp["Mes"] = tmp["Data"].dt.month
            tot = tmp.groupby("Mes", dropna=True)["Valor"].sum().reset_index(name="Total")

        tabela_mes = base.merge(tot, on="Mes", how="left")
        tabela_mes["Total"] = pd.to_numeric(tabela_mes["Total"], errors="coerce").fillna(0.0)
        tabela_mes = tabela_mes[["Mês", "Total"]]  # exatamente 12 linhas

        # Altura precisa para 12 linhas (sem linhas “sobrando”)
        altura_esq = _auto_df_height(tabela_mes, row_px=34, header_px=44, pad_px=14, max_px=10_000)

        st.dataframe(
            _zebra(tabela_mes).format({"Total": _fmt_moeda}),
            use_container_width=True,
            hide_index=True,
            height=altura_esq,
        )

    # 3.2) DIREITA — Detalhe diário do mês (MESMA ALTURA da 1ª → com scroll)
    with col_dir:
        st.markdown("**Detalhe diário do mês**")
        if mes is None or df_mes.empty:
            detalhado = pd.DataFrame(columns=["Dia", "Total"])
        else:
            df_dia = df_mes.copy()
            if not pd.api.types.is_datetime64_any_dtype(df_dia["Data"]):
                df_dia["Data"] = _safe_to_datetime(df_dia["Data"])
            df_dia["Dia"] = df_dia["Data"].dt.date
            detalhado = (
                df_dia.groupby("Dia", dropna=True)["Valor"]
                .sum()
                .reset_index()
                .sort_values("Dia")
                .rename(columns={"Valor": "Total"})
            )
            detalhado["Total"] = pd.to_numeric(detalhado["Total"], errors="coerce").fillna(0.0)

        st.dataframe(
            _zebra(detalhado[["Dia", "Total"]]).format({"Total": _fmt_moeda}),
            use_container_width=True,
            hide_index=True,
            height=altura_esq,  # mesma altura da 1ª → com scroll quando necessário
        )

    # 4) Tabela completa do mês (TODAS AS COLUNAS)
    st.divider()
    st.markdown("**Saídas do mês selecionado — Tabela completa**")

    if mes is None or df_mes.empty:
        st.info("Selecione um mês para visualizar a tabela completa.")
        return

    # Cópia do mês selecionado com TODAS as colunas
    df_full = df_mes.copy()

    # Ocultar coluna id (case-insensitive)
    cols_map = {c.lower(): c for c in df_full.columns}
    if "id" in cols_map:
        df_full = df_full.drop(columns=[cols_map["id"]])

    # Ajustes de exibição: Data só data; Valor/valor_liquido em R$
    if "data" in cols_map:
        c = cols_map["data"]
        try:
            df_full[c] = pd.to_datetime(df_full[c], errors="coerce").dt.date.astype(str)
        except Exception:
            pass

    fmt_map: dict[str, any] = {}
    if "valor" in cols_map:
        fmt_map[cols_map["valor"]] = _fmt_moeda
    for key in ("valor_liquido", "valorliquido", "valor_liq", "valorliq"):
        if key in cols_map:
            fmt_map[cols_map[key]] = _fmt_moeda
            break

    styled_full = _zebra(df_full).format(fmt_map) if fmt_map else _zebra(df_full)

    altura_full = _auto_df_height(df_full, max_px=1200)
    st.dataframe(
        styled_full,
        use_container_width=True,
        hide_index=True,
        height=altura_full,
    )
