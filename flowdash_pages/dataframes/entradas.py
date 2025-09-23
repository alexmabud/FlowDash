# flowdash_pages/dataframes/entradas.py
from __future__ import annotations
import pandas as pd
import streamlit as st

from flowdash_pages.dataframes.filtros import (
    selecionar_ano,
    selecionar_mes,
    resumo_por_mes,
)

# Helpers locais (evita dependência do dataframes.py)
def _fmt_moeda(v) -> str:
    try:
        return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v)

def _auto_df_height(df: pd.DataFrame, row_px: int = 36, header_px: int = 38, pad_px: int = 8, max_px: int = 2000) -> int:
    n = int(len(df))
    h = header_px + (n * row_px) + pad_px
    return min(h, max_px)

def render(df_entrada: pd.DataFrame) -> None:
    """
    Renderiza a visão de 📥 Entradas usando um DataFrame padronizado com colunas:
      - 'Data' (datetime64)
      - 'Valor' (float)
      - 'Usuario' (str) — opcional
    """
   
    if not isinstance(df_entrada, pd.DataFrame) or df_entrada.empty:
        st.info("Nenhuma entrada encontrada (ou DataFrame inválido/vazio).")
        return

    # deixa disponível para outras páginas (ex.: Metas)
    st.session_state["df_entrada"] = df_entrada

    # 1) Filtro por Ano
    ano, df_ano = selecionar_ano(df_entrada, key="entradas", label="Ano (Entradas)")
    if df_ano.empty:
        st.warning("Não há dados para o ano selecionado.")
        return

    # Frase destacada com total do ano (verde)
    total_ano = _fmt_moeda(df_ano["Valor"].sum())
    st.markdown(
        f"""
        <div style="font-size:1.15rem;font-weight:600;margin:6px 0 10px;">
            Ano selecionado: <span style="font-weight:700">{ano}</span> •
            Total no ano: <span style="color:#00C853;font-weight:800">{total_ano}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 2) Grade de meses (define o mês ativo)
    mes, df_mes = selecionar_mes(df_ano, key="entradas", label="Escolha um mês")

    # 3) Layout em 2 colunas
    col_esq, col_dir = st.columns(2)

    # 3.1) ESQUERDA — Faturamento por mês (sem índice e sem scroll)
    with col_esq:
        st.markdown("**Faturamento por mês (ano selecionado)**")
        resumo = resumo_por_mes(df_ano, valor_col="Valor")  # Mes, MesNome, Total
        tabela_mes = resumo[["MesNome", "Total"]].rename(columns={"MesNome": "Mês"})
        tabela_mes["Total"] = tabela_mes["Total"].map(_fmt_moeda)
        altura_esq = _auto_df_height(tabela_mes)
        st.dataframe(
            tabela_mes,
            use_container_width=True,
            hide_index=True,
            height=altura_esq,
        )

    # 3.2) DIREITA — Detalhe diário do mês (mesma altura da esquerda; com scroll se precisar)
    with col_dir:
        st.markdown("**Detalhe diário do mês**")
        if mes is None or df_mes.empty:
            st.info("Selecione um mês nos botões para ver o detalhamento diário.")
        else:
            df_dia = df_mes.copy()
            df_dia["Dia"] = df_dia["Data"].dt.date
            detalhado = (
                df_dia.groupby("Dia", dropna=True)["Valor"]
                .sum()
                .reset_index()
                .sort_values("Dia")
                .rename(columns={"Valor": "Total"})
            )
            detalhado["Total"] = detalhado["Total"].map(_fmt_moeda)
            st.dataframe(
                detalhado[["Dia", "Total"]],
                use_container_width=True,
                hide_index=True,
                height=altura_esq,  # fixa mesma altura da esquerda (scroll só aqui quando precisar)
            )
