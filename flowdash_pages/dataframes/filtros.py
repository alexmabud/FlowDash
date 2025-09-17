# flowdash_pages/dataframes/filtros.py
from __future__ import annotations

from typing import Optional, Tuple
import pandas as pd
import streamlit as st

# Abreviações para botões e nomes completos para exibição
MESES_ABREV = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
}
MESES_NOME = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
    7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
}


# ============================== Utils internos ==============================
def _ensure_datetime(df: pd.DataFrame, col: str = "Data") -> pd.DataFrame:
    """Garante que df[col] exista e seja datetime (coerce)."""
    if not isinstance(df, pd.DataFrame):
        return pd.DataFrame(columns=[col])
    if col not in df.columns:
        df = df.copy()
        df[col] = pd.NaT
        return df
    if not pd.api.types.is_datetime64_any_dtype(df[col]):
        df = df.copy()
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


# ============================== API pública ==============================
def selecionar_ano(
    df: pd.DataFrame,
    key: str,
    label: str = "Ano",
    default: Optional[int] = None,
) -> Tuple[Optional[int], pd.DataFrame]:
    """
    Mostra um selectbox de anos existentes em df['Data'] e retorna (ano_escolhido, df_filtrado).
    - df precisa ter coluna 'Data' (qualquer tipo; será convertida para datetime).
    - key deve ser único por página para não conflitar com outros filtros.
    """
    df = _ensure_datetime(df, "Data")
    anos = sorted({int(y) for y in df["Data"].dropna().dt.year.unique().tolist()})
    if not anos:
        st.info("Sem dados com coluna 'Data' para filtrar por Ano.")
        return None, df.iloc[0:0].copy()

    # índice default
    if default is not None and default in anos:
        idx = anos.index(default)
    else:
        # último ano disponível como padrão (ex.: ano mais recente)
        idx = len(anos) - 1

    ano_escolhido = st.selectbox(label, anos, index=idx, key=f"ano_{key}")
    df_ano = df[df["Data"].dt.year == int(ano_escolhido)].copy()
    return int(ano_escolhido), df_ano


def selecionar_mes(
    df_ano: pd.DataFrame,
    key: str,
    label: str = "Mês",
    colunas_por_linha: int = 6,
) -> Tuple[Optional[int], pd.DataFrame]:
    """
    Renderiza uma grade de botões (Jan..Dez). Retorna (mes_escolhido, df_mes).
    - Lembra a seleção em st.session_state[f"mes_{key}_selecionado"].
    - Botões de meses sem dados ficam desabilitados (mas aparecem).
    """
    df_ano = _ensure_datetime(df_ano, "Data")
    st.caption(label)

    skey = f"mes_{key}_selecionado"
    meses_presentes = sorted({int(m) for m in df_ano["Data"].dropna().dt.month.unique().tolist()})

    # grade (ex.: 2 linhas de 6)
    for start in range(1, 13, colunas_por_linha):
        cols = st.columns(min(colunas_por_linha, 12 - start + 1))
        for i, col in enumerate(cols, start=start):
            abrev = MESES_ABREV.get(i, str(i))
            disabled = i not in meses_presentes
            if col.button(abrev, key=f"btn_{key}_{i}", use_container_width=True, disabled=disabled):
                st.session_state[skey] = i

    mes_escolhido = st.session_state.get(skey)
    if mes_escolhido:
        df_mes = df_ano[df_ano["Data"].dt.month == int(mes_escolhido)].copy()
    else:
        df_mes = df_ano.iloc[0:0].copy()

    return int(mes_escolhido) if mes_escolhido else None, df_mes


def resumo_por_mes(df_ano: pd.DataFrame, valor_col: str = "Valor") -> pd.DataFrame:
    """
    Retorna DataFrame com totais por mês (1..12) e nomes.
    Colunas: ['Mes', 'MesNome', 'Total'].
    """
    df = _ensure_datetime(df_ano, "Data").copy()
    if valor_col not in df.columns:
        df[valor_col] = 0.0

    df["Mes"] = df["Data"].dt.month
    agg = (
        df.groupby("Mes", dropna=True)[valor_col]
        .sum()
        .reindex(range(1, 13), fill_value=0.0)
        .rename("Total")
        .reset_index()
    )
    agg["MesNome"] = agg["Mes"].map(MESES_NOME)
    return agg[["Mes", "MesNome", "Total"]]


def add_ano_mes_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona colunas auxiliares: Ano (int), Mes (int), MesNome (str).
    Útil para visualizações com groupby e exibição tabular.
    """
    df = _ensure_datetime(df, "Data").copy()
    df["Ano"] = df["Data"].dt.year.astype("Int64")
    df["Mes"] = df["Data"].dt.month.astype("Int64")
    df["MesNome"] = df["Mes"].map(MESES_NOME)
    return df
