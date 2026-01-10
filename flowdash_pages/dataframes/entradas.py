# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
from datetime import date
from typing import Optional, Iterable

import pandas as pd
import streamlit as st

from flowdash_pages.dataframes.filtros import (
    selecionar_ano,
    selecionar_mes,
    resumo_por_mes,
)
from utils.utils import formatar_moeda as _fmt_moeda_str

# ================= Descoberta de DB (segura) =================
try:
    # Usa nossa camada segura (não acessa session_state no import-time)
    from shared.db import get_db_path as _shared_get_db_path, ensure_db_path_or_raise
except Exception:
    _shared_get_db_path = None

    def ensure_db_path_or_raise(_: Optional[str] = None) -> str:
        for p in (
            os.path.join("data", "flowdash_data.db"),
            os.path.join("data", "dashboard_rc.db"),
            "dashboard_rc.db",
            os.path.join("data", "flowdash_template.db"),
        ):
            if os.path.exists(p):
                return p
        raise FileNotFoundError("Nenhum banco padrão encontrado.")

def _resolve_db_path(pref: Optional[str]) -> Optional[str]:
    if isinstance(pref, str) and os.path.exists(pref):
        return pref
    if callable(_shared_get_db_path):
        p = _shared_get_db_path()
        if isinstance(p, str) and os.path.exists(p):
            return p
    # fallbacks locais
    for p in (
        os.path.join("data", "flowdash_data.db"),
        os.path.join("data", "dashboard_rc.db"),
        "dashboard_rc.db",
        os.path.join("data", "flowdash_template.db"),
    ):
        if os.path.exists(p):
            return p
    return None

def _connect(db_like: Optional[str]) -> Optional[sqlite3.Connection]:
    try:
        db = ensure_db_path_or_raise(db_like)
    except Exception as e:
        st.error("❌ Banco de dados não encontrado para Entradas.")
        st.caption(str(e))
        return None
    try:
        return sqlite3.connect(db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    except Exception as e:
        st.error("❌ Erro ao conectar no banco (Entradas).")
        st.exception(e)
        return None

def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND LOWER(name)=LOWER(?) LIMIT 1;",
        (name,),
    )
    return cur.fetchone() is not None

def _find_first_table(conn: sqlite3.Connection, candidates: Iterable[str]) -> Optional[str]:
    for t in candidates:
        if _table_exists(conn, t):
            return t
    # fallback: qualquer tabela que contenha 'entrada' no nome
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
    rows = cur.fetchall()
    for (nm,) in rows:
        if "entrada" in nm.lower():
            return nm
    return None

# ---------------- Helpers de UI/format ----------------
def _auto_df_height(df: pd.DataFrame, row_px: int = 30, header_px: int = 36, pad_px: int = 6, max_px: int = 1200) -> int:
    """Altura aproximada (linhas compactas)."""
    n = int(len(df))
    h = header_px + (n * row_px) + pad_px
    return min(h, max_px)

def _safe_to_datetime(s: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(s):
        return s
    return pd.to_datetime(s, errors="coerce")

def _fmt_int_str(v) -> str:
    """Formata como inteiro (sem vírgula). Em branco para valores nulos/invalidos."""
    try:
        if pd.isna(v):
            return ""
        return str(int(float(v)))
    except Exception:
        try:
            return str(int(str(v).strip()))
        except Exception:
            return str(v)

def _zebra(df: pd.DataFrame, dark: str = "#12161d", light: str = "#1b212b") -> pd.io.formats.style.Styler:
    ncols = df.shape[1]
    def _row_style(row: pd.Series):
        bg = light if (row.name % 2) else dark
        return [f"background-color: {bg}"] * ncols
    return df.style.apply(_row_style, axis=1)

def _reorder_cols(df: pd.DataFrame, before_col_ci: str, target_before_ci: str) -> pd.DataFrame:
    """
    Move a coluna `before_col_ci` (case-insensitive) para antes de `target_before_ci`,
    se ambas existirem.
    """
    cols_map = {c.lower(): c for c in df.columns}
    if before_col_ci.lower() not in cols_map or target_before_ci.lower() not in cols_map:
        return df
    before = cols_map[before_col_ci.lower()]
    target = cols_map[target_before_ci.lower()]
    if before == target:
        return df
    cols = list(df.columns)
    cols.remove(before)
    cols.insert(cols.index(target), before)
    return df[cols]

# Mapeamento p/ títulos
_MESES_PT_ABREV = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
}

# ---------------- Página ----------------
def render(df_entrada: pd.DataFrame, caminho_banco: str | None = None) -> None:
    """
    Visão simples:
      - Tabelas nativas do Streamlit (st.dataframe), índice oculto.
      - 1ª tabela (Faturamento por mês) SEM SCROLL (todas as linhas).
      - 2ª tabela com a MESMA ALTURA da 1ª (usa scroll quando precisar).
      - 3ª tabela: SELECT * no mês (todas as colunas).
    """
    if not isinstance(df_entrada, pd.DataFrame) or df_entrada.empty:
        st.info("Nenhuma entrada encontrada (ou DataFrame inválido/vazio).")
        return

    # 1) Filtro por Ano
    ano, df_ano = selecionar_ano(df_entrada, key="entradas", label="Ano (Entradas)")
    if df_ano.empty:
        st.warning("Não há dados para o ano selecionado.")
        return

    # Cabeçalho com total do ano
    total_ano = float(pd.to_numeric(df_ano["Valor"], errors="coerce").sum())
    st.markdown(
        f"""
        <div style="font-size:1.25rem;font-weight:700;margin:6px 0 10px;">
            Ano selecionado: {ano} • Total no ano:
            <span style="color:#00C853;">{_fmt_moeda_str(total_ano)}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 2) Meses
    mes, df_mes = selecionar_mes(df_ano, key="entradas", label="Escolha um mês")

    # Auto-seleção mês atual (se houver dados); senão 1º mês com dados
    if ("Data" in df_ano.columns) and (mes is None or df_mes.empty):
        dt_all = _safe_to_datetime(df_ano["Data"])
        meses_com_dado = sorted(dt_all.dt.month.dropna().unique().tolist())
        hoje = date.today()
        mes_pref = None
        try:
            ano_int = int(ano)
        except Exception:
            ano_int = hoje.year
        if ano_int == hoje.year and hoje.month in meses_com_dado:
            mes_pref = hoje.month
        elif meses_com_dado:
            mes_pref = int(meses_com_dado[0])
        if mes_pref is not None:
            mes = mes_pref
            df_mes = df_ano[dt_all.dt.month == mes].copy()

    # Para títulos
    mes_nome = _MESES_PT_ABREV.get(int(mes), "—") if mes is not None else "—"
    total_mes = float(pd.to_numeric(df_mes.get("Valor", 0), errors="coerce").sum()) if mes is not None else 0.0

    # 3) Duas colunas
    col_esq, col_dir = st.columns(2)

    # 3.1) ESQUERDA — Faturamento por mês (SEM SCROLL)
    with col_esq:
        st.markdown(
            f"**Faturamento por mês no ano** "
            f"<span style='color:#60a5fa;'>{ano}</span>",
            unsafe_allow_html=True,
        )
        resumo = resumo_por_mes(df_ano, valor_col="Valor")  # Mes, MesNome, Total
        tabela_mes = resumo[["MesNome", "Total"]].rename(columns={"MesNome": "Mês"})
        tabela_mes["Total"] = pd.to_numeric(tabela_mes["Total"], errors="coerce").fillna(0.0)

        altura_esq = _auto_df_height(tabela_mes, row_px=34, header_px=44, pad_px=14, max_px=10_000)
        st.dataframe(
            _zebra(tabela_mes).format({"Total": _fmt_moeda_str}),
            use_container_width=True,
            hide_index=True,
            height=altura_esq,
        )

    # 3.2) DIREITA — Detalhe diário do mês (MESMA ALTURA DA 1ª)
    with col_dir:
        st.markdown(
            f"**Detalhe diário do mês** "
            f"<span style='color:#60a5fa;'>{mes_nome}</span> "
            f"— Total: <span style='color:#00C853;'>{_fmt_moeda_str(total_mes)}</span>",
            unsafe_allow_html=True,
        )
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
        detalhado["Total"] = pd.to_numeric(detalhado.get("Total", 0), errors="coerce").fillna(0.0)

        st.dataframe(
            _zebra(detalhado[["Dia", "Total"]]).format({"Total": _fmt_moeda_str}),
            use_container_width=True,
            hide_index=True,
            height=altura_esq,
        )

    # 4) Tabela completa do mês (SELECT * — todas as colunas)
    st.divider()
    st.markdown("**Entradas do mês selecionado — Tabela completa**")

    if mes is None:
        st.info("Selecione um mês para visualizar a tabela completa.")
        return

    db_path = _resolve_db_path(caminho_banco)
    if not db_path:
        st.error("Não foi possível localizar o banco de dados.")
        return

    ano_int, mes_int = int(ano), int(mes)
    first_day = pd.Timestamp(year=ano_int, month=mes_int, day=1).date()
    last_day  = (pd.Timestamp(year=ano_int, month=mes_int, day=1) + pd.offsets.MonthEnd(1)).date()

    try:
        con = _connect(db_path)
        if not con:
            return
        tabela = _find_first_table(con, ("entrada", "entradas", "lancamentos_entrada", "vendas", "venda"))
        if tabela is None:
            st.warning("Tabela de Entradas não encontrada no banco.")
            return

        # Nota: nome da tabela foi obtido do sqlite_master (baixo risco); cercamos com aspas.
        query = f"""
            SELECT *
            FROM "{tabela}"
            WHERE date(Data) BETWEEN ? AND ?
            ORDER BY datetime(Data) ASC
        """
        df_full = pd.read_sql_query(query, con, params=(str(first_day), str(last_day)))
    except Exception as e:
        st.error(f"Falha ao ler do banco '{db_path}': {e}")
        return
    finally:
        try:
            con and con.close()
        except Exception:
            pass

    # --- Formatações mínimas (sem remover colunas à força) ---
    cmap = {c.lower(): c for c in df_full.columns}

    if "data" in cmap:
        c = cmap["data"]
        try:
            df_full[c] = pd.to_datetime(df_full[c], errors="coerce").dt.date.astype(str)
        except Exception:
            pass

    # Reposiciona 'maquineta' antes de 'usuario' (se existirem)
    df_full = _reorder_cols(df_full, before_col_ci="maquineta", target_before_ci="usuario")

    # Formatação de valores e inteiros
    fmt_map: dict[str, any] = {}
    for key in ("valor", "valor_liquido", "valorliquido", "valor_liq", "valorliq"):
        if key in cmap:
            fmt_map[cmap[key]] = _fmt_moeda_str
    for key in ("parcela", "parcelas", "num_parcelas"):
        if key in cmap:
            fmt_map[cmap[key]] = _fmt_int_str

    styled_full = _zebra(df_full).format(fmt_map) if fmt_map else _zebra(df_full)

    altura_full = _auto_df_height(df_full, max_px=1200)
    st.dataframe(
        styled_full,
        use_container_width=True,
        hide_index=True,
        height=altura_full,
    )
