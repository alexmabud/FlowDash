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

# ================= Descoberta de DB (segura) =================
try:
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
        st.error("❌ Banco de dados não encontrado para Empréstimos/Financiamentos.")
        st.caption(str(e))
        return None
    try:
        return sqlite3.connect(db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    except Exception as e:
        st.error("❌ Erro ao conectar no banco (Empréstimos/Financiamentos).")
        st.exception(e)
        return None

def _object_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE LOWER(name)=LOWER(?)
          AND type IN ('table','view')
        LIMIT 1;
        """,
        (name,),
    )
    return cur.fetchone() is not None

def _find_first_table_or_view(conn: sqlite3.Connection, candidates: Iterable[str]) -> Optional[str]:
    for t in candidates:
        if _object_exists(conn, t):
            return t
    # fallback: qualquer **tabela ou view** que contenha 'emprest' ou 'financ'
    cur = conn.execute("SELECT type, name FROM sqlite_master WHERE type IN ('table','view');")
    rows = cur.fetchall()
    for (_type, nm) in rows:
        l = (nm or "").lower()
        if ("emprest" in l) or ("financ" in l):
            return nm
    return None

# ---------------- Helpers de UI/format ----------------
def _auto_df_height(df: pd.DataFrame, row_px: int = 30, header_px: int = 36, pad_px: int = 6, max_px: int = 1200) -> int:
    n = int(len(df))
    h = header_px + (n * row_px) + pad_px
    return min(h, max_px)

def _safe_to_datetime(s: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(s):
        return s
    return pd.to_datetime(s, errors="coerce")

def _fmt_moeda_str(v) -> str:
    try:
        return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v)

def _fmt_int_str(v) -> str:
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

_MESES_PT_ABREV = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
}

# ---------------- Colunas tolerantes (Data/Valor) ----------------
_DATA_CANDIDATAS = [
    "Data", "data",
    "Data_Vencimento", "data_vencimento", "vencimento",
    "DataReferencia", "data_referencia", "Data_Ref", "data_ref",
    # cobrir schemas comuns de empréstimos
    "data_contratacao", "data_inicio_pagamento", "data_lancamento", "data_quitacao",
]

_VALOR_CANDIDATAS = [
    "valor_parcela", "Valor_Parcela", "parcela_valor",
    "Valor", "valor", "valor_total", "Total", "total",
    "principal", "amortizacao", "amortização", "juros", "multa", "tarifa",
]

def _resolver_coluna_data(df: pd.DataFrame) -> Optional[str]:
    cmap = {c.lower(): c for c in df.columns}
    for k in _DATA_CANDIDATAS:
        if k.lower() in cmap:
            return cmap[k.lower()]
    # fallback amplo: primeira coluna com "data"
    for c in df.columns:
        if "data" in c.lower():
            return c
    return None

def _construir_coluna_valor(df: pd.DataFrame) -> pd.DataFrame:
    cmap = {c.lower(): c for c in df.columns}
    # 1) preferir colunas de parcela/valor
    for k in ("valor_parcela", "valor_total", "valor", "total"):
        if k in cmap:
            df["Valor"] = pd.to_numeric(df[cmap[k]], errors="coerce")
            return df
    # 2) somatório de partes
    soma = None
    for k in ("principal", "amortizacao", "amortização", "juros", "multa", "tarifa"):
        if k in cmap:
            v = pd.to_numeric(df[cmap[k]], errors="coerce")
            soma = v if soma is None else (soma + v)
    df["Valor"] = soma if soma is not None else pd.to_numeric(0.0)
    return df

# ---------------- Página ----------------
def render(df_base: Optional[pd.DataFrame] = None, caminho_banco: str | None = None) -> None:
    # 0) Obter DF base
    if df_base is None:
        db_path = _resolve_db_path(caminho_banco)
        if not db_path:
            st.error("Não foi possível localizar o banco de dados.")
            return
        con = _connect(db_path)
        if not con:
            return
        try:
            tabela = _find_first_table_or_view(con, (
                "emprestimos_financiamentos",
                "emprestimo_financiamento",
                "emprestimos",
                "financiamentos",
                "emprestimo",
                "financiamento",
            ))
            if tabela is None:
                st.warning("Tabela/View de Empréstimos/Financiamentos não encontrada no banco.")
                return
            df_base = pd.read_sql_query(f'SELECT * FROM "{tabela}"', con)
        except Exception as e:
            st.error(f"Falha ao ler do banco: {e}")
            return
        finally:
            try:
                con and con.close()
            except Exception:
                pass

    if not isinstance(df_base, pd.DataFrame) or df_base.empty:
        st.info("Nenhum empréstimo/financiamento encontrado (ou DataFrame inválido/vazio).")
        return

    # 1) Resolver data e valor para resumos
    col_data = _resolver_coluna_data(df_base)
    if not col_data:
        st.error("Não foi possível identificar a coluna de Data (ex.: data_contratacao/data_inicio_pagamento/...).")
        return

    df_work = df_base.copy()
    df_work[col_data] = _safe_to_datetime(df_work[col_data])
    df_work = _construir_coluna_valor(df_work)

    # 2) Filtro por Ano
    df_pad = df_work.rename(columns={col_data: "Data"})
    ano, df_ano = selecionar_ano(df_pad, key="empfin", label="Ano (Empréstimos/Financiamentos)")
    if df_ano.empty:
        st.warning("Não há dados para o ano selecionado.")
        return

    total_ano = float(pd.to_numeric(df_ano["Valor"], errors="coerce").sum())
    st.markdown(
        f"""
        <div style="font-size:1.1rem;font-weight:700;margin:6px 0 10px;">
            Ano selecionado: {ano} • Total no ano:
            <span style="color:#00C853;">{_fmt_moeda_str(total_ano)}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 3) Escolha de mês (mantemos para filtrar a tabela completa)
    mes, df_mes = selecionar_mes(df_ano, key="empfin", label="Escolha um mês")

    # Auto-seleção do mês (se nada escolhido)
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

    # 4) Layout final: esquerda menor com total por mês; direita com tabela completa
    col_esq, col_dir = st.columns([0.36, 1.64])

    with col_esq:
        st.markdown(f"**Totais por mês — {ano}**")
        resumo = resumo_por_mes(df_ano, valor_col="Valor")
        tabela_mes = resumo[["MesNome", "Total"]].rename(columns={"MesNome": "Mês"})
        tabela_mes["Total"] = pd.to_numeric(tabela_mes["Total"], errors="coerce").fillna(0.0)
        altura_esq = _auto_df_height(tabela_mes, row_px=34, header_px=44, pad_px=14, max_px=800)
        st.dataframe(
            _zebra(tabela_mes).format({"Total": _fmt_moeda_str}),
            use_container_width=True,
            hide_index=True,
            height=altura_esq,
        )

    with col_dir:
        st.markdown("**Tabela completa do mês selecionado**")

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
            tabela = _find_first_table_or_view(con, (
                "emprestimos_financiamentos",
                "emprestimo_financiamento",
                "emprestimos",
                "financiamentos",
                "emprestimo",
                "financiamento",
            ))
            if tabela is None:
                st.warning("Tabela/View de Empréstimos/Financiamentos não encontrada no banco.")
                return

            df_probe = pd.read_sql_query(f'SELECT * FROM "{tabela}" LIMIT 1;', con)
            col_data_full = _resolver_coluna_data(df_probe) if not df_probe.empty else None
            if not col_data_full:
                col_data_full = "data_contratacao"  # fallback amplo

            query = f'''
                SELECT *
                FROM "{tabela}"
                WHERE date({col_data_full}) BETWEEN ? AND ?
                ORDER BY datetime({col_data_full}) ASC
            '''
            df_full = pd.read_sql_query(query, con, params=(str(first_day), str(last_day)))
        except Exception as e:
            st.error(f"Falha ao ler do banco '{db_path}': {e}")
            return
        finally:
            try:
                con and con.close()
            except Exception:
                pass

        # formatações básicas
        cmap = {c.lower(): c for c in df_full.columns}
        if col_data_full.lower() in cmap:
            c = cmap[col_data_full.lower()]
            try:
                df_full[c] = pd.to_datetime(df_full[c], errors="coerce").dt.date.astype(str)
            except Exception:
                pass

        df_full = _reorder_cols(df_full, before_col_ci="maquineta", target_before_ci="usuario")

        fmt_map: dict[str, any] = {}
        for key in ("valor", "valor_parcela", "valor_total", "principal", "juros", "multa", "tarifa"):
            if key in cmap:
                fmt_map[cmap[key]] = _fmt_moeda_str
        for key in ("parcela", "parcelas", "num_parcelas", "n_parcelas", "parcelas_total", "parcelas_pagas"):
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
