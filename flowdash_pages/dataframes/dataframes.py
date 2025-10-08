# flowdash_pages/dataframes/dataframes.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
from typing import Optional, Tuple, List, Iterable as _Iterable

import pandas as pd
import numpy as np
import streamlit as st

# Páginas específicas já existentes
from flowdash_pages.dataframes import entradas as page_entradas
from flowdash_pages.dataframes import saidas as page_saidas
from flowdash_pages.dataframes import mercadorias as page_mercadorias
from flowdash_pages.dataframes import emprestimos as page_emprestimos  # mantém
from flowdash_pages.dataframes import contas_a_pagar as page_contas_a_pagar  # NOVO (padronizado)

# Descoberta de DB (segura)
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

# ============================ Descoberta do DB ============================

def _get_db_path() -> Optional[str]:
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

def _connect() -> Optional[sqlite3.Connection]:
    try:
        db = ensure_db_path_or_raise(_get_db_path())
    except Exception as e:
        st.error("❌ Banco de dados não encontrado.")
        st.caption(str(e))
        return None
    try:
        return sqlite3.connect(db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    except Exception as e:
        st.error("❌ Erro ao conectar no banco de dados.")
        st.exception(e)
        return None

def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND LOWER(name)=LOWER(?) LIMIT 1;",
        (name,),
    )
    return cur.fetchone() is not None

# ============================ Heurísticas de colunas ============================

_USER_COLS = ["Usuario", "usuario", "vendedor", "responsavel", "user", "nome_usuario"]
_DATE_COLS = [
    "Data", "data", "data_venda", "data_lanc", "data_emissao", "created_at",
    "data_evento", "data_pagamento", "data_compra", "data_fatura", "data_vencimento",
    "Data_Vencimento", "data_contratacao", "data_inicio_pagamento", "data_lancamento"
]
_VALU_COLS = [
    "Valor", "valor", "valor_total", "valor_liquido", "valor_bruto",
    "Valor_Mercadoria", "valor_evento", "valor_pago", "valor_a_pagar",
    "preco_total", "preço_total", "total",
]
_LOAN_VALUE_PREFS = ["valor_parcela", "Valor_Parcela", "parcela_valor"]
_LOAN_PARTS = ["principal", "amortizacao", "amortização", "juros", "multa", "tarifa"]

def _pick_cols(conn: sqlite3.Connection, table: str) -> Optional[Tuple[Optional[str], str, str]]:
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info('{table}')")]
    lower = {c.lower(): c for c in cols}
    def _first(cands: List[str]) -> Optional[str]:
        for c in cands:
            if c.lower() in lower:
                return lower[c.lower()]
        return None
    u = _first(_USER_COLS)
    d = _first(_DATE_COLS)
    v = _first(_VALU_COLS)
    if not d or not v:
        return None
    return (u, d, v)

# ============================ Normalização/Utilidades ============================

def _ensure_listlike(x):
    if x is None:
        return []
    if isinstance(x, (str, bytes)):
        return [x]
    if isinstance(x, (pd.Series, pd.Index, np.ndarray, set, tuple, list)):
        return list(x)
    if isinstance(x, _Iterable):
        try:
            return list(x)  # type: ignore[arg-type]
        except Exception:
            return [x]
    return [x]

def _to_series1d(x) -> pd.Series:
    if isinstance(x, pd.Series):
        return x
    if isinstance(x, pd.DataFrame):
        if x.shape[1] == 1:
            return x.iloc[:, 0]
        squeezed = x.squeeze("columns")
        return squeezed if isinstance(squeezed, pd.Series) else x.iloc[:, 0]
    try:
        return pd.Series(x)
    except Exception:
        return pd.Series([x])

def _to_datetime(s) -> pd.Series:
    s1 = _to_series1d(s)
    return pd.to_datetime(s1, errors="coerce")

def _to_numeric(s) -> pd.Series:
    s1 = _to_series1d(s)
    out = pd.to_numeric(s1, errors="coerce")
    return out.astype(float).fillna(0.0)

def _fmt_moeda(v) -> str:
    try:
        return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v)

# ============================ Loaders públicos (mantidos) ============================

def carregar_df_entrada() -> pd.DataFrame:
    conn = _connect()
    if not conn:
        return pd.DataFrame(columns=["Usuario", "Data", "Valor"])
    try:
        for tb in ["entradas", "entrada", "lancamentos_entrada", "vendas", "venda"]:
            if _table_exists(conn, tb):
                picked = _pick_cols(conn, tb)
                if not picked:
                    continue
                user_col, date_col, valu_col = picked
                if user_col:
                    sql = f'SELECT "{user_col}" AS Usuario, "{date_col}" AS Data, "{valu_col}" AS Valor FROM "{tb}";'
                else:
                    sql = f'SELECT "LOJA" AS Usuario, "{date_col}" AS Data, "{valu_col}" AS Valor FROM "{tb}";'
                df = pd.read_sql(sql, conn)
                df["Data"] = _to_datetime(df["Data"])
                df["Valor"] = _to_numeric(df["Valor"])
                df["Usuario"] = df["Usuario"].where(df["Usuario"].notna(), "LOJA").astype(str)
                return df[["Usuario", "Data", "Valor"]].copy()
        return pd.DataFrame(columns=["Usuario", "Data", "Valor"])
    finally:
        conn.close()

def carregar_df_saidas() -> pd.DataFrame:
    conn = _connect()
    if not conn:
        return pd.DataFrame(columns=["Data", "Valor"])
    try:
        for tb in ["saidas", "saida", "lancamentos_saida", "pagamentos_saida", "pagamentos"]:
            if not _table_exists(conn, tb):
                continue
            picked = _pick_cols(conn, tb)
            if not picked:
                continue
            user_col, date_col, valu_col = picked
            df_all = pd.read_sql(f'SELECT * FROM "{tb}";', conn)
            cols_lower = {c.lower(): c for c in df_all.columns}
            if date_col not in df_all.columns and date_col.lower() in cols_lower:
                date_col = cols_lower[date_col.lower()]
            if valu_col not in df_all.columns and valu_col.lower() in cols_lower:
                # ✅ correção do typo: valucol -> valu_col
                valu_col = cols_lower[valu_col.lower()]
            if date_col not in df_all.columns or valu_col not in df_all.columns:
                # fallback
                valu_col = cols_lower.get("valor", valu_col)
                if date_col not in df_all.columns or valu_col not in df_all.columns:
                    continue
            df_all["Data"] = _to_datetime(df_all[date_col])
            df_all["Valor"] = _to_numeric(df_all[valu_col])
            if user_col and user_col in df_all.columns:
                df_all["Usuario"] = df_all[user_col].astype(str)
            keep_cols = ["Data", "Valor"] + (["Usuario"] if "Usuario" in df_all.columns else [])
            return df_all[keep_cols].copy()
        return pd.DataFrame(columns=["Data", "Valor"])
    finally:
        conn.close()

def carregar_df_mercadorias() -> pd.DataFrame:
    conn = _connect()
    if not conn:
        return pd.DataFrame(columns=["Data", "Valor"])
    try:
        for tb in ["mercadorias", "estoque", "produtos_mov", "produtos", "compras", "itens_venda"]:
            if not _table_exists(conn, tb):
                continue
            df = pd.read_sql(f'SELECT * FROM "{tb}";', conn)
            if df.empty:
                continue
            cols_lower = {c.lower(): c for c in df.columns}
            date_col = None
            for c in _DATE_COLS:
                if c.lower() in cols_lower:
                    date_col = cols_lower[c.lower()]
                    break
            if date_col is None:
                continue
            value_col = None
            for c in _VALU_COLS:
                if c.lower() in cols_lower:
                    value_col = cols_lower[c.lower()]
                    break
            out = pd.DataFrame()
            out["Data"] = _to_datetime(df[date_col])
            if value_col is not None:
                out["Valor"] = _to_numeric(df[value_col])
            else:
                preco_col = None
                qtd_col = None
                for name in ["preco", "preço", "valor_unit", "vl_unit", "unitario", "unit_price"]:
                    if name in cols_lower:
                        preco_col = cols_lower[name]
                        break
                for name in ["quantidade", "qtd", "qtde", "qte", "qty"]:
                    if name in cols_lower:
                        qtd_col = cols_lower[name]
                        break
                out["Valor"] = _to_numeric(df.get(preco_col, 0)) * _to_numeric(df.get(qtd_col, 0)) if (preco_col and qtd_col) else 0.0
            for c in _USER_COLS:
                if c.lower() in cols_lower:
                    out["Usuario"] = df[cols_lower[c.lower()]].astype(str)
                    break
            keep_cols = ["Data", "Valor"] + (["Usuario"] if "Usuario" in out.columns else [])
            return out[keep_cols].copy()
        return pd.DataFrame(columns=["Data", "Valor"])
    finally:
        conn.close()

def carregar_df_fatura_cartao() -> pd.DataFrame:
    conn = _connect()
    if not conn:
        return pd.DataFrame(columns=["Data", "Valor"])
    try:
        candidates = ["fatura_cartao_itens","cartao_fatura_itens","faturas_cartao_itens","fatura_cartao","cartao_fatura"]
        for tb in candidates:
            if not _table_exists(conn, tb):
                continue
            picked = _pick_cols(conn, tb)
            if picked:
                user_col, date_col, valu_col = picked
                df_all = pd.read_sql(f'SELECT * FROM "{tb}";', conn)
                cols_lower = {c.lower(): c for c in df_all.columns}
                if date_col not in df_all.columns and date_col.lower() in cols_lower:
                    date_col = cols_lower[date_col.lower()]
                if valu_col not in df_all.columns and valu_col.lower() in cols_lower:
                    valu_col = cols_lower[valu_col.lower()]
                if date_col not in df_all.columns or valu_col not in df_all.columns:
                    continue
                df_all["Data"] = _to_datetime(df_all[date_col])
                df_all["Valor"] = _to_numeric(df_all[valu_col])
                if user_col and user_col in df_all.columns:
                    df_all["Usuario"] = df_all[user_col].astype(str)
                keep_cols = ["Data", "Valor"] + (["Usuario"] if "Usuario" in df_all.columns else [])
                return df_all[keep_cols].copy()
            # fallback heurístico
            df_raw = pd.read_sql(f'SELECT * FROM "{tb}";', conn)
            if df_raw.empty:
                continue
            cols_lower = {c.lower(): c for c in df_raw.columns}
            date_col = None
            for c in _DATE_COLS:
                if c.lower() in cols_lower:
                    date_col = cols_lower[c.lower()]
                    break
            if date_col is None:
                continue
            value_col = None
            for c in _VALU_COLS:
                if c.lower() in cols_lower:
                    value_col = cols_lower[c.lower()]
                    break
            if value_col is None:
                continue
            out = pd.DataFrame()
            out["Data"] = _to_datetime(df_raw[date_col])
            out["Valor"] = _to_numeric(df_raw[value_col])
            for c in _USER_COLS:
                if c.lower() in cols_lower:
                    out["Usuario"] = df_raw[cols_lower[c.lower()]].astype(str)
                    break
            keep_cols = ["Data", "Valor"] + (["Usuario"] if "Usuario" in out.columns else [])
            return out[keep_cols].copy()
        return pd.DataFrame(columns=["Data", "Valor"])
    finally:
        conn.close()

def carregar_df_emprestimos() -> pd.DataFrame:
    conn = _connect()
    if not conn:
        return pd.DataFrame(columns=["Data", "Valor"])
    try:
        candidates = ["emprestimos_financiamentos","emprestimo_financiamento","emprestimos","financiamentos","emprestimo","financiamento"]
        for tb in candidates:
            if not _table_exists(conn, tb):
                continue
            df_all = pd.read_sql(f'SELECT * FROM "{tb}";', conn)
            if df_all.empty:
                continue
            cols_lower = {c.lower(): c for c in df_all.columns}
            date_col = None
            for c in _DATE_COLS:
                if c.lower() in cols_lower:
                    date_col = cols_lower[c.lower()]
                    break
            if date_col is None:
                if "data" in cols_lower:
                    date_col = cols_lower["data"]
                else:
                    continue
            out = pd.DataFrame()
            out["Data"] = _to_datetime(df_all[date_col])
            value_col = None
            for c in _LOAN_VALUE_PREFS:
                if c.lower() in cols_lower:
                    value_col = cols_lower[c.lower()]
                    break
            if value_col is None:
                for c in _VALU_COLS:
                    if c.lower() in cols_lower:
                        value_col = cols_lower[c.lower()]
                        break
            if value_col is not None:
                out["Valor"] = _to_numeric(df_all[value_col])
            else:
                soma = None
                for c in _LOAN_PARTS:
                    if c in cols_lower:
                        v = _to_numeric(df_all[cols_lower[c]])
                        soma = v if soma is None else (soma + v)
                out["Valor"] = soma if soma is not None else 0.0
            for c in _USER_COLS:
                if c.lower() in cols_lower:
                    out["Usuario"] = df_all[cols_lower[c.lower()]].astype(str)
                    break
            keep_cols = ["Data", "Valor"] + (["Usuario"] if "Usuario" in out.columns else [])
            return out[keep_cols].copy()
        return pd.DataFrame(columns=["Data", "Valor"])
    finally:
        conn.close()

def publicar_dfs_na_session() -> Tuple[pd.DataFrame, pd.DataFrame]:
    df_e = carregar_df_entrada()
    df_m = pd.DataFrame()
    st.session_state["df_entrada"] = df_e
    st.session_state["df_metas"]   = df_m
    return df_e, df_m

# ============================ Renderizador (roteador simples) ============================

def render():
    pagina = st.session_state.get("pagina_atual", "")
    try:
        pagina_str = " ".join(_ensure_listlike(pagina)).strip()
    except Exception:
        pagina_str = str(pagina)

    db_path = _get_db_path()
    pag_low = (pagina_str or "").lower()

    if "entradas" in pag_low or "entrada" in pag_low:
        df_e = carregar_df_entrada()
        page_entradas.render(df_e, caminho_banco=db_path)
        return

    if "saídas" in pag_low or "saidas" in pag_low or "saida" in pag_low:
        df_s = carregar_df_saidas()
        page_saidas.render(df_s)
        return

    if "mercadorias" in pag_low or "estoque" in pag_low:
        df_m = carregar_df_mercadorias()
        page_mercadorias.render(df_m)
        return

    if ("fatura" in pag_low and ("cartão" in pag_low or "cartao" in pag_low)) or ("cartões" in pag_low or "cartoes" in pag_low):
        df_f = carregar_df_fatura_cartao()
        try:
            from flowdash_pages.dataframes import faturas_cartao as page_faturas_cartao  # type: ignore
            page_faturas_cartao.render(df_f, caminho_banco=db_path)
        except Exception:
            st.warning("Página 'Fatura Cartão' ainda não instalada.")
        return

    if ("emprest" in pag_low) or ("financ" in pag_low):
        df_emp = carregar_df_emprestimos()
        page_emprestimos.render(df_emp if not df_emp.empty else None, caminho_banco=db_path)
        return

    # ---- NOVO: Contas a Pagar
    if ("contas a pagar" in pag_low) or ("contas" in pag_low and "pagar" in pag_low) or ("cap" in pag_low):
        try:
            page_contas_a_pagar.render(db_path)
        except Exception as e:
            st.error("Erro ao renderizar 'Contas a Pagar'.")
            st.exception(e)
        return

    st.info("Selecione uma opção no menu de DataFrames.")

# ============================ Retrocompat (get_dataframe) ============================

def get_dataframe(name: Optional[str] = None) -> pd.DataFrame:
    key = (name or "").strip().lower()
    if key in {"entradas", "entrada", "df_entrada", "vendas", "lancamentos_entrada"}:
        return carregar_df_entrada()
    if key in {"saidas", "saida", "df_saidas", "pagamentos_saida", "pagamentos"}:
        return carregar_df_saidas()
    if key in {"mercadorias", "estoque", "produtos", "compras", "itens_venda", "df_mercadorias"}:
        return carregar_df_mercadorias()
    if key in {"fatura_cartao", "faturas_cartao", "fatura_cartao_itens", "cartao_fatura_itens"}:
        return carregar_df_fatura_cartao()
    if key in {"emprestimos", "financiamentos", "emprestimo", "financiamento", "df_emprestimos"}:
        return carregar_df_emprestimos()
    return pd.DataFrame()
