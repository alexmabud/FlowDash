# flowdash_pages/dataframes/dataframes.py
from __future__ import annotations

import os
import sqlite3
from typing import Optional, Tuple, List

import pandas as pd
import streamlit as st

# Helpers (ainda úteis para futuras páginas; usados nos módulos splitados)
from flowdash_pages.dataframes.filtros import (
    selecionar_ano,
    selecionar_mes,
    resumo_por_mes,
)

# Delegação para páginas específicas
from flowdash_pages.dataframes import entradas as page_entradas
from flowdash_pages.dataframes import saidas as page_saidas


# ============================ Descoberta do DB ============================

def _get_db_path() -> Optional[str]:
    # 1) session
    cand = st.session_state.get("caminho_banco")
    if isinstance(cand, str) and os.path.exists(cand):
        return cand

    # 2) shared.db
    try:
        from shared import db as sdb  # type: ignore
        for name in ("get_db_path", "db_path", "DB_PATH"):
            if hasattr(sdb, name):
                obj = getattr(sdb, name)
                p = obj() if callable(obj) else obj
                if isinstance(p, str) and os.path.exists(p):
                    return p
    except Exception:
        pass

    # 3) defaults (inclui seu legado `data/entrada.db`)
    for p in (
        os.path.join("data", "entrada.db"),         # legado
        os.path.join("data", "flowdash_data.db"),
        os.path.join("data", "dashboard_rc.db"),
        "dashboard_rc.db",
        os.path.join("data", "flowdash_template.db"),
    ):
        if os.path.exists(p):
            return p
    return None


def _connect() -> Optional[sqlite3.Connection]:
    db = _get_db_path()
    if not db:
        return None
    try:
        return sqlite3.connect(db)
    except Exception:
        return None


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND LOWER(name)=LOWER(?) LIMIT 1;",
        (name,),
    )
    return cur.fetchone() is not None


# ============================ Heurísticas de colunas ============================

_USER_COLS = ["Usuario", "usuario", "vendedor", "responsavel", "user", "nome_usuario"]
_DATE_COLS = ["Data", "data", "data_venda", "data_lanc", "data_emissao", "created_at", "data_evento", "data_pagamento", "data_compra"]
_VALU_COLS = [
    "Valor", "valor", "valor_total", "valor_liquido", "valor_bruto",
    "Valor_Mercadoria", "valor_evento", "valor_pago", "valor_a_pagar",
    "preco_total", "preço_total", "total",
]


def _pick_cols(conn: sqlite3.Connection, table: str) -> Optional[Tuple[Optional[str], str, str]]:
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info('{table}')")]
    lower = {c.lower(): c for c in cols}

    def _first(cands: List[str]) -> Optional[str]:
        for c in cands:
            if c.lower() in lower:
                return lower[c.lower()]
        return None

    u = _first(_USER_COLS)   # pode ser None
    d = _first(_DATE_COLS)   # obrigatório
    v = _first(_VALU_COLS)   # obrigatório
    if not d or not v:
        return None
    return (u, d, v)


def _to_datetime(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")


def _to_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def _fmt_moeda(v) -> str:
    try:
        return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v)


# ============================ Loaders públicos ============================

def carregar_df_entrada() -> pd.DataFrame:
    """
    Padroniza: Usuario, Data (datetime64), Valor (float).
    Compatível com seus scripts: lê `entradas` em `data/entrada.db` (coluna `data` minúscula).
    """
    conn = _connect()
    if not conn:
        return pd.DataFrame(columns=["Usuario", "Data", "Valor"])
    try:
        # ordem priorizando seu legado
        for tb in ["entradas", "entrada", "lancamentos_entrada", "vendas", "venda"]:
            if _table_exists(conn, tb):
                picked = _pick_cols(conn, tb)
                if not picked:
                    continue
                user_col, date_col, valu_col = picked
                if user_col:
                    sql = f'SELECT "{user_col}" AS Usuario, "{date_col}" AS Data, "{valu_col}" AS Valor FROM "{tb}";'
                else:
                    # sem coluna de usuário -> força LOJA (permite a página de Metas funcionar para LOJA)
                    sql = f'SELECT "LOJA" AS Usuario, "{date_col}" AS Data, "{valu_col}" AS Valor FROM "{tb}";'
                df = pd.read_sql(sql, conn)
                df["Data"] = _to_datetime(df["Data"])
                df["Valor"] = _to_numeric(df["Valor"])
                df["Usuario"] = df["Usuario"].astype(str).fillna("LOJA")
                return df
        return pd.DataFrame(columns=["Usuario", "Data", "Valor"])
    finally:
        conn.close()


def carregar_df_saidas() -> pd.DataFrame:
    """
    Padroniza Saídas para: Data (datetime64), Valor (float), mantendo demais colunas originais.
    Compatível com seus scripts: lê `saidas` em `data/entrada.db` (coluna `data` minúscula).
    """
    conn = _connect()
    if not conn:
        return pd.DataFrame(columns=["Data", "Valor"])
    try:
        for tb in ["saidas", "saida", "lancamentos_saida", "pagamentos_saida", "pagamentos"]:
            if _table_exists(conn, tb):
                picked = _pick_cols(conn, tb)
                if not picked:
                    continue
                user_col, date_col, valu_col = picked

                df_all = pd.read_sql(f'SELECT * FROM "{tb}";', conn)
                df_all["Data"] = _to_datetime(df_all[date_col])
                df_all["Valor"] = _to_numeric(df_all[valu_col])
                if user_col and user_col in df_all.columns:
                    df_all["Usuario"] = df_all[user_col].astype(str)
                return df_all

        return pd.DataFrame(columns=["Data", "Valor"])
    finally:
        conn.close()


def carregar_df_mercadorias() -> pd.DataFrame:
    """
    Detecta tabela de mercadorias/estoque e padroniza:
      - Data (datetime64)
      - Valor (float) -> tenta 'valor' ou 'preco_total' ou 'preco*quantidade'
      - Usuario (se houver)
    Tabelas candidatas: mercadorias, estoque, produtos_mov, produtos, compras, itens_venda.
    """
    conn = _connect()
    if not conn:
        return pd.DataFrame(columns=["Data", "Valor"])
    try:
        for tb in ["mercadorias", "estoque", "produtos_mov", "produtos", "compras", "itens_venda"]:
            if not _table_exists(conn, tb):
                continue

            df = pd.read_sql(f'SELECT * FROM "{tb}";', conn)
            cols_lower = {c.lower(): c for c in df.columns}

            # Data
            date_col = None
            for c in _DATE_COLS:
                if c.lower() in cols_lower:
                    date_col = cols_lower[c.lower()]
                    break
            if date_col is None:
                continue  # sem data, segue tentando outra tabela

            # Valor
            value_col = None
            for c in _VALU_COLS:
                if c.lower() in cols_lower:
                    value_col = cols_lower[c.lower()]
                    break

            df["Data"] = _to_datetime(df[date_col])

            if value_col is not None:
                df["Valor"] = _to_numeric(df[value_col])
            else:
                # tenta preco*quantidade
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
                if preco_col and qtd_col:
                    df["Valor"] = _to_numeric(df[preco_col]) * _to_numeric(df[qtd_col])
                else:
                    df["Valor"] = 0.0  # fallback

            # Usuario (opcional)
            for c in _USER_COLS:
                if c.lower() in cols_lower:
                    df["Usuario"] = df[cols_lower[c.lower()]].astype(str)
                    break

            return df

        return pd.DataFrame(columns=["Data", "Valor"])
    finally:
        conn.close()


def carregar_df_metas() -> pd.DataFrame:
    """
    Padroniza: vendedor, mensal, semanal, segunda..domingo, meta_ouro/prata/bronze.
    Se não existir tabela `metas`, devolve DF vazio com colunas esperadas.
    """
    conn = _connect()
    if not conn:
        return pd.DataFrame(columns=[
            "vendedor", "mensal", "semanal",
            "segunda", "terca", "quarta", "quinta", "sexta", "sabado", "domingo",
            "meta_ouro", "meta_prata", "meta_bronze",
        ])
    try:
        if not _table_exists(conn, "metas"):
            return pd.DataFrame(columns=[
                "vendedor", "mensal", "semanal",
                "segunda", "terca", "quarta", "quinta", "sexta", "sabado", "domingo",
                "meta_ouro", "meta_prata", "meta_bronze",
            ])
        df = pd.read_sql(
            """
            SELECT
                COALESCE(vendedor, 'LOJA')               AS vendedor,
                COALESCE(mensal, 0)                      AS mensal,
                COALESCE(semanal, 0)                     AS semanal,
                COALESCE(segunda, 0)                     AS segunda,
                COALESCE(terca, 0)                       AS terca,
                COALESCE(quarta, 0)                      AS quarta,
                COALESCE(quinta, 0)                      AS quinta,
                COALESCE(sexta, 0)                       AS sexta,
                COALESCE(sabado, 0)                      AS sabado,
                COALESCE(domingo, 0)                     AS domingo,
                COALESCE(meta_ouro, 16000)               AS meta_ouro,
                COALESCE(meta_prata, 14000)              AS meta_prata,
                COALESCE(meta_bronze, 12000)             AS meta_bronze
            FROM metas;
            """,
            conn,
        )
        for c in [
            "mensal", "semanal", "segunda", "terca", "quarta", "quinta", "sexta", "sabado", "domingo",
            "meta_ouro", "meta_prata", "meta_bronze",
        ]:
            df[c] = _to_numeric(df[c])
        df["vendedor"] = df["vendedor"].astype(str)
        return df
    finally:
        conn.close()


def publicar_dfs_na_session() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Conveniência: carrega e publica na session para outras páginas consumirem (ex.: Metas)."""
    df_e = carregar_df_entrada()
    df_m = carregar_df_metas()
    st.session_state["df_entrada"] = df_e
    st.session_state["df_metas"]   = df_m
    return df_e, df_m


# ============================ Renderizador (roteador local) ============================

def _resumo_df(df: pd.DataFrame, valor_col: str = "Valor") -> str:
    linhas = len(df)
    total = df[valor_col].sum() if valor_col in df.columns else None
    if total is None:
        return f"{linhas} linha(s)."
    return f"{linhas} linha(s) • Total: {_fmt_moeda(total)}"


def render():
    """
    Chamado pelo _call_page() do main.py.
    Decide a visão com base em st.session_state['pagina_atual'].
    """
    pagina = st.session_state.get("pagina_atual", "")

    if "Entradas" in pagina:
        df_e = carregar_df_entrada()
        page_entradas.render(df_e)     # delega para o módulo de Entradas
        return

    if "Saídas" in pagina:
        df_s = carregar_df_saidas()
        page_saidas.render(df_s)       # delega para o módulo de Saídas
        return

    if "Mercadorias" in pagina:
        
        st.info("Visão de Mercadorias ainda não implementada aqui. (Posso habilitar no próximo passo.)")
        return

    if "Fatura Cartão" in pagina:
        
        st.info("Visão de Fatura ainda não implementada aqui. (Posso habilitar no próximo passo.)")
        return

    if "Contas a Pagar" in pagina:
        
        st.info("Visão de Contas a Pagar ainda não implementada aqui. (Posso habilitar no próximo passo.)")
        return

    if "Empréstimos/Financiamentos" in pagina or "Empréstimos" in pagina:
        
        st.info("Visão de Empréstimos ainda não implementada aqui. (Posso habilitar no próximo passo.)")
        return

    st.info("Selecione uma opção no menu de DataFrames.")


# ============================ Retrocompatibilidade ============================
# Algumas páginas antigas importam `get_dataframe` deste módulo.
# Mantemos um shim aceitando vários apelidos para não quebrar nada.

def get_dataframe(name: Optional[str] = None) -> pd.DataFrame:
    """
    Compat: retorna um DataFrame conforme o nome pedido.
    Aceita variações/aliases sem acento e case-insensitive.
    """
    key = (name or "").strip().lower()

    # Entradas
    if key in {"entradas", "entrada", "df_entrada", "vendas", "lancamentos_entrada"}:
        return carregar_df_entrada()

    # Saídas
    if key in {"saidas", "saida", "df_saidas", "pagamentos_saida", "pagamentos"}:
        return carregar_df_saidas()

    # Mercadorias / Estoque
    if key in {"mercadorias", "estoque", "produtos", "compras", "itens_venda", "df_mercadorias"}:
        return carregar_df_mercadorias()

    # Metas
    if key in {"metas", "df_metas"}:
        return carregar_df_metas()

    # Desconhecido -> DF vazio
    return pd.DataFrame()
