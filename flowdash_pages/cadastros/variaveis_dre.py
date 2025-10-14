# -*- coding: utf-8 -*-
# flowdash_pages/cadastros/variaveis_dre.py
from __future__ import annotations

import os
import sqlite3
from typing import Optional, List, Tuple

import pandas as pd
import streamlit as st

# ============== Descoberta de DB (segura) ==============
def _ensure_db_path_or_raise(pref: Optional[str] = None) -> str:
    if pref and isinstance(pref, str) and os.path.exists(pref):
        return pref
    for k in ("caminho_banco", "db_path"):
        v = st.session_state.get(k)
        if isinstance(v, str) and os.path.exists(v):
            return v
    try:
        from shared.db import get_db_path as _shared_get_db_path  # type: ignore
        p = _shared_get_db_path()
        if isinstance(p, str) and os.path.exists(p):
            return p
    except Exception:
        pass
    for p in (
        os.path.join("data", "flowdash_data.db"),
        os.path.join("data", "dashboard_rc.db"),
        "dashboard_rc.db",
        os.path.join("data", "flowdash_template.db"),
    ):
        if os.path.exists(p):
            return p
    raise FileNotFoundError("Nenhum banco encontrado. Defina st.session_state['db_path'].")

# ============== Infra DB ==============
_SQL_CREATE = """
CREATE TABLE IF NOT EXISTS dre_variaveis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chave TEXT NOT NULL UNIQUE,
    tipo  TEXT NOT NULL CHECK (tipo IN ('num','text','bool')),
    valor_num  REAL,
    valor_text TEXT,
    descricao  TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

# Seeds oficiais (4 variáveis manuais)
_SEED: List[Tuple[str, str, Optional[float], Optional[str], str]] = [
    ("aliquota_simples_nacional", "num", 4.32, None, "Alíquota Simples Nacional (%)"),
    ("markup_medio",              "num", 2.40, None, "Markup médio (coeficiente)"),
    ("sacolas_percent",           "num", 1.20, None, "Custo de sacolas sobre faturamento (%)"),
    ("fundo_promocao_percent",    "num", 1.00, None, "Fundo de promoção (%)"),
]

def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn

def _bootstrap(conn: sqlite3.Connection) -> None:
    """Cria a tabela e garante as 4 chaves (sem limpar extras)."""
    conn.execute(_SQL_CREATE)
    for chave, tipo, vnum, vtxt, desc in _SEED:
        if tipo == "num":
            conn.execute(
                """
                INSERT INTO dre_variaveis (chave, tipo, valor_num, descricao)
                VALUES (?,?,?,?)
                ON CONFLICT(chave) DO UPDATE SET
                    tipo=excluded.tipo,
                    valor_num=excluded.valor_num,
                    descricao=excluded.descricao,
                    updated_at=datetime('now')
                """,
                (chave, tipo, float(vnum or 0.0), desc),
            )
        else:
            conn.execute(
                """
                INSERT INTO dre_variaveis (chave, tipo, valor_text, descricao)
                VALUES (?,?,?,?)
                ON CONFLICT(chave) DO UPDATE SET
                    tipo=excluded.tipo,
                    valor_text=excluded.valor_text,
                    descricao=excluded.descricao,
                    updated_at=datetime('now')
                """,
                (chave, tipo, (vtxt or ""), desc),
            )
    conn.commit()

def _list(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql(
        "SELECT id, chave, tipo, valor_num, valor_text, descricao, updated_at "
        "FROM dre_variaveis ORDER BY chave",
        conn,
    )

def _upsert(conn: sqlite3.Connection, chave: str, tipo: str,
            valor_num: Optional[float], valor_text: Optional[str], descricao: str):
    if tipo == "num":
        conn.execute(
            """
            INSERT INTO dre_variaveis (chave, tipo, valor_num, descricao)
            VALUES (?,?,?,?)
            ON CONFLICT(chave) DO UPDATE SET
                tipo=excluded.tipo,
                valor_num=excluded.valor_num,
                descricao=excluded.descricao,
                updated_at=datetime('now')
            """,
            (chave.strip(), "num", float(valor_num or 0.0), descricao.strip()),
        )
    elif tipo == "text":
        conn.execute(
            """
            INSERT INTO dre_variaveis (chave, tipo, valor_text, descricao)
            VALUES (?,?,?,?)
            ON CONFLICT(chave) DO UPDATE SET
                tipo=excluded.tipo,
                valor_text=excluded.valor_text,
                descricao=excluded.descricao,
                updated_at=datetime('now')
            """,
            (chave.strip(), "text", (valor_text or ""), descricao.strip()),
        )
    else:  # bool
        v = (valor_text or "false").strip().lower()
        v = "true" if v in ("true", "1", "yes", "y", "sim") else "false"
        conn.execute(
            """
            INSERT INTO dre_variaveis (chave, tipo, valor_text, descricao)
            VALUES (?,?,?,?)
            ON CONFLICT(chave) DO UPDATE SET
                tipo=excluded.tipo,
                valor_text=excluded.valor_text,
                descricao=excluded.descricao,
                updated_at=datetime('now')
            """,
            (chave.strip(), "bool", v, descricao.strip()),
        )
    conn.commit()

def _delete(conn: sqlite3.Connection, var_id: int):
    conn.execute("DELETE FROM dre_variaveis WHERE id = ?", (var_id,))
    conn.commit()

# ============== Garantia de schema no boot (p/ usar via main.py) ==============
def ensure_schema_dre_variaveis(db_path_pref: Optional[str] = None, prune_others: bool = False) -> None:
    """
    Garante a existência da tabela 'dre_variaveis' e das 4 chaves oficiais.
    Pode ser chamada no boot do app (main.py), funciona em qualquer banco.
    Se prune_others=True, remove chaves antigas que não estão no seed.
    """
    db_path = _ensure_db_path_or_raise(db_path_pref)
    conn = _connect(db_path)
    try:
        conn.execute(_SQL_CREATE)
        if prune_others:
            conn.execute("""
                DELETE FROM dre_variaveis
                 WHERE chave NOT IN (
                   'aliquota_simples_nacional',
                   'markup_medio',
                   'sacolas_percent',
                   'fundo_promocao_percent'
                 );
            """)
        # UPSERT das 4 oficiais
        for chave, tipo, vnum, vtxt, desc in _SEED:
            if tipo == "num":
                conn.execute(
                    """
                    INSERT INTO dre_variaveis (chave, tipo, valor_num, descricao)
                    VALUES (?,?,?,?)
                    ON CONFLICT(chave) DO UPDATE SET
                        tipo=excluded.tipo,
                        valor_num=excluded.valor_num,
                        descricao=excluded.descricao,
                        updated_at=datetime('now')
                    """,
                    (chave, tipo, float(vnum or 0.0), desc),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO dre_variaveis (chave, tipo, valor_text, descricao)
                    VALUES (?,?,?,?)
                    ON CONFLICT(chave) DO UPDATE SET
                        tipo=excluded.tipo,
                        valor_text=excluded.valor_text,
                        descricao=excluded.descricao,
                        updated_at=datetime('now')
                    """,
                    (chave, tipo, (vtxt or ""), desc),
                )
        conn.commit()
    finally:
        conn.close()

# ============== UI ==============
def render(db_path_pref: Optional[str] = None):
    """Cadastros » Variáveis do DRE (4 parâmetros manuais)."""
    db_path = _ensure_db_path_or_raise(db_path_pref)
    conn = _connect(db_path)
    _bootstrap(conn)

    st.markdown("### 🧮 Cadastros › Variáveis do DRE")

    with st.form("form_var_dre"):
        col1, col2 = st.columns(2)

        with col1:
            simples = st.number_input(
                "Simples Nacional (%)",
                min_value=0.0, step=0.01,
                value=_get_num(conn, "aliquota_simples_nacional", 4.32),
                format="%.2f"
            )
            sacolas = st.number_input(
                "Sacolas (%)",
                min_value=0.0, step=0.01,
                value=_get_num(conn, "sacolas_percent", 1.20),
                format="%.2f"
            )
        with col2:
            markup = st.number_input(
                "Markup médio (coeficiente)",
                min_value=0.0, step=0.1,
                value=_get_num(conn, "markup_medio", 2.40),
                format="%.2f"
            )
            fundo  = st.number_input(
                "Fundo de promoção (%)",
                min_value=0.0, step=0.01,
                value=_get_num(conn, "fundo_promocao_percent", 1.00),
                format="%.2f"
            )

        if st.form_submit_button("Salvar"):
            try:
                _upsert(conn, "aliquota_simples_nacional", "num", simples, None, "Alíquota Simples Nacional (%)")
                _upsert(conn, "markup_medio", "num", markup, None, "Markup médio (coeficiente)")
                _upsert(conn, "sacolas_percent", "num", sacolas, None, "Custo de sacolas sobre faturamento (%)")
                _upsert(conn, "fundo_promocao_percent", "num", fundo, None, "Fundo de promoção (%)")
                st.success("Variáveis salvas.")
            except Exception as e:
                st.error(f"Erro ao salvar: {e}")

    st.divider()

    df = _list(conn)
    if not df.empty:
        # Tabela amigável (agora com ID)
        def _fmt(row):
            if row["tipo"] == "num" and row["valor_num"] is not None:
                return f"{row['valor_num']:.4f}".rstrip("0").rstrip(".")
            return row.get("valor_text") or ""
        view = df.copy()
        view["valor"] = view.apply(_fmt, axis=1)
        view = view[["id", "chave", "tipo", "valor", "descricao", "updated_at"]]
        st.dataframe(view, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum registro em dre_variaveis ainda.")

    with st.expander("Excluir variável (cuidado)"):
        if not df.empty:
            # opções rotuladas "ID — chave"
            options = [(int(r["id"]), f"ID {int(r['id'])} — {r['chave']}") for _, r in df.iterrows()]
            selected = st.selectbox(
                "Selecione para excluir",
                options=options,
                format_func=lambda x: x[1] if isinstance(x, tuple) else str(x)
            )
            if st.button("Excluir selecionada", type="secondary", disabled=not options):
                try:
                    selected_id = int(selected[0]) if isinstance(selected, tuple) else int(selected)
                    _delete(conn, selected_id)
                    st.success("Excluída. Reabra a página para atualizar.")
                except Exception as e:
                    st.error(f"Erro ao excluir: {e}")
        else:
            st.info("Não há variáveis para excluir.")

# Helpers locais
def _get_num(conn: sqlite3.Connection, chave: str, default: float) -> float:
    try:
        cur = conn.execute("SELECT valor_num FROM dre_variaveis WHERE chave = ? LIMIT 1", (chave,))
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0])
    except Exception:
        pass
    return float(default)
