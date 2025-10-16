# -*- coding: utf-8 -*-
# flowdash_pages/cadastros/variaveis_dre.py
from __future__ import annotations

import os
import sqlite3
from typing import Optional

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

def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn

def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(_SQL_CREATE)
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

# ============== UI ==============
def render(db_path_pref: Optional[str] = None):
    """Cadastros » Variáveis do DRE (parâmetros manuais e KPIs avançados)."""
    db_path = _ensure_db_path_or_raise(db_path_pref)
    conn = _connect(db_path)
    _ensure_table(conn)

    st.markdown("### 🧮 Cadastros › Variáveis do DRE")

    with st.form("form_var_dre"):
        # ----------------- Bloco atual (mantido) -----------------
        st.subheader("Parâmetros Básicos")
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

        st.markdown("---")

        # ----------------- KPIs avançados -----------------
        st.subheader("KPIs Avançados (ROE/ROI/ROA/EBITDA)")

        st.caption("Base Patrimonial / Investimento — usados em ROE, ROI e ROA")
        col3, col4, col5 = st.columns(3)
        with col3:
            pl = st.number_input(
                "Patrimônio Líquido (R$)",
                min_value=0.0, step=100.0,
                value=_get_num(conn, "patrimonio_liquido_base", 0.0),
                format="%.2f",
                help="ROE = Lucro Líquido / Patrimônio Líquido"
            )
        with col4:
            inv = st.number_input(
                "Investimento Total (R$)",
                min_value=0.0, step=100.0,
                value=_get_num(conn, "investimento_total_base", 0.0),
                format="%.2f",
                help="ROI = Lucro Líquido / Investimento Total"
            )
        with col5:
            atv = st.number_input(
                "Ativos Totais (R$)",
                min_value=0.0, step=100.0,
                value=_get_num(conn, "ativos_totais_base", 0.0),
                format="%.2f",
                help="ROA = Lucro Líquido / Ativos Totais"
            )

        st.caption("Ajustes para EBITDA — como não estão nas Saídas, informe valores mensais")
        col6, col7 = st.columns(2)
        with col6:
            dep = st.number_input(
                "Depreciação Mensal (R$)",
                min_value=0.0, step=50.0,
                value=_get_num(conn, "depreciacao_mensal_padrao", 0.0),
                format="%.2f",
                help="Somada ao EBITDA se não vier das Saídas"
            )
        with col7:
            amo = st.number_input(
                "Amortização Mensal (R$)",
                min_value=0.0, step=50.0,
                value=_get_num(conn, "amortizacao_mensal_padrao", 0.0),
                format="%.2f",
                help="Somada ao EBITDA se não vier das Saídas"
            )

        # ----------------- Salvar -----------------
        if st.form_submit_button("Salvar"):
            try:
                # Básicos
                _upsert(conn, "aliquota_simples_nacional", "num", simples, None, "Alíquota Simples Nacional (%)")
                _upsert(conn, "markup_medio", "num", markup, None, "Markup médio (coeficiente)")
                _upsert(conn, "sacolas_percent", "num", sacolas, None, "Custo de sacolas sobre faturamento (%)")
                _upsert(conn, "fundo_promocao_percent", "num", fundo, None, "Fundo de promoção (%)")

                # Avançados (sem override de vendas)
                _upsert(conn, "patrimonio_liquido_base", "num", pl, None, "Base para ROE (R$)")
                _upsert(conn, "investimento_total_base", "num", inv, None, "Base para ROI (R$)")
                _upsert(conn, "ativos_totais_base", "num", atv, None, "Base para ROA (R$)")
                _upsert(conn, "depreciacao_mensal_padrao", "num", dep, None, "Depreciação mensal p/ EBITDA (R$)")
                _upsert(conn, "amortizacao_mensal_padrao", "num", amo, None, "Amortização mensal p/ EBITDA (R$)")

                st.success("Variáveis salvas.")
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao salvar: {e}")

    st.divider()

    df = _list(conn)
    if not df.empty:
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
        df2 = _list(conn)
        if not df2.empty:
            options = [(int(r["id"]), f"ID {int(r['id'])} — {r['chave']}") for _, r in df2.iterrows()]
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
