# -*- coding: utf-8 -*-
# flowdash_pages/cadastros/variaveis_dre.py
from __future__ import annotations

import os
import sqlite3
from typing import Optional, Tuple, List, Callable, Dict, Any
import html as _html
from datetime import date, datetime

import pandas as pd
import streamlit as st

# ============== Descoberta de DB (segura) ==============
def _ensure_db_path_or_raise(pref: Optional[str] = None) -> str:
    if pref and isinstance(pref, str) and os.path.exists(pref):
        return pref
    try:
        for k in ("caminho_banco", "db_path"):
            v = st.session_state.get(k)
            if isinstance(v, str) and os.path.exists(v):
                return v
    except Exception:
        pass
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
        "./flowdash_data.db",
    ):
        if os.path.exists(p):
            return p
    raise FileNotFoundError("Nenhum banco encontrado. Defina st.session_state['db_path'].")

def _load_ui_prefs(db_path: Optional[str] = None) -> Dict[str, Any]:
    """Carrega preferências diretamente do banco (dre_variaveis)."""
    prefs: Dict[str, Any] = {}
    try:
        path = _ensure_db_path_or_raise(db_path)
        conn = _connect(path)
        _ensure_table(conn)
        rows = conn.execute(
            "SELECT chave, tipo, valor_num, valor_text FROM dre_variaveis"
        ).fetchall()
        for r in rows:
            chave = _canon_key(r["chave"])
            if not chave:
                continue
            tipo = (r["tipo"] or "").strip().lower()
            if tipo == "num":
                prefs[chave] = float(r["valor_num"] or 0.0)
            elif tipo == "bool":
                prefs[chave] = str(r["valor_text"]).strip().lower() in ("true", "1", "yes", "y", "sim")
            else:
                prefs[chave] = r["valor_text"]
    except Exception:
        pass
    return prefs

def _save_ui_prefs(prefs: Dict[str, Any], db_path: Optional[str] = None) -> None:
    """Persiste chaves permitidas diretamente em dre_variaveis (sem JSON)."""
    try:
        path = _ensure_db_path_or_raise(db_path)
        conn = _connect(path)
        _ensure_table(conn)
        for k, v in prefs.items():
            tipo = "text"
            num_val: Optional[float] = None
            text_val: Optional[str] = None
            if isinstance(v, (int, float)):
                tipo = "num"
                num_val = float(v)
            elif isinstance(v, date):
                tipo = "text"
                text_val = v.strftime("%Y-%m-%d")
            elif isinstance(v, bool):
                tipo = "bool"
                text_val = "true" if v else "false"
            elif v is None:
                tipo = "num"
                num_val = 0.0
            else:
                tipo = "text"
                text_val = str(v)
            _upsert_allowed(conn, k, tipo, num_val, text_val, "")
    except Exception:
        pass

def _persist_keys_to_json(keys: List[str], db_path: Optional[str] = None) -> None:
    """Mantida por compatibilidade: agora persiste no DB, não em JSON."""
    prefs = _load_ui_prefs(db_path)
    for k in keys:
        v = st.session_state.get(k)
        if isinstance(v, date):
            prefs[k] = v.strftime("%Y-%m-%d")
        else:
            prefs[k] = v
    _save_ui_prefs(prefs, db_path)

def _on_change_persist(*keys: str, db_path: Optional[str] = None) -> Callable[[], None]:
    """Factory de callback para st.number_input / st.date_input que persiste no DB ao mudar."""
    def _cb():
        _persist_keys_to_json(list(keys), db_path=db_path)
    return _cb

def _on_change_upsert_num(db_key: str, widget_key: str, descricao: str, db_path: Optional[str]):
    """Callback para gravar imediatamente um número no DB quando o widget muda."""
    def _cb():
        try:
            path = db_path or st.session_state.get("db_path")
            conn = _connect(_ensure_db_path_or_raise(path))
            _ensure_table(conn)
            v = _nonneg(st.session_state.get(widget_key, 0.0))
            _upsert_allowed(conn, db_key, "num", v, None, descricao)
        except Exception:
            pass
    return _cb

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
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.row_factory = sqlite3.Row
    return conn

def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(_SQL_CREATE)
    conn.commit()

# ===== normalização de chave =====
def _canon_key(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def _list(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql(
        "SELECT id, chave, tipo, valor_num, valor_text, descricao, updated_at "
        "FROM dre_variaveis ORDER BY chave COLLATE NOCASE",
        conn,
    )

def _upsert(conn: sqlite3.Connection, chave: str, tipo: str,
            valor_num: Optional[float], valor_text: Optional[str],
            descricao: str) -> None:
    k = _canon_key(chave)
    if not k or not tipo:
        return
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
            (k, "num", float(valor_num or 0.0), (descricao or "").strip()),
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
            (k, "text", (valor_text or ""), (descricao or "").strip()),
        )
    else:
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
            (k, "bool", v, (descricao or "").strip()),
        )
    conn.commit()

# ====== Variáveis que PODEM ser gravadas no DB (somente as 8 pedidas) ======
_ALLOWED_KEYS = {
    # calculados e bases do DRE
    "ativos_totais_base",
    "patrimonio_liquido_base",
    "investimento_total_base",
    "depreciacao_mensal_padrao",
    # entradas de cadastro
    "fundo_promocao_percent",
    "sacolas_percent",
    "markup_medio",
    "aliquota_simples_nacional",
    # imobilizado / depreciação
    "pl_imobilizado_valor_total",
    "dep_taxa_mensal_percent_live",
    # estoque base
    "dre_estoque_inicial_live",
    "dre_data_corte_live",
}

def _upsert_allowed(conn: sqlite3.Connection, chave: str, tipo: str,
                    valor_num: Optional[float], valor_text: Optional[str],
                    descricao: str) -> None:
    k = _canon_key(chave)
    if k in _ALLOWED_KEYS:
        _upsert(conn, k, tipo, valor_num, valor_text, descricao)

# --------- utils ---------
def _nonneg(v) -> float:
    try:
        return max(0.0, float(v or 0.0))
    except Exception:
        return 0.0

def _get_num(conn: sqlite3.Connection, chave: str, default: float) -> float:
    try:
        k = _canon_key(chave)
        row = conn.execute(
            "SELECT valor_num FROM dre_variaveis WHERE chave = ? LIMIT 1", (k,)
        ).fetchone()
        if row and row[0] is not None:
            return float(row[0])
    except Exception:
        pass
    return float(default)

def _get_text(conn: sqlite3.Connection, chave: str, default: str = "") -> str:
    try:
        k = _canon_key(chave)
        row = conn.execute(
            "SELECT valor_text FROM dre_variaveis WHERE chave = ? LIMIT 1", (k,)
        ).fetchone()
        if row and row[0] is not None:
            return str(row[0])
    except Exception:
        pass
    return default

def _fmt_brl(v: float) -> str:
    try:
        return f"R$ {float(v or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"

def _green_label(text: str) -> None:
    # Removido: não renderiza títulos verdes para permitir tooltip padrão nos widgets
    return

def _title_with_help(text: str, help_msg: str) -> None:
    import html as _html
    safe_help = _html.escape(help_msg or "")
    st.markdown(
        f'<span style="color:#ccc;font-weight:600">{_html.escape(text)}</span> '
        f'<span style="color:#888;cursor:help" title="{safe_help}">?</span>',
        unsafe_allow_html=True,
    )

def _green_title_with_help(text: str, help_msg: str) -> None:
    safe_help = _html.escape(help_msg or "")
    st.markdown(
        f'<span style="color:#2ecc71;font-weight:700">{_html.escape(text)}</span> '
        f'<span style="color:#888;cursor:help" title="{safe_help}">?</span>',
        unsafe_allow_html=True,
    )

# ===== Helpers de widgets (evitam warnings e mantêm estado) =====
def number_input_state(label: str, key: str, default: float, **kwargs) -> float:
    # Garante que o rótulo permaneça visível para exibir tooltip padrão
    if 'label_visibility' in kwargs:
        try:
            kwargs.pop('label_visibility', None)
        except Exception:
            pass
    if key in st.session_state:
        return st.number_input(label, key=key, **kwargs)
    else:
        return st.number_input(label, value=default, key=key, **kwargs)

def date_input_state(label: str, key: str, default: date, **kwargs) -> date:
    if key in st.session_state:
        return st.date_input(label, key=key, **kwargs)
    else:
        return st.date_input(label, value=default, key=key, **kwargs)

# ============== Pré-carregar session_state a partir do DB + JSON (uma vez) ==============
_UI_FORM_KEYS = {
    "dre_estoque_inicial_live": ("float", 0.0),
    "dre_data_corte_live": ("date", date.today()),
    "pl_imobilizado_valor_total": ("float", 0.0),
    "dep_taxa_mensal_percent_live": ("float", 0.0),
}

def _preload_session_from_sources(conn: sqlite3.Connection, db_path: Optional[str]) -> None:
    # Reidrata sempre dos arquivos e do DB a cada abertura
    ui_prefs = _load_ui_prefs(db_path)
    for k, (kind, default) in _UI_FORM_KEYS.items():
        try:
            if kind == "date":
                v = ui_prefs.get(k)
                if isinstance(v, str):
                    st.session_state[k] = datetime.strptime(v, "%Y-%m-%d").date()
                elif isinstance(v, (date, )):
                    st.session_state[k] = v
                else:
                    st.session_state[k] = default if isinstance(default, date) else date.today()
            else:
                v = ui_prefs.get(k, default)
                st.session_state[k] = float(v) if kind == "float" else v
        except Exception:
            st.session_state[k] = default

    mapping_db = {
        "investimento_total_base_live": ("investimento_total_base", "num"),
        "fundo_promocao_percent_live": ("fundo_promocao_percent", "num"),
        "sacolas_percent_live": ("sacolas_percent", "num"),
        "markup_medio_live": ("markup_medio", "num"),
        "aliquota_simples_nacional_live": ("aliquota_simples_nacional", "num"),
        "pl_imobilizado_valor_total": ("pl_imobilizado_valor_total", "num"),
        "dep_taxa_mensal_percent_live": ("dep_taxa_mensal_percent_live", "num"),
    }
    for key_widget, (db_key, tipo) in mapping_db.items():
        try:
            if tipo == "num":
                st.session_state[key_widget] = _nonneg(_get_num(conn, db_key, 0.0))
            else:
                st.session_state[key_widget] = _get_text(conn, db_key, "")
        except Exception:
            pass

# ============== Helpers de UI ==============
def _stack(renderers: List[Callable[[], None]]) -> None:
    for r in renderers:
        with st.container():
            r()

# ============== Helpers de domínio ==============
def _get_passivos_totais_cap(db_path: Optional[str]) -> Tuple[float, str]:
    try:
        from flowdash_pages.dataframes.contas_a_pagar import (  # type: ignore
            _load_loans_raw, _build_loans_view, _loans_totals,
            _cards_view, _cards_totals,
            _build_boletos_view, _boletos_totals_view, DB
        )
        hoje = date.today()
        db = DB(db_path or "data/flowdash_data.db")

        df_loans_raw = _load_loans_raw(db)
        df_loans_view = _build_loans_view(df_loans_raw) if not df_loans_raw.empty else pd.DataFrame()
        loans_sums = _loans_totals(df_loans_view)

        df_cards_view = _cards_view(db, hoje.year, hoje.month)
        cards_sums = _cards_totals(df_cards_view)

        df_boletos_view = _build_boletos_view(db, hoje.year, hoje.month)
        bols_sums = _boletos_totals_view(df_boletos_view)

        total_saldo = (
            float(loans_sums.get("saldo_total", 0.0)) +
            float(cards_sums.get("aberto_total", 0.0)) +
            float(bols_sums.get("saldo_total", 0.0))
        )
        return float(total_saldo), ""
    except Exception as e:
        return 0.0, f"Erro ao calcular passivos totais: {e}"

def _get_total_consolidado_bancos_caixa(conn: sqlite3.Connection, db_path: str) -> Tuple[float, str]:
    try:
        from flowdash_pages.fechamento.fechamento import _ultimo_caixas_ate, _somar_bancos_totais  # type: ignore
        hoje = date.today()
        disp_caixa, disp_caixa2, _ = _ultimo_caixas_ate(db_path, hoje)
        bancos_totais = _somar_bancos_totais(db_path, hoje) or {}
        total_bancos = float(sum(bancos_totais.values()))
        return float(disp_caixa + disp_caixa2 + total_bancos), ""
    except Exception as e:
        hint_1 = f"Tentativa por Fechamento falhou: {e}"
        try:
            candidates = [
                ("fechamento_caixa", ["saldo_esperado", "valor_informado", "total_consolidado", "consolidado_total", "total_geral", "saldo_total"]),
                ("fechamento_caixa_saldos", ["total_consolidado", "total_geral", "saldo_total"]),
                ("saldos_bancos", ["total", "saldo_total"]),
                ("saldos_caixas", ["total", "saldo_total"]),
            ]
            for tbl, cols in candidates:
                cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (tbl,))
                if not cur.fetchone():
                    continue
                for col in cols:
                    try:
                        row = conn.execute(
                            f"SELECT {col} FROM {tbl} ORDER BY DATE(data) DESC, ROWID DESC LIMIT 1;"
                        ).fetchone()
                        if row and row[0] is not None:
                            return float(row[0]), ""
                    except Exception:
                        continue
            return 0.0, "Não localizei 'total consolidado' nas tabelas de fechamento/saldos."
        except Exception as e2:
            return 0.0, f"{hint_1} | Fallback falhou: {e2}"

# === Estoque (data de corte) ===
_MERC_DATE_CANDS  = ["Data", "data", "data_compra", "Data_Compra", "dt", "DT"]
_MERC_VALUE_CANDS = [
    "Valor_Mercadoria", "Valor_Mercadorias", "valor_mercadoria", "valor_mercadorias",
    "valor_das_mercadorias", "Valor_das_mercadorias", "valor_total", "Valor_Total"
]

def _detect_merc_cols(conn: sqlite3.Connection) -> Tuple[str, str]:
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='mercadorias';").fetchone():
        raise RuntimeError("Tabela 'mercadorias' não encontrada.")
    info = conn.execute("PRAGMA table_info(mercadorias);").fetchall()
    cols_real = [r[1] for r in info]
    cols_lc = {c.lower(): c for c in cols_real}

    date_real  = next((cols_lc[c.lower()] for c in _MERC_DATE_CANDS  if c.lower() in cols_lc), None)
    value_real = next((cols_lc[c.lower()] for c in _MERC_VALUE_CANDS if c.lower() in cols_lc), None)
    if not date_real or not value_real:
        raise RuntimeError(f"Não consegui detectar colunas de Data/Valor em 'mercadorias'. Colunas: {cols_real}")
    return date_real, value_real

def _normalized_date_expr(date_col: str) -> str:
    return f"""
        CASE
            WHEN instr("{date_col}", '/') > 0
                THEN date(substr("{date_col}", 7, 4) || '-' || substr("{date_col}", 4, 2) || '-' || substr("{date_col}", 1, 2))
            ELSE date("{date_col}")
        END
    """.strip()

def _numeric_value_expr(value_col: str) -> str:
    return f"""
        CASE
            WHEN typeof("{value_col}")='text'
                THEN CAST(REPLACE(REPLACE("{value_col}", '.', ''), ',', '.') AS REAL)
            ELSE "{value_col}"
        END
    """.strip()

def _compute_estoque_base_corte(conn: sqlite3.Connection, data_corte_iso: str) -> Tuple[float, str]:
    if not data_corte_iso:
        return 0.0, "Defina a data de corte do estoque."
    try:
        date_col, value_col = _detect_merc_cols(conn)
        sql = f"""
            SELECT COALESCE(SUM({_numeric_value_expr(value_col)}), 0.0)
            FROM mercadorias
            WHERE {_normalized_date_expr(date_col)} >= date(?)
        """
        total = conn.execute(sql, (data_corte_iso,)).fetchone()[0]
        return float(total or 0.0), ""
    except Exception as e:
        return 0.0, f"Erro ao calcular estoque base na data de corte: {e}"

# === Vendas (para CMV estimado) ===
_SALES_TABLE_CANDS = ["entradas", "entrada", "vendas", "venda", "movimentacoes"]
_SALES_DATE_CANDS  = ["data", "Data", "dt", "DT", "data_venda", "Data_Venda"]
_SALES_VALUE_PREFS = [
    "valor_liquido", "Valor_Liquido", "valor_liq", "Valor_Liq",
    "valor_recebido", "Valor_Recebido", "valor", "Valor", "valor_total", "Valor_Total"
]

def _detect_sales_table_and_cols(conn: sqlite3.Connection) -> Tuple[str, str, str]:
    tbl = next((t for t in _SALES_TABLE_CANDS
                if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (t,)).fetchone()), None)
    if not tbl:
        raise RuntimeError("Tabela de vendas não encontrada (entradas/entrada/vendas).")

    info = conn.execute(f"PRAGMA table_info({tbl});").fetchall()
    cols_real = [r[1] for r in info]
    cols_lc = {c.lower(): c for c in cols_real}

    date_real  = next((cols_lc[c.lower()] for c in _SALES_DATE_CANDS  if c.lower() in cols_lc), None)
    value_real = next((cols_lc[c.lower()] for c in _SALES_VALUE_PREFS if c.lower() in cols_lc), None)
    if not date_real or not value_real:
        raise RuntimeError(f"Colunas necessárias não localizadas em '{tbl}'. Colunas: {cols_real}")
    return tbl, date_real, value_real

def _compute_receita_liquida_acum(conn: sqlite3.Connection, data_corte_iso: str) -> Tuple[float, str]:
    try:
        tbl, date_col, val_col = _detect_sales_table_and_cols(conn)
        sql = f"""
            SELECT COALESCE(SUM({_numeric_value_expr(val_col)}), 0.0)
            FROM {tbl}
            WHERE {_normalized_date_expr(date_col)} >= date(?)
        """
        total = conn.execute(sql, (data_corte_iso or "1900-01-01",)).fetchone()[0]
        return float(total or 0.0), ""
    except Exception as e:
        return 0.0, f"Erro ao calcular receita líquida acumulada: {e}"

# ============== API pública p/ DRE (estoque atual on-the-fly) ==============
def get_estoque_atual_estimado(db_path_pref: Optional[str] = None) -> float:
    """
    Estoque atual (estimado) = (Estoque inicial + Estoque base >= data corte)
                               − (Receita líquida acumulada ÷ Markup médio)
    """
    db_path = _ensure_db_path_or_raise(db_path_pref)
    conn = _connect(db_path)
    _ensure_table(conn)

    prefs = _load_ui_prefs(db_path)
    estoque_inicial = float(prefs.get("dre_estoque_inicial_live", 0.0) or 0.0)
    data_corte_iso  = prefs.get("dre_data_corte_live")
    if isinstance(data_corte_iso, date):
        data_corte_iso = data_corte_iso.strftime("%Y-%m-%d")
    if not data_corte_iso:
        try:
            date_col, _ = _detect_merc_cols(conn)
            row = conn.execute(
                f"SELECT MIN({_normalized_date_expr(date_col)}) FROM mercadorias"
            ).fetchone()
            data_corte_iso = (row[0] or "1900-01-01")
        except Exception:
            data_corte_iso = "1900-01-01"

    markup_medio = _get_num(conn, "markup_medio", 2.40) or 1.0

    try:
        estoque_base, _ = _compute_estoque_base_corte(conn, data_corte_iso)
    except Exception:
        estoque_base = 0.0

    try:
        receita_acum, _ = _compute_receita_liquida_acum(conn, data_corte_iso)
    except Exception:
        receita_acum = 0.0

    cmv_estimado = float(receita_acum or 0.0) / float(markup_medio)
    estoque_total = float(estoque_inicial or 0.0) + float(estoque_base or 0.0)
    return float((estoque_total or 0.0) - (cmv_estimado or 0.0))

# ============== UI ==============
def render(db_path_pref: Optional[str] = None):
    """Cadastros › Variáveis do DRE (só 8 variáveis no DB; demais via JSON p/ não zerar)."""
    db_path = _ensure_db_path_or_raise(db_path_pref)
    conn = _connect(db_path)
    _ensure_table(conn)

    _preload_session_from_sources(conn, db_path)
    # expõe o caminho do banco para callbacks
    try:
        st.session_state["db_path"] = db_path
    except Exception:
        pass

    st.markdown("### 🧩 Cadastros › Variáveis do DRE")

    # ===== Estoque (persistência via JSON)
    with st.container():
        st.subheader("Estoque")

        estoque_inicial_live = number_input_state(
            "Estoque inicial (R$)",
            key="dre_estoque_inicial_live",
            default=0.0,
            min_value=0.0, step=100.0, format="%.2f",
            help="Base manual do valor de estoque no início do período. Usada no cálculo do estoque atual estimado. Persistida em arquivo (não no banco).",
            on_change=_on_change_persist("dre_estoque_inicial_live", db_path=db_path),
        )

        data_corte_widget = date_input_state(
            "Data de corte do estoque",
            key="dre_data_corte_live",
            default=date.today(),
            format="DD/MM/YYYY",
            help="Data a partir da qual as compras (mercadorias) entram no 'Estoque base na data de corte'. Afeta CMV e estoque atual estimado. Persistida em arquivo.",
            on_change=_on_change_persist("dre_data_corte_live", db_path=db_path),
        )
        data_corte_iso_live = data_corte_widget.strftime("%Y-%m-%d")

        estoque_base_auto_live, hint_calc_live = _compute_estoque_base_corte(conn, data_corte_iso_live)
        estoque_total_live = float(estoque_inicial_live or 0.0) + float(estoque_base_auto_live or 0.0)
        markup_medio_live = _get_num(conn, "markup_medio", 2.40)
        receita_acum_live, hint_rec_live = _compute_receita_liquida_acum(conn, data_corte_iso_live)
        cmv_est_live = float(receita_acum_live or 0.0) / float(markup_medio_live or 1.0)
        estoque_atual_est_live = float(estoque_total_live or 0.0) - float(cmv_est_live or 0.0)

        _stack([
            lambda: st.text_input(
                "Estoque base na data de corte (R$)",
                _fmt_brl(estoque_base_auto_live),
                disabled=True,
                key="vi_txt_estoque_base_corte",
                help="Soma das compras de mercadoria a partir da Data de corte. Calculado automaticamente a partir da tabela 'mercadorias'."
            ),
            lambda: st.text_input(
                "Total de estoque (R$) — inicial + base",
                _fmt_brl(estoque_total_live),
                disabled=True,
                key="vi_txt_total_estoque",
                help="Soma do Estoque inicial com o Estoque base na data de corte."
            ),
            lambda: st.text_input(
                "Receita líquida acumulada (desde a data de corte)",
                _fmt_brl(receita_acum_live),
                disabled=True,
                key="vi_txt_receita_acum",
                help="Vendas líquidas acumuladas desde a Data de corte, usadas para estimar CMV via Markup médio."
            ),
            lambda: st.text_input(
                "CMV acumulado (estimado)",
                _fmt_brl(cmv_est_live),
                disabled=True,
                key="vi_txt_cmv_acum",
                help="Estimativa do Custo das Mercadorias Vendidas = Receita líquida acumulada ÷ Markup médio."
            ),
            lambda: st.text_input(
                "Estoque atual (estimado)",
                _fmt_brl(estoque_atual_est_live),
                disabled=True,
                key="vi_txt_estoque_atual_1",
                help="Estoque atual aproximado = (Estoque inicial + Estoque base) − CMV estimado."
            ),
        ])

        if hint_calc_live:
            st.caption(f"ℹ️ {hint_calc_live}")
        if hint_rec_live:
            st.caption(f"ℹ️ {hint_rec_live}")

    st.divider()

    # ===== Patrimônio / Investimento
    with st.container():
        st.subheader("Indicadores para Patrimônio Líquido / ROE / ROI / ROA")

        bancos_total_preview, hint_bancos = _get_total_consolidado_bancos_caixa(conn, db_path)
        if hint_bancos:
            st.caption(f"ℹ️ Fechamento: {hint_bancos}")

        try:
            estoque_atual_est_calc = estoque_atual_est_live
        except Exception:
            estoque_atual_est_calc = 0.0

        passivos_totais_preview, hint_cap = _get_passivos_totais_cap(db_path)
        if hint_cap:
            st.caption(f"ℹ️ CAP: {hint_cap}")

        # Imobilizado (persistência via JSON)
        imobilizado_valor_input = number_input_state(
            "Valor total dos bens (R$) – Imobilizado",
            key="pl_imobilizado_valor_total",
            default=0.0,
            min_value=0.0, step=100.0, format="%.2f",
            help="Somatório do imobilizado (fachada, mobiliário, TI, etc.). Entra nos Ativos Totais. Persistido em arquivo (não no banco).",
            on_change=_on_change_upsert_num("pl_imobilizado_valor_total", "pl_imobilizado_valor_total", "Valor total dos bens (R$) – Imobilizado", db_path),
        )
        _upsert_allowed(conn, "pl_imobilizado_valor_total", "num", imobilizado_valor_input, None, "Valor total dos bens (R$) – Imobilizado")
        _persist_keys_to_json(["pl_imobilizado_valor_total"], db_path)

        # Cálculos (ativos / PL)
        ativos_totais_preview = float(bancos_total_preview or 0.0) + float(estoque_atual_est_calc or 0.0) + float(imobilizado_valor_input or 0.0)
        pl_preview = float(ativos_totais_preview) - float(passivos_totais_preview or 0.0)

        # Persistências no DB (somente as 8)
        _upsert_allowed(conn, "ativos_totais_base", "num", ativos_totais_preview, None, "Ativos Totais (calc.) — usado no DRE")
        _upsert_allowed(conn, "patrimonio_liquido_base", "num", pl_preview, None, "Patrimônio Líquido (calc.) — usado no DRE")

        def _ativos_calc_green():
            st.text_input(
                "Ativos Totais - Utilizado no DRE",
                value=_fmt_brl(ativos_totais_preview),
                disabled=True,
                key="vi_txt_ativos_totais_calc_green",
                help="Soma de Bancos+Caixas + Estoque atual (estimado) + Imobilizado. Persistido no DB como 'ativos_totais_base'."
            )

        def _pl_calc_green():
            st.text_input(
                "Patrimônio Líquido - Utilizado no DRE",
                value=_fmt_brl(pl_preview),
                disabled=True,
                key="vi_txt_pl_calc_persist",
                help="Ativos Totais − Passivos Totais (CAP). Persistido no DB como 'patrimonio_liquido_base'."
            )

        # Investimento Base (DB)
        investimento_default = _nonneg(_get_num(conn, "investimento_total_base", 0.0))
        def _invest_input():
            val = number_input_state(
                "Investimento Total Base (R$) - Utilizado no DRE",
                key="investimento_total_base_live",
                default=investimento_default,
                min_value=0.0, step=100.0, format="%.2f",
                help="Aportes/reformas/capital investido acumulado. Usado para ROI. Persistido no DB como 'investimento_total_base'.",
                on_change=_on_change_upsert_num("investimento_total_base", "investimento_total_base_live", "Investimento Total Base (R$)", db_path),
            )
            _upsert_allowed(conn, "investimento_total_base", "num", val, None, "Investimento Total Base (R$)")

        _stack([
            lambda: st.text_input(
                "Bancos + Caixa (Total consolidado)",
                _fmt_brl(bancos_total_preview),
                disabled=True,
                key="vi_txt_bancos_caixa_total",
                help="Total disponível somando saldos dos bancos e dos caixas. Vem do módulo Fechamento."
            ),
            lambda: st.text_input(
                "Passivos Totais (CAP)",
                _fmt_brl(passivos_totais_preview),
                disabled=True,
                key="vi_txt_passivos_totais",
                help="Dívidas consolidadas (empréstimos, cartões a pagar, boletos). Vem do módulo Contas a Pagar."
            ),
            _ativos_calc_green,
            _pl_calc_green,
            _invest_input,
        ])

    st.divider()

    # ===== Depreciação
    with st.container():
        st.subheader("Depreciação")

        st.text_input(
            "Valor total dos bens (R$)",
            value=_fmt_brl(st.session_state.get("pl_imobilizado_valor_total", 0.0)),
            disabled=True,
            key="vi_txt_dep_valor_bens",
            help="Espelha o valor informado em Imobilizado. Base para estimar a depreciação mensal padrão."
        )

        taxa_dep = number_input_state(
            "Taxa mensal (%)",
            key="dep_taxa_mensal_percent_live",
            default=0.0,
            min_value=0.0, step=0.10, format="%.2f",
            help="Percentual mensal estimado para depreciação do imobilizado. Persistido em arquivo (não no DB).",
            on_change=_on_change_upsert_num("dep_taxa_mensal_percent_live", "dep_taxa_mensal_percent_live", "Taxa mensal (%)", db_path),
        )
        _upsert_allowed(conn, "dep_taxa_mensal_percent_live", "num", taxa_dep, None, "Taxa mensal (%)")
        _persist_keys_to_json(["dep_taxa_mensal_percent_live"], db_path)

        imobilizado_valor = float(st.session_state.get("pl_imobilizado_valor_total", 0.0) or 0.0)
        estimativa = float(imobilizado_valor * ((taxa_dep or 0.0) / 100.0))

        _upsert_allowed(conn, "depreciacao_mensal_padrao", "num", estimativa, None, "Depreciação mensal p/ EBITDA (R$)")

        _green_label("Depreciação mensal padrão (R$/mês) - Utilizado no DRE")
        st.text_input(
            "Depreciação mensal padrão (R$/mês) - Utilizado no DRE",
            value=_fmt_brl(estimativa),
            disabled=True,
            key="vi_txt_dep_estimativa",
            help="Valor em R$/mês usado no DRE para EBIT/EBITDA. Persistido no DB como 'depreciacao_mensal_padrao'."
        )

    st.divider()

    # ===== Parâmetros Básicos (todas no DB)
    with st.container():
        st.subheader("Parâmetros Básicos")

        _green_label("Fundo de promoção (%) - Utilizado no DRE")
        fundo_default = _nonneg(_get_num(conn, "fundo_promocao_percent", 1.00))
        fundo = number_input_state(
            "Fundo de promoção (%) - Utilizado no DRE",
            key="fundo_promocao_percent_live",
            default=fundo_default,
            min_value=0.0, step=0.01, format="%.2f",
            label_visibility="collapsed",
            help="Percentual do faturamento destinado a fundo de promoção. Usado no DRE e persistido no DB.",
            on_change=_on_change_upsert_num("fundo_promocao_percent", "fundo_promocao_percent_live", "Fundo de promoção (%)", db_path)
        )
        _upsert_allowed(conn, "fundo_promocao_percent", "num", fundo, None, "Fundo de promoção (%)")

        _green_label("Sacolas (%) - Utilizado no DRE")
        sacolas_default = _nonneg(_get_num(conn, "sacolas_percent", 1.20))
        sacolas = number_input_state(
            "Sacolas (%) - Utilizado no DRE",
            key="sacolas_percent_live",
            default=sacolas_default,
            min_value=0.0, step=0.01, format="%.2f",
            label_visibility="collapsed",
            help="Percentual médio gasto com sacolas sobre a receita. Usado no DRE e persistido no DB.",
            on_change=_on_change_upsert_num("sacolas_percent", "sacolas_percent_live", "Custo de sacolas (%)", db_path)
        )
        _upsert_allowed(conn, "sacolas_percent", "num", sacolas, None, "Custo de sacolas (%)")

        _green_label("Markup médio)")
        markup_default = _nonneg(_get_num(conn, "markup_medio", 2.40))
        markup = number_input_state(
            "Markup médio - Utilizado no DRE",
            key="markup_medio_live",
            default=markup_default,
            min_value=0.0, step=0.1, format="%.2f",
            label_visibility="collapsed",
            help="Coeficiente médio de precificação (Preço/CMV). Usado para estimar CMV quando não há custo unitário. Persistido no DB.",
            on_change=_on_change_upsert_num("markup_medio", "markup_medio_live", "Markup médio (coeficiente)", db_path)
        )
        _upsert_allowed(conn, "markup_medio", "num", markup, None, "Markup médio (coeficiente)")

        _green_label("Simples Nacional (%) - Utilizado no DRE")
        simples_default = _nonneg(_get_num(conn, "aliquota_simples_nacional", 4.32))
        simples = number_input_state(
            "Simples Nacional (%) - Utilizado no DRE",
            key="aliquota_simples_nacional_live",
            default=simples_default,
            min_value=0.0, step=0.01, format="%.2f",
            label_visibility="collapsed",
            help="Alíquota efetiva média de tributos no Simples sobre a receita. Usado no DRE e persistido no DB.",
            on_change=_on_change_upsert_num("aliquota_simples_nacional", "aliquota_simples_nacional_live", "Alíquota Simples Nacional (%)", db_path)
        )
        _upsert_allowed(conn, "aliquota_simples_nacional", "num", simples, None, "Alíquota Simples Nacional (%)")

    # ===== Salvaguarda final (idempotente) — mantém compatibilidade com fluxos antigos
    prefs = _load_ui_prefs(db_path)
    prefs.update({
        "dre_estoque_inicial_live": float(st.session_state.get("dre_estoque_inicial_live", 0.0)),
        "dre_data_corte_live": st.session_state.get("dre_data_corte_live", date.today()).strftime("%Y-%m-%d"),
        "pl_imobilizado_valor_total": float(st.session_state.get("pl_imobilizado_valor_total", 0.0)),
        "dep_taxa_mensal_percent_live": float(st.session_state.get("dep_taxa_mensal_percent_live", 0.0)),
    })
    _save_ui_prefs(prefs, db_path)

    # ===== Tabela de variáveis (debug)
    st.subheader("Tabela de variáveis (debug)")
    df = _list(conn)
    if not df.empty:
        df["valor"] = df.apply(
            lambda r: f"{r['valor_num']:.2f}" if r["tipo"] == "num" and r["valor_num"] is not None else (r["valor_text"] or ""),
            axis=1,
        )
        st.dataframe(
            df[["id", "chave", "tipo", "valor", "descricao", "updated_at"]],
            use_container_width=True, hide_index=True
        )
    else:
        st.info("Nenhum registro em dre_variaveis ainda.")
