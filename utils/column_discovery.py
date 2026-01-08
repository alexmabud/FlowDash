# utils/column_discovery.py
"""
Utilitário centralizado para descoberta e normalização de colunas.

Elimina duplicação de código em 15+ arquivos do FlowDash que implementam
funções idênticas ou muito similares para:
- Normalização de strings (upper, lower, acentos)
- Busca de colunas por lista de candidatos
- Inferência automática de colunas (data, valor, referência)
- Descoberta de colunas em banco de dados
- Normalização SQL

Arquivos impactados:
- finance_logic.py
- dre/dre.py
- dataframes/* (livro_caixa, contas_a_pagar, emprestimos, faturas_cartao)
- dashboard/dashboard.py
- cadastros/variaveis_dre.py
- lancamentos/shared_ui.py
- metas/metas.py
- services/taxas.py
- + outros 6 arquivos

Author: FlowDash Team + Claude Code
Created: 2026-01-08
"""

from __future__ import annotations

import sqlite3
import unicodedata
import re
from typing import Optional, Tuple, List
import pandas as pd


# ============================================================================
# TIER 1: NORMALIZAÇÃO BÁSICA
# ============================================================================

def normalize_string(
    s: str,
    mode: str = "upper",
) -> str:
    """
    Normaliza string para comparação.

    Args:
        s: String a normalizar
        mode:
            - "upper": MAIÚSCULA (padrão)
            - "lower": minúscula
            - "normalize_accents": remove acentos + lowercase

    Returns:
        String normalizada

    Examples:
        >>> normalize_string("  João  ")
        'JOÃO'
        >>> normalize_string("  João  ", mode="lower")
        'joão'
        >>> normalize_string("João García", mode="normalize_accents")
        'joao garcia'
    """
    s = (s or "").strip()

    if mode == "upper":
        return s.upper()
    elif mode == "lower":
        return s.lower()
    elif mode == "normalize_accents":
        # Remove acentos via unicodedata (melhor que replace manual)
        text = unicodedata.normalize("NFKD", s)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        return text.lower()

    return s


def normalize_bank_name(s: str) -> str:
    """
    Remove tudo exceto A-Z0-9 de nome de banco.

    Usado para normalização de nomes de bancos em comparações.

    Examples:
        >>> normalize_bank_name("Banco Inter S.A.")
        'BANCOINTER'
        >>> normalize_bank_name("Bradesco - Ag 1234")
        'BRADESCO1234'
    """
    return re.sub(r"[^A-Z0-9]", "", (s or "").upper())


# ============================================================================
# TIER 2: BUSCA EM LISTAS/DATAFRAMES
# ============================================================================

def find_column_in_list(
    cols: list[str],
    candidates: list[str],
    case_sensitive: bool = False,
) -> Optional[str]:
    """
    Busca primeira coluna candidata existente em lista de colunas.

    Retorna o NOME ORIGINAL (preserva maiúsculas/minúsculas).

    Args:
        cols: Lista de nomes reais de colunas
        candidates: Lista de candidatos a buscar (na ordem de preferência)
        case_sensitive: Se False, busca case-insensitive

    Returns:
        Nome original da coluna encontrada ou None

    Examples:
        >>> find_column_in_list(["Data", "Valor"], ["data", "dt"])
        'Data'
        >>> find_column_in_list(["fecha", "valor"], ["data", "dt"])
        None
    """
    if not case_sensitive:
        cols_lower = [c.lower() for c in cols]
        for cand in candidates:
            cand_lower = cand.lower()
            if cand_lower in cols_lower:
                idx = cols_lower.index(cand_lower)
                return cols[idx]  # Retorna NOME ORIGINAL
    else:
        for cand in candidates:
            if cand in cols:
                return cand

    return None


def find_column_in_dataframe(
    df: pd.DataFrame,
    candidates: list[str],
    case_sensitive: bool = False,
) -> Optional[str]:
    """
    Busca primeira coluna candidata em DataFrame.

    Retorna o NOME ORIGINAL (como está em df.columns).

    Args:
        df: DataFrame
        candidates: Lista de candidatos a buscar (na ordem de preferência)
        case_sensitive: Se False, case-insensitive

    Returns:
        Nome da coluna em df.columns ou None

    Examples:
        >>> import pandas as pd
        >>> df = pd.DataFrame({"Data": [1], "Valor": [2]})
        >>> find_column_in_dataframe(df, ["data", "dt"])
        'Data'
    """
    if not case_sensitive:
        cols_lower_map = {c.lower(): c for c in df.columns}
        for cand in candidates:
            # Primeiro tenta match exato (case-sensitive)
            if cand in df.columns:
                return cand
            # Depois tenta case-insensitive
            if cand.lower() in cols_lower_map:
                return cols_lower_map[cand.lower()]
    else:
        for cand in candidates:
            if cand in df.columns:
                return cand

    return None


# ============================================================================
# TIER 3: INFERÊNCIA DE COLUNAS (AUTOMÁTICA)
# ============================================================================

_DEFAULT_DATE_CANDIDATES = [
    "data", "Data", "data_venda", "Data_Venda",
    "dt", "DT", "data_lanc", "data_emissao",
    "created_at", "data_evento", "data_pagamento",
    "data_liq", "Data_Liq", "date", "Date"
]

_DEFAULT_VALUE_CANDIDATES = [
    "valor", "Valor", "valor_total", "Valor_Total",
    "valor_liquido", "Valor_Liquido", "valor_liq", "Valor_Liq",
    "amount", "Amount", "valor_bruto", "Valor_Bruto",
    "Valor_Mercadoria", "valor_mercadoria", "valor_recebido"
]


def infer_date_column(
    df: pd.DataFrame,
    candidates: Optional[list[str]] = None,
) -> Optional[str]:
    """
    Detecta coluna de data automaticamente.

    Procura por nomes comuns de colunas de data e verifica se são datetime.

    Args:
        df: DataFrame
        candidates: Lista customizada de candidatos (usa padrão se None)

    Returns:
        Nome da coluna de data ou None
    """
    if candidates is None:
        candidates = _DEFAULT_DATE_CANDIDATES

    col = find_column_in_dataframe(df, candidates)
    if col and pd.api.types.is_datetime64_any_dtype(df[col]):
        return col

    # Fallback: tenta converter para datetime
    for col in candidates:
        if col in df.columns:
            try:
                test = pd.to_datetime(df[col], errors="coerce")
                if test.notna().any():
                    return col
            except Exception:
                pass

    return None


def infer_value_column(
    df: pd.DataFrame,
    candidates: Optional[list[str]] = None,
) -> Optional[str]:
    """
    Detecta coluna de valor (numérica) automaticamente.

    Procura por nomes comuns de colunas monetárias e verifica se são numéricas.

    Args:
        df: DataFrame
        candidates: Lista customizada de candidatos (usa padrão se None)

    Returns:
        Nome da coluna de valor ou None
    """
    if candidates is None:
        candidates = _DEFAULT_VALUE_CANDIDATES

    col = find_column_in_dataframe(df, candidates)
    if col and pd.api.types.is_numeric_dtype(df[col]):
        return col

    # Fallback: procura coluna numérica
    for col in candidates:
        if col in df.columns:
            try:
                test = pd.to_numeric(df[col], errors="coerce")
                if test.notna().any():
                    return col
            except Exception:
                pass

    # Último recurso: primeira coluna numérica
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            return col

    return None


def infer_reference_column(
    df: pd.DataFrame,
    candidates: Optional[list[str]] = None,
) -> Optional[str]:
    """
    Detecta coluna de referência (tipo/origem/categoria).

    Busca por padrões: "referencia", "tipo", "origem", "categoria"

    Args:
        df: DataFrame
        candidates: Lista customizada de candidatos

    Returns:
        Nome da coluna de referência ou None
    """
    if candidates is None:
        candidates = ["referencia_tabela", "tipo", "tipo_mov", "referencia", "origem", "categoria"]

    return find_column_in_dataframe(df, candidates)


def infer_currency_columns(df: pd.DataFrame) -> list[str]:
    """
    Detecta automaticamente colunas monetárias baseado em nome.

    Procura por: valor, preço, principal, saldo, multa, desconto, pago, etc.

    Args:
        df: DataFrame

    Returns:
        Lista de nomes de colunas monetárias
    """
    out: list[str] = []
    keywords = ["valor", "preco", "preço", "principal", "saldo", "multa", "desconto", "pago"]

    for col in df.columns:
        col_lower = str(col).lower()
        if any(kw in col_lower for kw in keywords):
            try:
                s = pd.to_numeric(df[col], errors="coerce")
                if s.notna().any():
                    out.append(col)
            except Exception:
                pass

    # Remove duplicatas preservando ordem
    seen = set()
    return [c for c in out if not (c in seen or seen.add(c))]


def infer_percent_columns(df: pd.DataFrame) -> list[str]:
    """
    Detecta automaticamente colunas percentuais.

    Procura por: juros, taxa, percent, %

    Args:
        df: DataFrame

    Returns:
        Lista de nomes de colunas percentuais
    """
    out: list[str] = []
    keywords = ["juros", "taxa", "percent", "%"]

    for col in df.columns:
        col_lower = str(col).lower()
        if any(kw in col_lower for kw in keywords):
            try:
                s = pd.to_numeric(df[col], errors="coerce")
                if s.notna().any():
                    out.append(col)
            except Exception:
                pass

    seen = set()
    return [c for c in out if not (c in seen or seen.add(c))]


# ============================================================================
# TIER 4: DESCOBERTA EM BANCO DE DADOS
# ============================================================================

def get_table_columns_lower(
    conn: sqlite3.Connection,
    table: str,
) -> dict[str, str]:
    """
    Retorna {nome_lower: nome_original} das colunas da tabela.

    Útil para lookup case-insensitive em DB real.

    Args:
        conn: Conexão SQLite
        table: Nome da tabela

    Returns:
        Dicionário {nome_lowercase: nome_original}
    """
    try:
        info = conn.execute(f"PRAGMA table_info({table})").fetchall()
        cols = [r[1] for r in info]
        return {c.lower(): c for c in cols}
    except Exception:
        return {}


def find_column_in_table(
    conn: sqlite3.Connection,
    table: str,
    candidates: list[str],
) -> Optional[str]:
    """
    Busca coluna em tabela real do DB.

    Args:
        conn: Conexão SQLite
        table: Nome da tabela
        candidates: Lista de candidatos a buscar

    Returns:
        Nome ORIGINAL da coluna no DB ou None
    """
    cols_lower = get_table_columns_lower(conn, table)

    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]

    return None


def detect_table_by_candidates(
    conn: sqlite3.Connection,
    candidates: list[str],
) -> Optional[str]:
    """
    Detecta primeira tabela existente na lista de candidatos.

    Args:
        conn: Conexão SQLite
        candidates: Lista de nomes de tabela a buscar

    Returns:
        Nome da tabela ou None
    """
    for table in candidates:
        try:
            result = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,)
            ).fetchone()
            if result:
                return table
        except Exception:
            pass

    return None


def detect_table_columns(
    conn: sqlite3.Connection,
    table: str,
    date_candidates: Optional[list[str]] = None,
    value_candidates: Optional[list[str]] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Detecta colunas de data e valor em tabela real.

    Args:
        conn: Conexão SQLite
        table: Nome da tabela
        date_candidates: Candidatos para coluna de data
        value_candidates: Candidatos para coluna de valor

    Returns:
        Tuple[data_col, value_col] ou (None, None)
    """
    if date_candidates is None:
        date_candidates = _DEFAULT_DATE_CANDIDATES
    if value_candidates is None:
        value_candidates = _DEFAULT_VALUE_CANDIDATES

    cols_lower = get_table_columns_lower(conn, table)
    if not cols_lower:
        return None, None

    date_col = next(
        (cols_lower[c.lower()] for c in date_candidates if c.lower() in cols_lower),
        None
    )
    value_col = next(
        (cols_lower[c.lower()] for c in value_candidates if c.lower() in cols_lower),
        None
    )

    return date_col, value_col


# ============================================================================
# TIER 5: NORMALIZAÇÃO SQL
# ============================================================================

def sql_normalize_date_expression(
    date_col: str,
    input_format: str = "auto",
) -> str:
    """
    Gera SQL para normalizar datas em formato diverso.

    Detecta automaticamente:
    - DD/MM/YYYY -> ISO DATE
    - YYYY-MM-DD -> ISO DATE (passthrough)
    - YYYYMMDD -> ISO DATE

    Args:
        date_col: Nome da coluna de data
        input_format: "auto", "br" (DD/MM/YYYY), "iso", etc.

    Returns:
        Expressão SQL (CASE WHEN...)
    """
    if input_format == "auto" or input_format == "br":
        return f"""
            CASE
                WHEN instr("{date_col}", '/') > 0
                    THEN date(substr("{date_col}", 7, 4) || '-' || substr("{date_col}", 4, 2) || '-' || substr("{date_col}", 1, 2))
                ELSE date("{date_col}")
            END
        """.strip()

    # Fallback
    return f'date("{date_col}")'


def sql_normalize_numeric_expression(value_col: str) -> str:
    """
    Gera SQL para normalizar valores (casas decimais, separadores BR).

    Converte:
    - Strings com "." e "," para float (1.234,56 -> 1234.56)
    - Values já numéricos passam direto

    Args:
        value_col: Nome da coluna de valor

    Returns:
        Expressão SQL CASE WHEN
    """
    return f"""
        CASE
            WHEN typeof("{value_col}")='text'
                THEN CAST(REPLACE(REPLACE("{value_col}", '.', ''), ',', '.') AS REAL)
            ELSE "{value_col}"
        END
    """.strip()


# ============================================================================
# COMPATIBILITY EXPORTS (aliases para transição gradual)
# ============================================================================

# Aliases para manter compatibilidade com código antigo
_norm = normalize_string
_find_col = find_column_in_list
_first_existing = find_column_in_dataframe
_infer_valor_col = infer_value_column
_infer_ref_col = infer_reference_column


__all__ = [
    # Tier 1: Normalização
    "normalize_string",
    "normalize_bank_name",
    # Tier 2: Busca
    "find_column_in_list",
    "find_column_in_dataframe",
    # Tier 3: Inferência
    "infer_date_column",
    "infer_value_column",
    "infer_reference_column",
    "infer_currency_columns",
    "infer_percent_columns",
    # Tier 4: DB
    "get_table_columns_lower",
    "find_column_in_table",
    "detect_table_by_candidates",
    "detect_table_columns",
    # Tier 5: SQL
    "sql_normalize_date_expression",
    "sql_normalize_numeric_expression",
    # Compatibility
    "_norm",
    "_find_col",
    "_first_existing",
    "_infer_valor_col",
    "_infer_ref_col",
]
