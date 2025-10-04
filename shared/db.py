# -*- coding: utf-8 -*-
"""
shared.db
=========

Camada utilitária para **resolver o caminho do banco** sem acessar
`st.session_state` antes do runtime do Streamlit iniciar.

- Usa `shared.safe_session` (exists/get/setdefault) para ler/escrever com segurança.
- Evita QUALQUER acesso a Streamlit no escopo de módulo (apenas dentro de funções).
- Mantém compatibilidade com chaves já usadas: "caminho_banco" e "db_path".

Ordem de resolução (primeiro que existir):
1) Valor passado por parâmetro (preferencial).
2) Session State (chaves: "caminho_banco", "db_path") — apenas se runtime existir.
3) Variáveis de ambiente: FLOWDASH_DB_PATH, DB_PATH.
4) Caminhos padrão do projeto.
"""

from __future__ import annotations

import os
import sqlite3
from typing import Optional, Iterable

# Acesso seguro ao session_state
try:
    from shared.safe_session import exists as _ss_exists, get as _ss_get, setdefault as _ss_setdefault
except Exception:
    def _ss_exists() -> bool:  # type: ignore
        return False
    def _ss_get(key: str, default=None):  # type: ignore
        return default
    def _ss_setdefault(key: str, default):  # type: ignore
        return default

# ---------- descoberta do caminho ----------

def _first_existing(paths: Iterable[str]) -> Optional[str]:
    for p in paths:
        if isinstance(p, str) and p and os.path.exists(p):
            return p
    return None

def _session_candidates() -> list[str]:
    if not _ss_exists():
        return []
    cands: list[str] = []
    for k in ("caminho_banco", "db_path"):
        v = _ss_get(k)
        if isinstance(v, str) and v:
            cands.append(v)
    return cands

def _env_candidates() -> list[str]:
    cands: list[str] = []
    for k in ("FLOWDASH_DB_PATH", "DB_PATH"):
        v = os.getenv(k)
        if isinstance(v, str) and v:
            cands.append(v)
    return cands

def _default_candidates() -> list[str]:
    return [
        "data/flowdash_data.db",
        "data/flowdash_template.db",
        "./flowdash_data.db",
    ]

def get_db_path(prefer: Optional[str] = None) -> Optional[str]:
    cands: list[str] = []
    if isinstance(prefer, str) and prefer:
        cands.append(prefer)
    cands.extend(_session_candidates())
    cands.extend(_env_candidates())
    cands.extend(_default_candidates())
    return _first_existing(cands)

def set_db_path_in_session(path: str) -> str:
    if _ss_exists() and isinstance(path, str) and path:
        _ss_setdefault("caminho_banco", path)
    return path

def ensure_db_path_or_raise(prefer: Optional[str] = None) -> str:
    p = get_db_path(prefer)
    if not p:
        raise FileNotFoundError(
            "FlowDash: nenhum banco encontrado. "
            "Defina o caminho em 'caminho_banco' (session_state), "
            "variável de ambiente FLOWDASH_DB_PATH ou coloque o arquivo em data/."
        )
    return p

# ---------- conexão SQLite pronta para produção ----------

def get_conn(prefer: Optional[str] = None) -> sqlite3.Connection:
    """
    Abre uma conexão SQLite com PRAGMAs padrão do projeto.
    `prefer` pode ser um caminho de banco para priorizar.
    """
    db_path = ensure_db_path_or_raise(prefer)
    conn = sqlite3.connect(
        db_path,
        timeout=30,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.row_factory = sqlite3.Row
    return conn

__all__ = ["get_db_path", "set_db_path_in_session", "ensure_db_path_or_raise", "get_conn"]
