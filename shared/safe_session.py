# -*- coding: utf-8 -*-
"""
safe_session
------------

Acesso **seguro** ao st.session_state, evitando o erro:
" Tried to use SessionInfo before it was initialized ".

Uso (nos próximos passos vamos substituir acessos diretos):
    from shared.safe_session import exists, get, setdefault

    if exists():
        usuario = get("usuario_logado")
"""

from __future__ import annotations

from typing import Any, Optional

def _runtime_exists() -> bool:
    # Importa só aqui para não disparar no import-time.
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx  # type: ignore
        return get_script_run_ctx() is not None
    except Exception:
        return False


def exists() -> bool:
    """Retorna True se o runtime do Streamlit estiver pronto."""
    return _runtime_exists()


def get(key: str, default: Optional[Any] = None) -> Any:
    """
    Lê uma chave do session_state com segurança.
    Se o runtime ainda não existir, retorna `default` e **não** quebra a app.
    """
    if not _runtime_exists():
        return default
    try:
        import streamlit as st  # import tardio
        return st.session_state.get(key, default)
    except Exception:
        return default


def setdefault(key: str, default: Any) -> Any:
    """
    Define valor padrão de forma segura. Se o runtime não existir, devolve `default`
    sem tentar escrever.
    """
    if not _runtime_exists():
        return default
    try:
        import streamlit as st  # import tardio
        return st.session_state.setdefault(key, default)
    except Exception:
        return default
