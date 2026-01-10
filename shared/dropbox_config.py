# shared/dropbox_config.py
# -*- coding: utf-8 -*-
"""
Leitor robusto de configuração do Dropbox (ENV + st.secrets) com diagnóstico.

Uso (CLI):
    python -m shared.dropbox_config

Integração (Streamlit) — no próximo passo:
    from shared.dropbox_config import print_streamlit_debug, load_dropbox_settings
    print_streamlit_debug()  # mostra diagnóstico dentro do app
"""
from __future__ import annotations

import os
import sqlite3
from typing import Dict, Tuple
from shared.db import get_conn

DEFAULT_FILE_PATH = "/FlowDash/data/flowdash_data.db"


def _read_env() -> Tuple[str, str, str]:
    """Lê das variáveis de ambiente."""
    tok = (os.getenv("DROPBOX_ACCESS_TOKEN") or os.getenv("DROPBOX_TOKEN") or "").strip()
    if tok:
        src = "env:DROPBOX_ACCESS_TOKEN" if os.getenv("DROPBOX_ACCESS_TOKEN") else "env:DROPBOX_TOKEN"
    else:
        src = "none"

    fp = (os.getenv("DROPBOX_FILE_PATH") or "").strip()
    fd = (os.getenv("DROPBOX_FORCE_DOWNLOAD") or "").strip()
    return tok, src, fp or "" , fd or ""


def _read_secrets() -> Tuple[str, str, str]:
    """Tenta ler do st.secrets['dropbox'] se Streamlit estiver disponível."""
    try:
        import streamlit as st  # import tardio para não quebrar CLI
        if "dropbox" in st.secrets:
            sec = dict(st.secrets.get("dropbox", {}))
            tok = str(sec.get("access_token", "") or sec.get("token", "")).strip()
            fp  = str(sec.get("file_path", "")).strip()
            fd  = str(sec.get("force_download", "")).strip()
            return tok, "st.secrets:/dropbox/access_token" if tok else "none", fp, fd
    except Exception:
        pass
    return "", "none", "", ""


def _coerce_bool(val: str) -> bool:
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}


def load_dropbox_settings(prefer_env_first: bool = True) -> Dict[str, object]:
    """
    Carrega config do Dropbox, priorizando ENV (por padrão) e depois st.secrets.
    Retorna dict com: access_token, token_source, file_path, force_download.
    """
    tok_env, src_env, fp_env, fd_env = _read_env()
    tok_sec, src_sec, fp_sec, fd_sec = _read_secrets()

    if prefer_env_first:
        tok = tok_env or tok_sec
        src = src_env if tok_env else src_sec
        fp  = fp_env or fp_sec or DEFAULT_FILE_PATH
        fd  = fd_env or fd_sec or "0"
    else:
        tok = tok_sec or tok_env
        src = src_sec if tok_sec else src_env
        fp  = fp_sec or fp_env or DEFAULT_FILE_PATH
        fd  = fd_sec or fd_env or "0"

    return {
        "access_token": tok,
        "token_source": src if tok else "none",
        "file_path": fp,
        "force_download": _coerce_bool(fd),
    }


def mask_token(token: str) -> str:
    """Mascara o token mantendo só os 4 últimos caracteres."""
    if not token:
        return ""
    if len(token) <= 8:
        return "*" * (len(token) - 2) + token[-2:]
    return "*" * (len(token) - 4) + token[-4:]


def validate_sqlite(path: str) -> Tuple[bool, str]:
    """
    Verifica se o arquivo SQLite existe e possui a tabela 'usuarios'.
    Retorna (ok, detalhe).
    """
    if not os.path.exists(path):
        return False, "arquivo não existe"
    try:
        with get_conn(path) as conn:
            cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='usuarios';")
            row = cur.fetchone()
            if not row:
                return False, "sem tabela 'usuarios'"
            return True, "ok"
    except Exception as e:
        return False, f"erro ao abrir: {e}"


def build_diagnostic() -> Dict[str, object]:
    """Monta um dicionário de diagnóstico para exibir no app ou via CLI."""
    cfg = load_dropbox_settings(prefer_env_first=True)
    ok, det = validate_sqlite(os.path.join("data", "flowdash_data.db"))
    return {
        "st.secrets keys": _safe_list_secrets_keys(),
        "has_dropbox_section": _has_secrets_dropbox(),
        "token_source": cfg["token_source"],
        "access_token(masked)": mask_token(cfg["access_token"]),
        "token_length": len(cfg["access_token"] or ""),
        "file_path": cfg["file_path"],
        "force_download": 1 if cfg["force_download"] else 0,
        "local_db_check": {"data/flowdash_data.db": {"valid": ok, "detail": det}},
    }


def _safe_list_secrets_keys():
    try:
        import streamlit as st
        return list(st.secrets.keys())
    except Exception:
        return []


def _has_secrets_dropbox() -> bool:
    try:
        import streamlit as st
        return "dropbox" in st.secrets
    except Exception:
        return False


def print_streamlit_debug():
    """Exibe diagnóstico dentro do Streamlit sem vazar token."""
    try:
        import streamlit as st
    except Exception:
        raise RuntimeError("Streamlit não disponível aqui. Rode via app ou use CLI.")

    diag = build_diagnostic()
    st.write("**Diagnóstico Dropbox (robusto)**")
    st.json(diag)


if __name__ == "__main__":
    # Execução via terminal: python -m shared.dropbox_config
    import json
    print(json.dumps(build_diagnostic(), ensure_ascii=False, indent=2))
