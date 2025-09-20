# -*- coding: utf-8 -*-
"""
dbx_token_fallback.py — Fallback para ler token do Dropbox

Ordem de leitura:
1) st.secrets['dropbox']['access_token']
2) ENV: FLOWDASH_DBX_TOKEN
3) Arquivo local: data/dropbox_token.txt  (conteúdo = token, sem quebras extras)

Uso (no main.py depois):
    from shared.dbx_token_fallback import get_dropbox_token, get_dropbox_path, get_force_download
    token = get_dropbox_token(st)
    dropbox_path = get_dropbox_path(st)
    force_download = get_force_download(st)
"""

from __future__ import annotations
import os
import pathlib
from typing import Optional

def _get_from_secrets(st) -> tuple[Optional[str], Optional[str], bool]:
    try:
        sec = st.secrets.get("dropbox", {})
        if isinstance(sec, dict) and sec:
            token = (sec.get("access_token") or "").strip() or None
            path = (sec.get("file_path") or "/FlowDash/data/flowdash_data.db").strip()
            force_download = str(sec.get("force_download", "0")).strip() == "1"
            return token, path, force_download
    except Exception:
        pass
    return None, None, False

def _get_from_env() -> tuple[Optional[str], Optional[str], bool]:
    token = (os.getenv("FLOWDASH_DBX_TOKEN") or "").strip() or None
    path = (os.getenv("FLOWDASH_DBX_FILE") or "/FlowDash/data/flowdash_data.db").strip()
    force_download = (os.getenv("FLOWDASH_FORCE_DB_DOWNLOAD", "0").strip() == "1")
    return token, path, force_download

def _get_from_file() -> tuple[Optional[str], Optional[str], bool]:
    """
    Lê token de data/dropbox_token.txt (linha única).
    Não versionar esse arquivo (garantir .gitignore).
    """
    try:
        root = pathlib.Path(__file__).resolve().parents[1]  # raiz do projeto
        fpath = root / "data" / "dropbox_token.txt"
        if fpath.exists() and fpath.stat().st_size > 0:
            token = fpath.read_text(encoding="utf-8").strip() or None
            path = "/FlowDash/data/flowdash_data.db"
            return token, path, False
    except Exception:
        pass
    return None, None, False

def get_dropbox_token(st) -> Optional[str]:
    for getter in (_get_from_secrets, lambda s=st: _get_from_env(), lambda s=st: _get_from_file()):
        token, _, _ = getter(st) if getter is _get_from_secrets else getter()
        if token:
            return token
    return None

def get_dropbox_path(st) -> str:
    # devolve o primeiro path encontrado; default padrão se nada
    for getter in (_get_from_secrets, lambda s=st: _get_from_env(), lambda s=st: _get_from_file()):
        _, path, _ = getter(st) if getter is _get_from_secrets else getter()
        if path:
            return path
    return "/FlowDash/data/flowdash_data.db"

def get_force_download(st) -> bool:
    for getter in (_get_from_secrets, lambda s=st: _get_from_env(), lambda s=st: _get_from_file()):
        _, _, force = getter(st) if getter is _get_from_secrets else getter()
        if force:
            return True
    return False
