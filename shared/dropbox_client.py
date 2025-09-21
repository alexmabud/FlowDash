# shared/dropbox_client.py
"""
FlowDash — Dropbox Client (Refresh Token)

Resumo
------
Cria um cliente Dropbox com renovação automática de token usando:
  - dropbox.oauth2_refresh_token
  - dropbox.app_key
  - dropbox.app_secret

Expõe utilitários:
  - get_dbx(): retorna um cliente autenticado (com refresh automático)
  - download_bytes(dbx, path): baixa bytes de um arquivo
  - upload_bytes(dbx, path, data): envia bytes para um arquivo (overwrite)

Dependências
------------
- pip install dropbox

Segurança
---------
- NUNCA commitar chaves/segredos. Use .streamlit/secrets.toml (local e cloud).
- Campos esperados em secrets:
    [dropbox]
    app_key         = "APP_KEY"
    app_secret      = "APP_SECRET"
    refresh_token   = "REFRESH_TOKEN"
    file_path       = "/FlowDash/data/flowdash_data.db"   # caminho no Dropbox
    force_download  = "0"                                 # opcional

Notas
-----
- Se ainda existir 'access_token' legado nos secrets, ele será ignorado aqui.
- Este módulo NÃO sabe onde salvar/ler o DB local; apenas trafega bytes.
"""

from __future__ import annotations
from typing import Optional
import io

import streamlit as st
import dropbox
from dropbox.files import WriteMode


class DropboxConfigError(RuntimeError):
    pass


def _read_cfg():
    cfg = st.secrets.get("dropbox", {})
    app_key = cfg.get("app_key")
    app_secret = cfg.get("app_secret")
    refresh_token = cfg.get("refresh_token")
    if not app_key or not app_secret or not refresh_token:
        raise DropboxConfigError(
            "Configuração incompleta do Dropbox. "
            "Defina dropbox.app_key, dropbox.app_secret e dropbox.refresh_token nos secrets."
        )
    return app_key, app_secret, refresh_token


def get_dbx() -> dropbox.Dropbox:
    """
    Retorna um cliente Dropbox com refresh automático de token.

    Retorna
    -------
    dropbox.Dropbox
    """
    app_key, app_secret, refresh_token = _read_cfg()
    # O SDK vai cuidar do ciclo de refresh do access token internamente:
    dbx = dropbox.Dropbox(
        oauth2_refresh_token=refresh_token,
        app_key=app_key,
        app_secret=app_secret,
        timeout=30,
    )
    # Ping rápido para validar credenciais (opcional, mas útil para erros claros):
    try:
        dbx.users_get_current_account()
    except Exception as e:
        raise DropboxConfigError(f"Falha ao autenticar no Dropbox (verifique refresh_token/app_key/app_secret): {e}")
    return dbx


def download_bytes(dbx: dropbox.Dropbox, path: str) -> bytes:
    """
    Baixa um arquivo do Dropbox e retorna seus bytes.
    """
    try:
        meta, resp = dbx.files_download(path)
        return resp.content
    except dropbox.exceptions.ApiError as e:
        raise RuntimeError(f"Falha ao baixar '{path}' do Dropbox: {e}")


def upload_bytes(dbx: dropbox.Dropbox, path: str, data: bytes) -> None:
    """
    Envia bytes para o Dropbox (overwrite).
    """
    try:
        dbx.files_upload(
            data,
            path,
            mode=WriteMode("overwrite"),
            mute=True,
        )
    except dropbox.exceptions.ApiError as e:
        raise RuntimeError(f"Falha ao enviar '{path}' ao Dropbox: {e}")
