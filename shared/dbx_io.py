# shared/dbx_io.py
"""
FlowDash — I/O do DB no Dropbox (Refresh Token)

Resumo
------
Wrap simples para baixar/subir o arquivo do banco usando o cliente com
refresh token (shared.dropbox_client). Mantém nomes de funções claros:

    - baixar_db_para_local() -> str (caminho local do DB)
    - enviar_db_local() -> None

Dependências
------------
- shared/dropbox_client.py
- streamlit.secrets['dropbox'] com:
    app_key, app_secret, refresh_token, file_path, force_download (opcional)

Segurança
---------
- Nunca commitar secrets. Usar .streamlit/secrets.toml (local e cloud).
"""

from __future__ import annotations
import pathlib
import streamlit as st

from shared.dropbox_client import get_dbx, download_bytes, upload_bytes


def _local_db_path() -> pathlib.Path:
    """Retorna o caminho local padrão do DB (data/flowdash_data.db)."""
    p = pathlib.Path("data") / "flowdash_data.db"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def baixar_db_para_local() -> str:
    """
    Baixa o DB do Dropbox para o caminho local e retorna o caminho como string.
    Respeita o caminho remoto definido em secrets: dropbox.file_path.
    """
    file_path = st.secrets["dropbox"]["file_path"]
    dbx = get_dbx()
    data = download_bytes(dbx, file_path)

    dst = _local_db_path()
    dst.write_bytes(data)
    return str(dst)


def enviar_db_local() -> None:
    """
    Lê o DB local e envia para o caminho remoto no Dropbox (overwrite).
    """
    file_path = st.secrets["dropbox"]["file_path"]
    src = _local_db_path()
    data = src.read_bytes()

    dbx = get_dbx()
    upload_bytes(dbx, file_path, data)
