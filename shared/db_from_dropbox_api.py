# -*- coding: utf-8 -*-
r"""
FlowDash — Bootstrap do banco via Dropbox (API com Token)

Resumo
------
Baixa um .db do Dropbox usando a API oficial (sem link público), salvando em `dest_path`.
Dispensa instalar SDK: usa endpoint HTTPS /2/files/download.

Como usar (próximo passo)
------------------------
1) Adicione ao `.streamlit/secrets.toml`:
    [dropbox]
    access_token = "sl.ABC..."           # token de API (App OAuth2)
    file_path    = "/FlowDash/flowdash_data.db"  # caminho no Dropbox
    force_download = "0"

2) No `main.py`, priorize este loader quando `access_token` existir:
    from shared.db_from_dropbox_api import ensure_local_db_api
    caminho_banco = ensure_local_db_api(...)

Permissões necessárias no App do Dropbox
----------------------------------------
- files.content.read

Observações
-----------
- Valida cabeçalho SQLite e tabela obrigatória 'usuarios' por padrão.
- Em caso de erro, lança DropboxApiError com mensagem detalhada.
"""

from __future__ import annotations
import json
import os
import time
import pathlib
from typing import Optional
import sqlite3
import requests


class DropboxApiError(RuntimeError):
    pass


def _download_via_api(
    access_token: str,
    dropbox_path: str,
    dest_path: str,
    timeout: int = 60,
    max_retries: int = 3,
) -> None:
    """
    Faz download do arquivo `dropbox_path` usando a API /2/files/download.
    Salva atomically em `dest_path`.
    """
    if not access_token:
        raise ValueError("access_token vazio.")
    if not dropbox_path:
        raise ValueError("dropbox_path vazio.")

    url = "https://content.dropboxapi.com/2/files/download"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Dropbox-API-Arg": json.dumps({"path": dropbox_path}),
    }

    tmp_path = f"{dest_path}.part"
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            with requests.post(url, headers=headers, timeout=timeout, stream=True) as r:
                if r.status_code != 200:
                    # Tenta extrair mensagem de erro do corpo
                    try:
                        err = r.json()
                    except Exception:
                        err = r.text
                    raise DropboxApiError(f"HTTP {r.status_code}: {err}")

                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)

            # troca atômica
            if os.path.exists(dest_path):
                os.remove(dest_path)
            os.replace(tmp_path, dest_path)
            return
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
            time.sleep(min(2 * attempt, 5))

    raise DropboxApiError(f"Falha ao baixar via API após {max_retries} tentativas: {last_exc}")


def _is_sqlite(path: pathlib.Path) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(16).startswith(b"SQLite format 3")
    except Exception:
        return False


def _has_table(path: pathlib.Path, table: str) -> bool:
    try:
        with sqlite3.connect(str(path)) as conn:
            cur = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (table,)
            )
            return cur.fetchone() is not None
    except Exception:
        return False


def ensure_local_db_api(
    access_token: str,
    dropbox_path: str = "/FlowDash/flowdash_data.db",
    dest_path: str = "data/flowdash_data.db",
    force_download: bool = False,
    validate_table: str = "usuarios",
) -> str:
    """
    Garante `dest_path` local, baixando via API do Dropbox se necessário.

    - Se force_download=True, sempre baixa.
    - Valida arquivo SQLite e (opcionalmente) existência de `validate_table`.
    - Retorna o caminho absoluto para o arquivo local.
    """
    dest = pathlib.Path(dest_path).resolve()
    dest.parent.mkdir(parents=True, exist_ok=True)

    must_download = force_download or (not dest.exists() or dest.stat().st_size == 0)
    if must_download:
        _download_via_api(access_token, dropbox_path, str(dest))

    # validação mínima
    if not _is_sqlite(dest):
        raise DropboxApiError("Arquivo baixado não parece ser um SQLite válido.")
    if validate_table and not _has_table(dest, validate_table):
        raise DropboxApiError(f"SQLite válido, porém sem a tabela obrigatória '{validate_table}'.")

    return str(dest)
