# -*- coding: utf-8 -*-
r"""
FlowDash — Bootstrap do banco via Dropbox (API com Token)

Resumo
------
Baixa um .db do Dropbox usando a API oficial (sem link público), salvando em `dest_path`.
Dispensa instalar SDK: usa endpoint HTTPS /2/files/download.

Como usar (opção A: via secrets)
-------------------------------
1) Adicione ao `.streamlit/secrets.toml`:
    [dropbox]
    access_token   = "sl.ABC..."                      # token de API (App OAuth2)
    file_path      = "/FlowDash/flowdash_data.db"     # caminho no Dropbox
    force_download = "0"                              # "1" para forçar baixar sempre (opcional)
    dest_path      = "data/flowdash_data.db"          # opcional; default cai aqui

2) No `main.py`, logo no início:
    from shared.db_from_dropbox_api import ensure_local_db_api_from_secrets
    caminho_banco = ensure_local_db_api_from_secrets()  # seta na sessão e retorna o path

Como usar (opção B: via variáveis de ambiente)
----------------------------------------------
Defina:
    DROPBOX_ACCESS_TOKEN=sl.ABC...
    DROPBOX_FILE_PATH=/FlowDash/flowdash_data.db
    DROPBOX_FORCE_DOWNLOAD=0
    FLOWDASH_DB_PATH=data/flowdash_data.db                  # opcional (destino)
    FLOWDASH_DB_VALIDATE_TABLE=usuarios                     # opcional

No `main.py`:
    from shared.db_from_dropbox_api import ensure_local_db_api_from_env
    caminho_banco = ensure_local_db_api_from_env()          # seta na sessão e retorna o path

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
from typing import Optional, Mapping
import sqlite3
import requests

# Integração com camada de DB do projeto (para registrar caminho na sessão)
try:
    from shared.db import set_db_path_in_session as _set_db_path_in_session, get_conn as _get_conn
except Exception:
    def _set_db_path_in_session(path: str) -> str:  # fallback neutro
        return path
    import sqlite3 as _sqlite3
    def _get_conn(path: str):  # fallback
        return _sqlite3.connect(path)

# Integração opcional com Streamlit secrets (sem quebrar quando não estiver no runtime do Streamlit)
try:
    import streamlit as st  # type: ignore
except Exception:
    st = None  # noqa: N816 (usamos nome curto st)


class DropboxApiError(RuntimeError):
    pass


# -------------------------- Helpers internos --------------------------

def _to_bool(v: object, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "sim"}:
        return True
    if s in {"0", "false", "no", "n", "nao", "não"}:
        return False
    return default


def _guess_dest_path(dest_path: Optional[str]) -> str:
    """
    Resolve destino do .db:
      1) dest_path informado
      2) FLOWDASH_DB_PATH (env)
      3) data/flowdash_data.db (default do projeto)
    """
    if isinstance(dest_path, str) and dest_path.strip():
        return dest_path
    envp = os.getenv("FLOWDASH_DB_PATH", "").strip()
    if envp:
        return envp
    return "data/flowdash_data.db"


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
        with _get_conn(str(path)) as conn:
            cur = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (table,)
            )
            return cur.fetchone() is not None
    except Exception:
        return False


# -------------------------- API pública principal --------------------------

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
    - Integra com o projeto: registra o caminho no session_state (quando disponível).
    """
    dest_path = _guess_dest_path(dest_path)
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

    # Integra com sessão (caso o runtime do Streamlit exista)
    try:
        _set_db_path_in_session(str(dest))
    except Exception:
        pass

    return str(dest)


# -------------------------- Atalhos de auto-config --------------------------

def ensure_local_db_api_from_secrets(
    secrets: Optional[Mapping[str, object]] = None,
    validate_table: Optional[str] = None,
) -> str:
    """
    Lê configurações de `st.secrets['dropbox']` (ou de um dicionário passado)
    e garante o DB local via API, retornando o caminho.
    Chaves reconhecidas:
      - access_token (obrigatória)
      - file_path (default: /FlowDash/flowdash_data.db)
      - force_download ("0"/"1", default: "0")
      - dest_path (default: data/flowdash_data.db)
    """
    cfg = None
    if isinstance(secrets, Mapping):
        cfg = secrets
    elif st is not None:
        try:
            cfg = st.secrets.get("dropbox")  # type: ignore[attr-defined]
        except Exception:
            cfg = None

    if not isinstance(cfg, Mapping):
        raise DropboxApiError("Configuração Dropbox não encontrada em st.secrets['dropbox'].")

    access_token = str(cfg.get("access_token", "")).strip()
    file_path = str(cfg.get("file_path", "/FlowDash/flowdash_data.db")).strip()
    force = _to_bool(cfg.get("force_download", "0"))
    dest_path = str(cfg.get("dest_path", "data/flowdash_data.db")).strip()

    if not access_token:
        raise DropboxApiError("dropbox.access_token ausente em secrets.")

    vt = validate_table if isinstance(validate_table, str) else str(cfg.get("validate_table", "usuarios")).strip()

    return ensure_local_db_api(
        access_token=access_token,
        dropbox_path=file_path or "/FlowDash/flowdash_data.db",
        dest_path=dest_path or "data/flowdash_data.db",
        force_download=force,
        validate_table=vt or "usuarios",
    )


def ensure_local_db_api_from_env(
    validate_table: Optional[str] = None,
) -> str:
    """
    Lê configurações de variáveis de ambiente e garante o DB local via API.
    Variáveis:
      - DROPBOX_ACCESS_TOKEN (obrigatória)
      - DROPBOX_FILE_PATH (default: /FlowDash/flowdash_data.db)
      - DROPBOX_FORCE_DOWNLOAD (default: 0)
      - FLOWDASH_DB_PATH (opcional, destino do arquivo local)
      - FLOWDASH_DB_VALIDATE_TABLE (opcional; ex.: usuarios)
    """
    access_token = os.getenv("DROPBOX_ACCESS_TOKEN", "").strip()
    file_path = os.getenv("DROPBOX_FILE_PATH", "/FlowDash/flowdash_data.db").strip()
    force = _to_bool(os.getenv("DROPBOX_FORCE_DOWNLOAD", "0"))
    dest_path = os.getenv("FLOWDASH_DB_PATH", "data/flowdash_data.db").strip()

    if not access_token:
        raise DropboxApiError("DROPBOX_ACCESS_TOKEN não definido.")

    vt = validate_table or os.getenv("FLOWDASH_DB_VALIDATE_TABLE", "usuarios").strip() or "usuarios"

    return ensure_local_db_api(
        access_token=access_token,
        dropbox_path=file_path or "/FlowDash/flowdash_data.db",
        dest_path=dest_path or "data/flowdash_data.db",
        force_download=force,
        validate_table=vt,
    )


__all__ = [
    "DropboxApiError",
    "ensure_local_db_api",
    "ensure_local_db_api_from_secrets",
    "ensure_local_db_api_from_env",
]
