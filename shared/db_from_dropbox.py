# -*- coding: utf-8 -*-
r"""
FlowDash — Bootstrap do banco via Dropbox

Resumo
------
Baixa um arquivo .db hospedado no Dropbox (link compartilhado) para `data/flowdash_data.db`
de forma idempotente, com normalização do parâmetro `dl=1`, timeout e tentativas de retry.

Como usar
---------
No seu `main.py` (ou onde você define o caminho do DB), adicione:

    from shared.db_from_dropbox import ensure_local_db

    DB_PATH = ensure_local_db(
        dropbox_url=os.getenv("FLOWDASH_DB_URL", "<COLE_AQUI_SEU_LINK_DROPBOX>"),
        dest_path=os.getenv("FLOWDASH_DB_PATH", "data/flowdash_data.db"),
        force_download=os.getenv("FLOWDASH_FORCE_DB_DOWNLOAD", "0") == "1",
    )

Observações
-----------
- Se `FLOWDASH_DB_URL` estiver setado no ambiente (Streamlit Cloud), ele prevalece.
- Se o arquivo já existir localmente, não baixa novamente (a menos que force_download=True).
- Troca automaticamente `?dl=0` por `?dl=1`.
- Requer `requests` (já usado no projeto).
"""

from __future__ import annotations
import os
import re
import time
import pathlib
from typing import Optional
import requests


def _normalize_dropbox_url(url: str) -> str:
    """Garante `dl=1` no link do Dropbox e remove parâmetros transitórios."""
    if not url:
        raise ValueError("URL do Dropbox vazia.")
    # remove parâmetros transitórios comuns (ex.: st=, rlkey=) sem quebrar o link
    # mantém 'scl/fi' e 's' etc. Apenas normaliza dl=1.
    if "dl=" in url:
        url = re.sub(r"dl=\d", "dl=1", url)
    else:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}dl=1"
    return url


def _stream_download(url: str, dest_path: str, timeout: int = 30, max_retries: int = 3) -> None:
    """Faz download em chunks para `dest_path` com retries simples."""
    tmp_path = f"{dest_path}.part"
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                total = int(r.headers.get("Content-Length") or 0)
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)
                # valida tamanho (se header presente)
                if total and os.path.getsize(tmp_path) < total * 0.9:
                    raise IOError("Download incompleto: tamanho recebido menor que o esperado.")
            # troca atomica
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
            # pequeno backoff
            time.sleep(min(2 * attempt, 5))

    # estourou tentativas
    raise RuntimeError(f"Falha ao baixar DB do Dropbox após {max_retries} tentativas: {last_exc}")


def ensure_local_db(
    dropbox_url: str,
    dest_path: str = "data/flowdash_data.db",
    force_download: bool = False,
    create_dirs: bool = True,
) -> str:
    """
    Garante que `dest_path` exista localmente, baixando do Dropbox se necessário.

    Parâmetros
    ----------
    dropbox_url : str
        Link compartilhado do Dropbox (qualquer forma). O função normaliza para `dl=1`.
    dest_path : str
        Caminho local do .db (padrão: data/flowdash_data.db).
    force_download : bool
        Se True, baixa sempre e sobrescreve o arquivo local.
    create_dirs : bool
        Se True, cria a pasta de destino (ex.: data/).

    Retorno
    -------
    str
        Caminho absoluto para o arquivo .db local.
    """
    if not dropbox_url:
        raise ValueError("É obrigatório informar o link do Dropbox em `dropbox_url`.")

    dest = pathlib.Path(dest_path).resolve()
    if create_dirs:
        dest.parent.mkdir(parents=True, exist_ok=True)

    must_download = force_download or (not dest.exists() or dest.stat().st_size == 0)

    if must_download:
        url = _normalize_dropbox_url(dropbox_url.strip())
        _stream_download(url, str(dest))

    return str(dest)
