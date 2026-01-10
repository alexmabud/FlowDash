# -*- coding: utf-8 -*-
"""
Sincroniza o template (sem dados) a partir do banco real.

Uso (na raiz do projeto):
    python scripts/sync_template_from_live.py

- Lê o esquema do banco local real: data/flowdash_data.db
- Recria data/flowdash_template.db com o MESMO ESQUEMA (tabelas, índices, triggers, views)
- NÃO copia dados (remove INSERTs)
- Mantém PRAGMA user_version (compatível com migrations)
"""

from __future__ import annotations
import sqlite3
import re
import sys
from pathlib import Path
from typing import Iterable

# Add parent directory to path for imports
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.db import get_conn

DATA_DIR = ROOT / "data"
LIVE_DB = DATA_DIR / "flowdash_data.db"
TEMPLATE_DB = DATA_DIR / "flowdash_template.db"
BACKUP_DB = DATA_DIR / "flowdash_template.bak"


def _read_user_version(conn: sqlite3.Connection) -> int:
    cur = conn.execute("PRAGMA user_version;")
    row = cur.fetchone()
    return int(row[0]) if row else 0


def _iter_schema_ddl(conn: sqlite3.Connection) -> Iterable[str]:
    """
    Usa iterdump() e filtra para manter apenas DDL (CREATE/ALTER/INDEX/TRIGGER/VIEW)
    e pragmas relevantes. Remove INSERTs para evitar dados no template.
    """
    for line in conn.iterdump():
        s = line.strip()
        if not s:
            continue

        SUP = s.upper()

        # Ignora dados
        if SUP.startswith("INSERT INTO "):
            continue

        # Mantém DDL
        if SUP.startswith(("CREATE TABLE", "CREATE INDEX", "CREATE TRIGGER", "CREATE VIEW", "ALTER TABLE")):
            yield s
            continue

        # Pragmas úteis
        if SUP.startswith(("PRAGMA foreign_keys", "PRAGMA auto_vacuum", "PRAGMA encoding")):
            yield s
            continue

        # Ignora controle de transação do dump
        if SUP in ("BEGIN TRANSACTION;", "COMMIT;"):
            continue

        # Fallback para DDL
        if re.match(r"^(CREATE|ALTER)\b", s, flags=re.IGNORECASE):
            yield s


def main() -> None:
    if not LIVE_DB.exists():
        raise SystemExit(f"[ERRO] Banco real não encontrado: {LIVE_DB}")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[1/4] Lendo esquema do banco real: {LIVE_DB}")
    with get_conn(str(LIVE_DB)) as live:
        user_version = _read_user_version(live)
        ddl_lines = list(_iter_schema_ddl(live))

    # Backup do template atual, se existir
    if TEMPLATE_DB.exists():
        try:
            if BACKUP_DB.exists():
                BACKUP_DB.unlink()
            TEMPLATE_DB.replace(BACKUP_DB)
            print(f"[2/4] Backup do template salvo em: {BACKUP_DB.name}")
        except Exception as e:
            print(f"[AVISO] Falha ao criar backup do template: {e}")

    # Recria template do zero
    if TEMPLATE_DB.exists():
        TEMPLATE_DB.unlink()

    print(f"[3/4] Criando novo template vazio: {TEMPLATE_DB}")
    with get_conn(str(TEMPLATE_DB)) as tpl:
        cur = tpl.cursor()
        for stmt in ddl_lines:
            try:
                cur.execute(stmt)
            except Exception as e:
                raise RuntimeError(f"Falhou ao executar DDL:\n{stmt}\nErro: {e}") from e
        # Mantém user_version para migrations
        try:
            cur.execute(f"PRAGMA user_version = {int(user_version)};")
        except Exception:
            pass
        tpl.commit()

    print("[4/4] Template atualizado com o mesmo esquema do banco real (sem dados).")
    print("Pronto! Faça commit do novo data/flowdash_template.db.")


if __name__ == "__main__":
    main()
