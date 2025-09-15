#!/usr/bin/env python3
"""
Migration: adiciona as colunas `origem` e `ledger_id` na tabela `contas_a_pagar_mov`
se elas não existirem.

Uso:
    python migrations/2025_09_15_add_origem_ledger_CAP.py [CAMINHO_DO_DB]

- Idempotente (seguro rodar mais de uma vez).
- Não altera dados existentes.
- Tipos TEXT e NULL por padrão (mais seguro para compatibilidade).
"""

import os
import sys
import sqlite3
from datetime import datetime

DEFAULT_DB = os.path.join("data", "flowdash_data.db")

def get_db_path() -> str:
    if len(sys.argv) >= 2 and sys.argv[1].strip():
        return sys.argv[1].strip()
    env = os.getenv("FLOWDASH_DB", "").strip()
    return env if env else DEFAULT_DB

def has_table(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
        (table,),
    )
    return cur.fetchone() is not None

def has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table});")
    return any(row[1] == column for row in cur.fetchall())

def list_columns(conn: sqlite3.Connection, table: str):
    cur = conn.execute(f"PRAGMA table_info({table});")
    return [row[1] for row in cur.fetchall()]

def add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, ddl_type: str):
    if not has_column(conn, table, column):
        print(f"Criando coluna `{column}` em `{table}`...")
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_type};")
        conn.commit()
        print(f"Coluna `{column}` criada com sucesso.")
    else:
        print(f"OK: coluna `{column}` já existe. Nenhuma alteração feita.")

def main():
    db_path = get_db_path()
    print(f"[{datetime.now().isoformat(timespec='seconds')}] Conectando em: {db_path}")
    if not os.path.exists(db_path):
        print("ERRO: arquivo .db não encontrado. Informe o caminho correto.")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")

        table = "contas_a_pagar_mov"
        if not has_table(conn, table):
            print(f"ERRO: tabela `{table}` não existe neste banco.")
            sys.exit(2)

        # Colunas a garantir (TEXT por segurança/compatibilidade)
        add_column_if_missing(conn, table, "origem", "TEXT")
        add_column_if_missing(conn, table, "ledger_id", "TEXT")

        cols = list_columns(conn, table)
        print("Colunas atuais:", cols)
        print("Concluído.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
