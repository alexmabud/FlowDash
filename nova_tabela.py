#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Migration segura: adiciona a coluna `pin` (4 dígitos) na tabela `usuarios`
sem recriar a tabela (evita falhas de FOREIGN KEY). Idempotente.

O que faz:
  1) ALTER TABLE usuarios ADD COLUMN pin TEXT     (se não existir)
  2) CREATE INDEX idx_usuarios_pin                (se não existir)
  3) Triggers de validação (INSERT/UPDATE) para garantir 4 dígitos ou NULL (se não existirem)

Compatível com Jupyter/IPython (ignora flags tipo --f=...).
Uso:
  - Terminal ou Jupyter:
      python migrations/2025_09_17_add_pin_usuarios.py
    (ou informe outro caminho como argumento posicional)
"""

from __future__ import annotations
import sys
import sqlite3
from pathlib import Path

# ===== CAMINHO PADRÃO DO SEU BANCO (Abud) =====
DEFAULT_DB_PATH = r"C:\Users\User\OneDrive\Documentos\Python\FlowDash\data\flowdash_data.db"
# ==============================================

def _resolve_db_path() -> str:
    # 1) argumento posicional que não começa com '-'
    if len(sys.argv) >= 2:
        for a in reversed(sys.argv[1:]):
            if a and not a.startswith("-"):
                return a
    # 2) padrão
    return DEFAULT_DB_PATH

def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None

def table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(r[1] == column for r in cur.fetchall())

def trigger_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?", (name,))
    return cur.fetchone() is not None

def run_migration(db_path: str) -> int:
    db = Path(db_path)
    print(f"Conectado a base (Python {sys.version.split()[0]})")
    print(f"[INFO] Usando banco: {db}")

    if not db.exists():
        print(f"[ERRO] Banco não encontrado: {db}")
        return 1

    conn = sqlite3.connect(str(db))
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        if not table_exists(conn, "usuarios"):
            print("[ERRO] Tabela `usuarios` não encontrada.")
            return 2

        # 1) Add coluna `pin` se não existir
        if not table_has_column(conn, "usuarios", "pin"):
            print("[INFO] Adicionando coluna `pin` em `usuarios`...")
            conn.execute("ALTER TABLE usuarios ADD COLUMN pin TEXT")
            print("[OK] Coluna `pin` adicionada.")
        else:
            print("[OK] Coluna `pin` já existe.")

        # 2) Índice (opcional)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_usuarios_pin ON usuarios(pin)")

        # 3) Triggers de validação
        trig_ins = "usuarios_pin_check_ins"
        trig_upd = "usuarios_pin_check_upd"

        if not trigger_exists(conn, trig_ins):
            conn.execute(f"""
                CREATE TRIGGER {trig_ins}
                BEFORE INSERT ON usuarios
                FOR EACH ROW
                WHEN NEW.pin IS NOT NULL AND NEW.pin NOT GLOB '[0-9][0-9][0-9][0-9]'
                BEGIN
                    SELECT RAISE(ABORT, 'PIN inválido: use exatamente 4 dígitos (0-9) ou deixe em branco');
                END;
            """)
            print(f"[OK] Trigger {trig_ins} criado.")
        else:
            print(f"[OK] Trigger {trig_ins} já existe.")

        if not trigger_exists(conn, trig_upd):
            conn.execute(f"""
                CREATE TRIGGER {trig_upd}
                BEFORE UPDATE OF pin ON usuarios
                FOR EACH ROW
                WHEN NEW.pin IS NOT NULL AND NEW.pin NOT GLOB '[0-9][0-9][0-9][0-9]'
                BEGIN
                    SELECT RAISE(ABORT, 'PIN inválido: use exatamente 4 dígitos (0-9) ou deixe em branco');
                END;
            """)
            print(f"[OK] Trigger {trig_upd} criado.")
        else:
            print(f"[OK] Trigger {trig_upd} já existe.")

        conn.commit()
        print("[FINALIZADO] Migração concluída com sucesso.")
        return 0

    except Exception as e:
        conn.rollback()
        print(f"[ERRO] Falha na migração: {e}")
        return 4
    finally:
        conn.close()

if __name__ == "__main__":
    path = _resolve_db_path()
    raise SystemExit(run_migration(path))
