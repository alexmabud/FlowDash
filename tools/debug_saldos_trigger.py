# -*- coding: utf-8 -*-
"""
Trava simples de DEBUG em `saldos_caixas`
----------------------------------------
Uso:
    # Instalar (padrão)
    python tools/debug_saldos_trigger.py

    # Remover
    python tools/debug_saldos_trigger.py --off

Como funciona:
- Detecta automaticamente o .db (prioridade: $FLOWDASH_DB > data/flowdash_data.db > data/flowdash_template.db > varredura local).
- Instala uma trigger que ABORTA qualquer INSERT em `saldos_caixas`.
- Ao subir o app, se algo tentar inserir no boot, vai estourar:
      DEBUG: INSERT em saldos_caixas no boot
  e a stack no terminal mostra arquivo/linha culpados.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path
from typing import Optional

TRIGGER_NAME = "debug_block_saldos_insert"
TRIGGER_SQL = f"""
CREATE TRIGGER IF NOT EXISTS {TRIGGER_NAME}
BEFORE INSERT ON saldos_caixas
BEGIN
  SELECT RAISE(ABORT, 'DEBUG: INSERT em saldos_caixas no boot');
END;
"""
DROP_SQL = f"DROP TRIGGER IF EXISTS {TRIGGER_NAME};"

def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;",
        (name,),
    )
    return cur.fetchone() is not None

def _trigger_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?;",
        (name,),
    )
    return cur.fetchone() is not None

def _first_existing(paths: list[Path]) -> Optional[Path]:
    for p in paths:
        if p.exists() and p.is_file():
            return p
    return None

def _discover_db(root: Path) -> Optional[Path]:
    # procura por .db/.sqlite que tenha a tabela saldos_caixas (mais novo primeiro)
    candidates = []
    for p in root.rglob("*"):
        if p.suffix.lower() in {".db", ".sqlite", ".sqlite3"} and p.is_file():
            candidates.append(p)
    candidates.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    for p in candidates:
        try:
            with sqlite3.connect(f"file:{p}?mode=ro", uri=True) as conn:
                if _table_exists(conn, "saldos_caixas"):
                    return p
        except Exception:
            pass
    return None

def _choose_db() -> Path:
    # 1) FLOWDASH_DB
    env = os.environ.get("FLOWDASH_DB")
    if env:
        path = Path(env).expanduser().resolve()
        if path.exists():
            return path

    # 2) caminhos comuns do FlowDash
    here = Path(__file__).resolve().parents[1]  # raiz do projeto (../)
    common = [
        here / "data" / "flowdash_data.db",
        here / "data" / "flowdash_template.db",
    ]
    found = _first_existing(common)
    if found:
        return found

    # 3) varredura local (projeto)
    discovered = _discover_db(here)
    if discovered:
        return discovered

    # 4) fallback: raiz atual
    return (here / "dashboard_rc.db").resolve()

def install(db_path: Path) -> int:
    if not db_path.exists():
        print(f"❌ Banco não encontrado: {db_path}", file=sys.stderr)
        print("   Dica: verifique se o app usa outro .db (ex.: data/flowdash_data.db).", file=sys.stderr)
        return 2
    try:
        # checa existência da tabela sem criar arquivo por engano
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as ro:
            if not _table_exists(ro, "saldos_caixas"):
                print(f"❌ Tabela `saldos_caixas` não existe em: {db_path}", file=sys.stderr)
                return 3
        # instala trigger
        with sqlite3.connect(str(db_path)) as conn:
            conn.executescript(TRIGGER_SQL)
            conn.commit()
        print(f"✅ Trigger instalada em: {db_path}\n   → {TRIGGER_NAME}")
        print("Agora rode:  streamlit run main.py")
        return 0
    except Exception as e:
        print(f"❌ Erro ao instalar trigger: {e}", file=sys.stderr)
        return 1

def remove(db_path: Path) -> int:
    if not db_path.exists():
        print(f"❌ Banco não encontrado: {db_path}", file=sys.stderr)
        return 2
    try:
        with sqlite3.connect(str(db_path)) as conn:
            conn.executescript(DROP_SQL)
            conn.commit()
        print(f"🧹 Trigger removida de: {db_path}\n   → {TRIGGER_NAME}")
        return 0
    except Exception as e:
        print(f"❌ Erro ao remover trigger: {e}", file=sys.stderr)
        return 1

def main() -> int:
    parser = argparse.ArgumentParser(description="Instala/Remove trigger de DEBUG em saldos_caixas (auto-descobre o .db).")
    parser.add_argument("--off", action="store_true", help="Remove a trigger em vez de instalar.")
    args = parser.parse_args()

    db_path = _choose_db()
    print(f"📂 Banco alvo: {db_path}")

    if args.off:
        return remove(db_path)
    return install(db_path)

if __name__ == "__main__":
    raise SystemExit(main())
