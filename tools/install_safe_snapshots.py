# -*- coding: utf-8 -*-
"""
Instala gatilhos SEGUROS que criam o snapshot diário em `saldos_caixas`
APENAS quando houver a 1ª operação do dia (entrada/saída) E se existir véspera.

Regras:
- BEFORE INSERT ON entrada/saida
- Cria a linha do dia SÓ se:
    (1) NÃO existir linha do dia ainda; e
    (2) EXISTIR pelo menos uma linha anterior (véspera) para copiar os totais.
- Copia:
    caixa     <- caixa_total (véspera)
    caixa_2   <- caixa2_total (véspera)
    caixa_vendas <- 0.0
    caixa2_dia   <- 0.0
- `caixa_total` e `caixa2_total` são recalculados por seus triggers atuais.

Uso:
    python tools/install_safe_snapshots.py --db data/flowdash_data.db
    python tools/install_safe_snapshots.py --remove --db data/flowdash_data.db
"""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
import sys

TRG_IN_NAME = "trg_autosnapshot_on_entrada"
TRG_OUT_NAME = "trg_autosnapshot_on_saida"

SNAPSHOT_SQL_TEMPLATE = """
CREATE TRIGGER IF NOT EXISTS {trg_name}
BEFORE INSERT ON {table_name}
BEGIN
    INSERT INTO saldos_caixas (data, caixa, caixa_2, caixa_vendas, caixa2_dia, caixa_total, caixa2_total)
    SELECT
        DATE(NEW.Data) AS data_alvo,
        prev.caixa_total AS caixa_base,
        prev.caixa2_total AS caixa2_base,
        0.0 AS caixa_vendas,
        0.0 AS caixa2_dia,
        prev.caixa_total AS caixa_total_tmp,
        prev.caixa2_total AS caixa2_total_tmp
    FROM (
        SELECT caixa_total, caixa2_total
        FROM saldos_caixas
        WHERE DATE(data) = (
            SELECT MAX(DATE(data)) FROM saldos_caixas
            WHERE DATE(data) < DATE(NEW.Data)
        )
    ) AS prev
    WHERE
        -- só cria se NÃO existir a linha do dia:
        NOT EXISTS (SELECT 1 FROM saldos_caixas WHERE DATE(data) = DATE(NEW.Data))
        -- e se EXISTIR véspera (prev retornou linha):
        AND EXISTS (
            SELECT 1 FROM saldos_caixas
            WHERE DATE(data) < DATE(NEW.Data)
        );
END;
"""

DROP_SQL = f"""
DROP TRIGGER IF EXISTS {TRG_IN_NAME};
DROP TRIGGER IF EXISTS {TRG_OUT_NAME};
"""

def install(db: Path) -> int:
    try:
        with sqlite3.connect(str(db)) as conn:
            # entrada
            conn.executescript(SNAPSHOT_SQL_TEMPLATE.format(
                trg_name=TRG_IN_NAME, table_name="entrada"
            ))
            # saida
            conn.executescript(SNAPSHOT_SQL_TEMPLATE.format(
                trg_name=TRG_OUT_NAME, table_name="saida"
            ))
            conn.commit()
        print(f"✅ Gatilhos instalados em: {db}")
        print(f"   - {TRG_IN_NAME} (entrada)")
        print(f"   - {TRG_OUT_NAME} (saida)")
        print("Obs.: Não cria linha no boot, nem linha zerada (exige véspera).")
        return 0
    except Exception as e:
        print(f"❌ Erro instalando gatilhos: {e}", file=sys.stderr)
        return 1

def remove(db: Path) -> int:
    try:
        with sqlite3.connect(str(db)) as conn:
            conn.executescript(DROP_SQL)
            conn.commit()
        print(f"🧹 Gatilhos removidos de: {db}")
        return 0
    except Exception as e:
        print(f"❌ Erro removendo gatilhos: {e}", file=sys.stderr)
        return 1

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Caminho do .db (ex.: data/flowdash_data.db)")
    ap.add_argument("--remove", action="store_true", help="Remove em vez de instalar")
    args = ap.parse_args()

    db = Path(args.db).expanduser().resolve()
    if not db.exists():
        print(f"❌ Banco não encontrado: {db}", file=sys.stderr)
        return 2

    return remove(db) if args.remove else install(db)

if __name__ == "__main__":
    raise SystemExit(main())
