# -*- coding: utf-8 -*-
"""
Init DRE Variáveis (idempotente)
Cria a tabela dre_variaveis (se não existir) e garante as 4 chaves oficiais.
Altera/atualiza (UPSERT) sem duplicar.

Como rodar:
  - No VS Code (Terminal):  python scripts/init_dre_variaveis.py
"""

import os
import sqlite3
from datetime import datetime

# === Ajuste aqui se o seu DB tiver nome diferente ===
BASE_DIR = r"C:\Users\User\OneDrive\Documentos\Python\FlowDash\data"
DB_NAME  = "flowdash_data.db"
DB_PATH  = os.path.join(BASE_DIR, DB_NAME)

SQL_CREATE = """
CREATE TABLE IF NOT EXISTS dre_variaveis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chave TEXT NOT NULL UNIQUE,
    tipo  TEXT NOT NULL CHECK (tipo IN ('num','text','bool')),
    valor_num  REAL,
    valor_text TEXT,
    descricao  TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

UPSERTS = [
    ("aliquota_simples_nacional", "num", 4.32, None, "Alíquota Simples Nacional (%)"),
    ("markup_medio",              "num", 2.40, None, "Markup médio (coeficiente)"),
    ("sacolas_percent",           "num", 1.20, None, "Custo de sacolas sobre faturamento (%)"),
    ("fundo_promocao_percent",    "num", 1.00, None, "Fundo de promoção (%)"),
]

SQL_DELETE_OTHERS = """
DELETE FROM dre_variaveis
WHERE chave NOT IN ('aliquota_simples_nacional','markup_medio','sacolas_percent','fundo_promocao_percent');
"""

def main():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Banco não encontrado em: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA busy_timeout = 5000;")

    try:
        with conn:
            conn.execute(SQL_CREATE)

            # Limpa qualquer chave antiga que não usamos mais (opcional, mas solicitado)
            conn.execute(SQL_DELETE_OTHERS)

            # UPSERT das 4 chaves oficiais
            for chave, tipo, vnum, vtxt, desc in UPSERTS:
                if tipo == "num":
                    conn.execute(
                        """
                        INSERT INTO dre_variaveis (chave, tipo, valor_num, descricao, updated_at)
                        VALUES (?,?,?,?,datetime('now'))
                        ON CONFLICT(chave) DO UPDATE SET
                          tipo=excluded.tipo,
                          valor_num=excluded.valor_num,
                          valor_text=NULL,
                          descricao=excluded.descricao,
                          updated_at=datetime('now');
                        """,
                        (chave, tipo, float(vnum or 0.0), desc),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO dre_variaveis (chave, tipo, valor_text, descricao, updated_at)
                        VALUES (?,?,?,?,datetime('now'))
                        ON CONFLICT(chave) DO UPDATE SET
                          tipo=excluded.tipo,
                          valor_text=excluded.valor_text,
                          valor_num=NULL,
                          descricao=excluded.descricao,
                          updated_at=datetime('now');
                        """,
                        (chave, tipo, (vtxt or ""), desc),
                    )

        # Exibe um resumo no final
        cur = conn.execute("""
            SELECT chave, tipo, COALESCE(valor_num, valor_text) AS valor, descricao, updated_at
            FROM dre_variaveis
            ORDER BY chave;
        """)
        rows = cur.fetchall()
        print(f"\n✅ dre_variaveis atualizado em {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"DB: {DB_PATH}\n")
        for r in rows:
            print(f"- {r[0]:>24s} | {r[1]:3s} | {str(r[2]):>8s} | {r[3]}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
