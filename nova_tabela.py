# Apagar a tabela mercadorias_old do banco
import sqlite3
from pathlib import Path

DB_PATH = Path(r"C:\Users\User\OneDrive\Documentos\Python\FlowDash\data\flowdash_data.db")
assert DB_PATH.exists(), f"DB não encontrado: {DB_PATH}"

conn = sqlite3.connect(str(DB_PATH))
cur = conn.cursor()

# Verifica existência
exists = cur.execute(
    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='mercadorias_old';"
).fetchone()

if not exists:
    print("[INFO] Tabela 'mercadorias_old' já não existe.")
else:
    conn.execute("BEGIN;")
    try:
        # Remove a tabela antiga
        cur.execute("DROP TABLE mercadorias_old;")
        # (opcional) limpa sequência caso exista
        try:
            cur.execute("DELETE FROM sqlite_sequence WHERE name='mercadorias_old';")
        except Exception:
            pass
        conn.commit()
        print("[OK] Tabela 'mercadorias_old' apagada com sucesso.")
    except Exception as e:
        conn.rollback()
        raise

# Mostrar tabelas restantes para conferência
tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;").fetchall()]
conn.close()
print("\n[Tabelas atuais]:")
print(tables)
