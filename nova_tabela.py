import sqlite3
from pathlib import Path

DB = Path(r"C:\Users\User\OneDrive\Documentos\Python\FlowDash\data\flowdash_data.db")

with sqlite3.connect(str(DB)) as conn:
    cur = conn.cursor()
    # (opcional) ver a linha antes
    print("Antes:", cur.execute("SELECT * FROM usuarios WHERE id = 1;").fetchall())

    cur.execute("DELETE FROM usuarios WHERE id = 1;")
    conn.commit()

    # conferir
    print("Depois:", cur.execute("SELECT * FROM usuarios WHERE id = 1;").fetchall())
