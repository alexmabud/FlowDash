import sqlite3, os

# <<< AJUSTE O CAMINHO DO DB AQUI SE PRECISAR >>>
db_path = r"C:\Users\User\OneDrive\Documentos\Python\FlowDash\data\flowdash_data.db"
tabela = "contas_a_pagar_mov"
col = "ledger_id"

print(f"Conectando em: {db_path}")
with sqlite3.connect(db_path) as conn:
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys=OFF;")
    cur.execute("PRAGMA recursive_triggers=OFF;")

    # Confere se a tabela existe
    r = cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;",
        (tabela,)
    ).fetchone()
    if not r:
        raise SystemExit(f"Tabela não encontrada: {tabela}")

    # Confere se a coluna existe
    cols = [row[1] for row in cur.execute(f"PRAGMA table_info({tabela});")]
    cols_lower = {c.lower() for c in cols}
    if col not in cols_lower:
        print(f"- OK: coluna '{col}' já não existe.")
    else:
        # Remover índices que referenciem a coluna
        idx_rows = cur.execute(
            "SELECT name, sql FROM sqlite_master "
            "WHERE type='index' AND tbl_name=? AND sql IS NOT NULL;",
            (tabela,)
        ).fetchall()
        for name, sql in idx_rows:
            if col in (sql or "").lower():
                cur.execute(f'DROP INDEX IF EXISTS "{name}";')
                print(f"- Index removido: {name}")

        # Remover views que referenciem a coluna
        views = cur.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='view' AND sql IS NOT NULL;"
        ).fetchall()
        for vname, vsql in views:
            if col in (vsql or "").lower():
                cur.execute(f'DROP VIEW IF EXISTS "{vname}";')
                print(f"- View removida: {vname}")

        # Remover triggers que referenciem a coluna (caso existam)
        trigs = cur.execute(
            "SELECT name, sql FROM sqlite_master "
            "WHERE type='trigger' AND tbl_name=? AND sql IS NOT NULL;",
            (tabela,)
        ).fetchall()
        for tname, tsql in trigs:
            if col in (tsql or "").lower():
                cur.execute(f'DROP TRIGGER IF EXISTS "{tname}";')
                print(f"- Trigger removida: {tname}")

        # Drop da coluna
        try:
            cur.execute(f'ALTER TABLE {tabela} DROP COLUMN "{col}";')
            print(f"- OK: coluna removida: {col}")
        except sqlite3.OperationalError as e:
            raise SystemExit(f"FALHA no DROP COLUMN {col}: {e}")

    conn.commit()

print("Concluído.")
