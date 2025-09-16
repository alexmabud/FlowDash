import sqlite3

# >>> AJUSTE AQUI <<<
DB = r"data/flowdash_data.db"
DATA = "2025-09-16"
VALOR = 100.0
COLECAO = "Rick Morty"
FORNECEDOR = "Piticas"

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# Detecta nomes de colunas (para lidar com Colecao/Coleção)
cols = {r[1] for r in cur.execute("PRAGMA table_info(mercadorias)").fetchall()}
col_colecao = "Colecao" if "Colecao" in cols else ("Coleção" if "Coleção" in cols else None)

# Monta WHERE dinâmico conforme colunas existentes
where = ['DATE("Recebimento") = DATE(?)', '"Valor_Recebido" = ?']
params = [DATA, VALOR]

if "Fornecedor" in cols:
    where.append('"Fornecedor" = ?')
    params.append(FORNECEDOR)

if col_colecao:
    where.append(f'"{col_colecao}" = ?')
    params.append(COLECAO)

where_sql = " AND ".join(where)

# Busca linhas (inclui id e rowid para atualização)
sql_sel = f'''
SELECT id, rowid, Data,
       {f'"{col_colecao}",' if col_colecao else ''} 
       "Fornecedor","Faturamento","Recebimento","Valor_Recebido"
FROM mercadorias
WHERE {where_sql}
'''
rows = cur.execute(sql_sel, params).fetchall()

print(f"Encontradas {len(rows)} linhas alvo.")
for r in rows:
    print(dict(r))

if rows:
    ids = [r["id"] for r in rows if "id" in r.keys() and r["id"] is not None]
    if ids:
        qmarks = ",".join("?" * len(ids))
        cur.execute(
            f'''UPDATE mercadorias
                   SET "Faturamento" = NULL,
                       "Recebimento" = NULL,
                       "Valor_Recebido" = NULL
                 WHERE id IN ({qmarks})''',
            ids,
        )
    else:
        rowids = [r["rowid"] for r in rows]
        qmarks = ",".join("?" * len(rowids))
        cur.execute(
            f'''UPDATE mercadorias
                   SET "Faturamento" = NULL,
                       "Recebimento" = NULL,
                       "Valor_Recebido" = NULL
                 WHERE rowid IN ({qmarks})''',
            rowids,
        )
    conn.commit()
    print("✅ Campos limpos em", cur.rowcount, "linha(s).")
else:
    print("Nada a atualizar.")

conn.close()
