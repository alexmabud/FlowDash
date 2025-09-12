import sqlite3
import pathlib

DB_PATH = r"data/flowdash_data.db"
SQL_PATH = pathlib.Path("sql/001_vw_cap_saldos.sql")

def main():
    sql = SQL_PATH.read_text(encoding="utf-8")
    con = sqlite3.connect(DB_PATH)
    con.executescript(sql)
    con.commit()
    ok = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='view' AND name='vw_cap_saldos'"
    ).fetchone() is not None
    con.close()
    print(f"vw_cap_saldos {'OK' if ok else 'NÃO CRIADA'} em {DB_PATH}")

if __name__ == "__main__":
    main()
