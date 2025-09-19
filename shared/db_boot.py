# shared/db_boot.py
from __future__ import annotations
import os
import pathlib
import shutil
import sqlite3
import time
import streamlit as st
from shared.db_from_dropbox_api import ensure_local_db_api  # usa o “motor” via token

# ---------- utils ----------
def _root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parent.parent

def _db_local_path() -> pathlib.Path:
    p = _root() / "data" / "flowdash_data.db"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def _tpl_path() -> pathlib.Path:
    return _root() / "data" / "flowdash_template.db"

def _is_sqlite(p: pathlib.Path) -> bool:
    try:
        with open(p, "rb") as f:
            return f.read(16).startswith(b"SQLite format 3")
    except Exception:
        return False

def _has_table(p: pathlib.Path, name: str) -> bool:
    try:
        with sqlite3.connect(str(p)) as conn:
            return conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;", (name,)
            ).fetchone() is not None
    except Exception:
        return False

def _create_table_from_template(db: pathlib.Path, table: str) -> None:
    tpl = _tpl_path()
    if not tpl.exists():
        return
    with sqlite3.connect(str(tpl)) as tconn, sqlite3.connect(str(db)) as dconn:
        tconn.row_factory = sqlite3.Row
        row = tconn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?;", (table,)).fetchone()
        if row and row["sql"]:
            dconn.execute(row["sql"])
        for r in tconn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL;", (table,)
        ):
            if r["sql"]:
                dconn.execute(r["sql"])
        for r in tconn.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? AND sql IS NOT NULL;", (table,)
        ):
            if r["sql"]:
                dconn.execute(r["sql"])
        dconn.commit()

def _ensure_required_tables(db: pathlib.Path) -> None:
    if not _has_table(db, "usuarios"):
        _create_table_from_template(db, "usuarios")

# ---------- função pública ----------
def ensure_db_available(*, show_badge: bool = True) -> str:
    """
    Mesma política para main e PDV:
      1) Tenta baixar via TOKEN do Dropbox (secrets/env);
      2) Se falhar, usa local se válido;
      3) Senão, tenta copiar template; se não houver, cria vazio.

    Seta:
      st.session_state['db_source'] em {'dropbox_token','local','template','vazio','erro'}
      os.environ['FLOWDASH_DB'] com o caminho local.
    """
    db_local = _db_local_path()
    tpl = _tpl_path()

    access_token = (st.secrets.get("dropbox", {}).get("access_token", "") or
                    os.getenv("FLOWDASH_DBX_TOKEN", "")).strip()
    dropbox_path = (st.secrets.get("dropbox", {}).get("file_path", "/FlowDash/data/flowdash_data.db") or
                    os.getenv("FLOWDASH_DBX_FILE", "/FlowDash/data/flowdash_data.db")).strip()
    force_download = (st.secrets.get("dropbox", {}).get("force_download", "0") == "1" or
                      os.getenv("FLOWDASH_FORCE_DB_DOWNLOAD", "0") == "1")

    # 1) via token
    if access_token:
        try:
            ensure_local_db_api(
                access_token=access_token,
                dropbox_path=dropbox_path,
                dest_path=str(db_local),
                force_download=force_download,
                validate_table="usuarios",
            )
            if db_local.exists() and db_local.stat().st_size > 0 and _is_sqlite(db_local):
                _ensure_required_tables(db_local)
                st.session_state["db_source"] = "dropbox_token"
        except Exception as e:
            st.warning(f"Falha ao baixar via token do Dropbox: {e}")

    # 2) local
    if st.session_state.get("db_source") != "dropbox_token":
        if db_local.exists() and db_local.stat().st_size > 0 and _is_sqlite(db_local):
            _ensure_required_tables(db_local)
            st.session_state["db_source"] = "local"
        else:
            # 3) template ou vazio
            try:
                db_local.parent.mkdir(parents=True, exist_ok=True)
                if tpl.exists():
                    shutil.copy2(tpl, db_local)
                    _ensure_required_tables(db_local)
                    st.session_state["db_source"] = "template"
                else:
                    db_local.touch()
                    st.session_state["db_source"] = "vazio"
            except Exception as e:
                st.session_state["db_source"] = "erro"
                st.error(f"Falha no fallback do DB: {e}")

    # exporta p/ outros módulos
    os.environ["FLOWDASH_DB"] = str(db_local)

    if show_badge:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        src = st.session_state.get("db_source", "?")
        st.caption(f"🗃️ Banco em uso **{src}** → `{db_local}` • {ts}")

    return str(db_local)
