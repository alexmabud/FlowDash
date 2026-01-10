# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import sqlite3
import calendar
import datetime as dt
from typing import Optional, List, Tuple

import pandas as pd
import streamlit as st

from flowdash_pages.dataframes.filtros import selecionar_ano, resumo_por_mes
from utils.utils import formatar_moeda as _fmt_moeda_str

# ================= Descoberta de DB (segura) =================
try:
    from shared.db import get_db_path as _shared_get_db_path, ensure_db_path_or_raise
except Exception:
    _shared_get_db_path = None
    def ensure_db_path_or_raise(_: Optional[str] = None) -> str:
        for p in (
            os.path.join("data", "flowdash_data.db"),
            os.path.join("data", "dashboard_rc.db"),
            "dashboard_rc.db",
            os.path.join("data", "flowdash_template.db"),
        ):
            if os.path.exists(p):
                return p
        raise FileNotFoundError("Nenhum banco padrão encontrado.")

def _resolve_db_path(pref: Optional[str]) -> Optional[str]:
    if isinstance(pref, str) and os.path.exists(pref):
        return pref
    if callable(_shared_get_db_path):
        p = _shared_get_db_path()
        if isinstance(p, str) and os.path.exists(p):
            return p
    for p in (
        os.path.join("data", "flowdash_data.db"),
        os.path.join("data", "dashboard_rc.db"),
        "dashboard_rc.db",
        os.path.join("data", "flowdash_template.db"),
    ):
        if os.path.exists(p):
            return p
    return None

def _connect(db_like: Optional[str]) -> Optional[sqlite3.Connection]:
    try:
        db = ensure_db_path_or_raise(db_like)
    except Exception as e:
        st.error("❌ Banco de dados não encontrado para Empréstimos/Financiamentos.")
        st.caption(str(e))
        return None
    try:
        return sqlite3.connect(db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    except Exception as e:
        st.error("❌ Erro ao conectar no banco (Empréstimos/Financiamentos).")
        st.exception(e)
        return None

def _object_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE LOWER(name)=LOWER(?) AND type IN ('table','view') LIMIT 1;",
        (name,),
    )
    return cur.fetchone() is not None

def _list_like_loan_objects(conn: sqlite3.Connection) -> List[Tuple[str,str]]:
    rows = conn.execute("SELECT type, name FROM sqlite_master WHERE type IN ('table','view');").fetchall()
    outs: List[Tuple[str,str]] = []
    for t, n in rows:
        ln = (n or "").lower()
        if ("emprest" in ln) or ("financ" in ln):
            outs.append((t, n))
    return outs

def _table_cols(conn: sqlite3.Connection, name: str) -> List[str]:
    try:
        rows = conn.execute(f'PRAGMA table_info("{name}")').fetchall()
        if rows:
            return [r[1] for r in rows]
    except Exception:
        pass
    try:
        df = pd.read_sql_query(f'SELECT * FROM "{name}" LIMIT 1;', conn)
        return list(df.columns)
    except Exception:
        return []

_CONTRATO_HINT_COLS = {
    "parcelas_total","num_parcelas","n_parcelas","parcelas",
    "valor_parcela","valor_total","banco","taxa_juros_am","taxa_juros","tipo",
    "data_contratacao","data_inicio_pagamento","vencimento","data_vencimento"
}

def _score_loan_object(conn: sqlite3.Connection, name: str) -> Tuple[int,int]:
    cols = [c.lower() for c in _table_cols(conn, name)]
    ncols = len(cols)
    score = 0
    if name.lower() == "emprestimos_financiamentos":
        score += 100
    score += sum(1 for c in cols if c in _CONTRATO_HINT_COLS) * 5
    if set(cols).issubset({"data", "valor", "usuario"}):
        score -= 50
    score += min(ncols, 200)
    return score, ncols

def _pick_loans_object(conn: sqlite3.Connection) -> Optional[str]:
    for name in ("emprestimos_financiamentos", "emprestimo_financiamento"):
        if _object_exists(conn, name):
            return name
    cands = _list_like_loan_objects(conn)
    if not cands:
        return None
    best_name, best_score, best_ncols = None, -10**9, -1
    for _type, name in cands:
        score, ncols = _score_loan_object(conn, name)
        if (score > best_score) or (score == best_score and ncols > best_ncols):
            best_name, best_score, best_ncols = name, score, ncols
    return best_name

# ---------------- Helpers visuais ----------------
def _auto_df_height(df: pd.DataFrame, row_px: int = 30, header_px: int = 36, pad_px: int = 6, max_px: int = 1200) -> int:
    n = int(len(df))
    h = header_px + (n * row_px) + pad_px
    return min(h, max_px)

def _zebra(df: pd.DataFrame, dark: str = "#12161d", light: str = "#1b212b") -> pd.io.formats.style.Styler:
    ncols = df.shape[1]
    def _row_style(row: pd.Series):
        bg = light if (row.name % 2) else dark
        return [f"background-color: {bg}"] * ncols
    return df.style.apply(_row_style, axis=1)

def _fmt_percent_str(v) -> str:
    try:
        return f"{float(v):,.2f}%".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v)

# --------- Parsers robustos (BRL / int) ----------
def _to_float_brl(x, default: float = 0.0) -> float:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return default
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return default
    s = re.sub(r"[^\d,.\-]", "", s)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return default

def _to_int_brl(x, default: int = 0) -> int:
    try:
        return int(round(_to_float_brl(x, float(default))))
    except Exception:
        return default

_MESES_PT_ABREV = {1:"Jan",2:"Fev",3:"Mar",4:"Abr",5:"Mai",6:"Jun",7:"Jul",8:"Ago",9:"Set",10:"Out",11:"Nov",12:"Dez"}

# --------- DataRef auxiliar (somente para filtro do cabeçalho) ---------
_LOAN_DATE_PRIORITY = [
    "data_inicio_pagamento","data_contratacao","data_vencimento",
    "data","vencimento","data_lancamento",
]
def _safe_to_datetime(s: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(s):
        return s
    return pd.to_datetime(s, errors="coerce")
def _best_date_series(df: pd.DataFrame) -> pd.Series:
    lower = {str(c).lower(): c for c in df.columns}
    for key in _LOAN_DATE_PRIORITY:
        if key in lower:
            s = _safe_to_datetime(df[lower[key]])
            if s.notna().any():
                return s
    for c in df.columns:
        if "data" in str(c).lower():
            return _safe_to_datetime(df[c])
    return pd.to_datetime(pd.NaT)

# --------- Geração do CRONOGRAMA (sempre usado para a esquerda) ----------
def _month_add(y: int, m: int, k: int) -> tuple[int,int]:
    z = (y * 12 + (m - 1) + k)
    ny, nm = divmod(z, 12)
    return ny, nm + 1

def _clamp_day(y: int, m: int, d: int) -> int:
    return min(max(1, d), calendar.monthrange(y, m)[1])

def _parcelas_calendar_from_contracts(df: pd.DataFrame, year: int) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame({"Mês": [v for k, v in _MESES_PT_ABREV.items()], "Total": [0.0]*12})

    lower = {str(c).lower(): c for c in df.columns}
    get = lambda key: lower.get(key.lower())

    col_inicio = get("data_inicio_pagamento") or get("data_contratacao") or get("data_lancamento")
    col_parcelas_total = get("parcelas_total") or get("num_parcelas") or get("n_parcelas")
    col_venc_dia = get("vencimento_dia")
    col_valor_parcela = get("valor_parcela")
    col_valor_total = get("valor_total")
    col_quit = get("data_quitacao")

    registros: list[tuple[int,float]] = []

    for _, row in df.iterrows():
        dt_ini = _safe_to_datetime(pd.Series([row.get(col_inicio) if col_inicio else None])).iloc[0]
        if pd.isna(dt_ini):
            continue
        n = _to_int_brl(row.get(col_parcelas_total), 0) if col_parcelas_total else 0
        if n <= 0:
            continue
        if col_valor_parcela:
            vparc = _to_float_brl(row.get(col_valor_parcela), 0.0)
        elif col_valor_total and n > 0:
            vparc = _to_float_brl(row.get(col_valor_total), 0.0) / n
        else:
            vparc = 0.0
        if vparc == 0.0:
            continue
        base_day = _to_int_brl(row.get(col_venc_dia), int(getattr(dt_ini, "day", 1))) if col_venc_dia else int(getattr(dt_ini, "day", 1))
        dt_quit = _safe_to_datetime(pd.Series([row.get(col_quit) if col_quit else None])).iloc[0]

        sy, sm = int(dt_ini.year), int(dt_ini.month)
        for k in range(n):
            y, m = _month_add(sy, sm, k)
            d = _clamp_day(y, m, base_day)
            due = dt.date(y, m, d)
            if pd.notna(dt_quit) and due > dt_quit.date():
                break
            if y == year:
                registros.append((m, vparc))

    full = pd.DataFrame({"m": list(range(1,13))})
    if not registros:
        out = full.copy()
        out["Total"] = 0.0
        out["Mês"] = out["m"].map(_MESES_PT_ABREV)
        return out[["Mês","Total"]]

    parc_df = pd.DataFrame(registros, columns=["m", "valor"])
    soma = parc_df.groupby("m", as_index=False)["valor"].sum()
    out = full.merge(soma, on="m", how="left").fillna({"valor": 0.0})
    out["Total"] = pd.to_numeric(out["valor"], errors="coerce").round(2)
    out["Mês"] = out["m"].map(_MESES_PT_ABREV)
    return out[["Mês","Total"]]

# --------- Monetárias / Percentuais (para a direita) ----------
def _infer_currency_cols(df: pd.DataFrame) -> List[str]:
    out: List[str] = []
    for c in df.columns:
        name = str(c); lc = name.lower()
        if ("parcelas" in lc and "valor" not in lc) or ("taxa" in lc and "valor" not in lc):
            continue
        if any(k in lc for k in ["valor", "preco", "preço", "principal", "saldo", "multa", "desconto", "montante", "em_aberto", "pago"]):
            try:
                s = pd.to_numeric(df[name], errors="coerce")
                if s.notna().any():
                    out.append(name)
            except Exception:
                pass
    seen = set()
    return [c for c in out if not (c in seen or seen.add(c))]

def _infer_percent_cols(df: pd.DataFrame) -> List[str]:
    out: List[str] = []
    for c in df.columns:
        lc = str(c).lower()
        if "juros" in lc or lc.startswith("taxa") or "percent" in lc:
            try:
                s = pd.to_numeric(df[c], errors="coerce")
                if s.notna().any():
                    out.append(str(c))
            except Exception:
                pass
    seen = set()
    return [c for c in out if not (c in seen or seen.add(c))]

# --------- Colunas duplicadas: tornar únicas sem mudar visual (zero-width) ---------
def _unique_cols_invisible(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    seen: dict[str, int] = {}
    new_cols: list[str] = []
    for c in list(df.columns):
        c_str = str(c)
        cnt = seen.get(c_str, 0)
        new_cols.append(c_str if cnt == 0 else (c_str + ("\u200B" * cnt)))
        seen[c_str] = cnt + 1
    out = df.copy()
    out.columns = new_cols
    return out

# ---------------- Página ----------------
def render(df_base: Optional[pd.DataFrame] = None, caminho_banco: str | None = None) -> None:
    """
    ESQUERDA: sempre usa CRONOGRAMA dos contratos (independe do CAP).
    DIREITA: SELECT * do banco (todas as colunas), com BRL/2 casas e juros %.
    """
    # --------- Carregar do banco para a TABELA COMPLETA (direita)
    db_path = _resolve_db_path(caminho_banco)
    if not db_path:
        st.error("Não foi possível localizar o banco de dados.")
        return
    con = _connect(db_path)
    if not con:
        return
    try:
        tabela = _pick_loans_object(con)
        if tabela is None:
            st.warning("Tabela/View de Empréstimos/Financiamentos não encontrada no banco.")
            return
        df_full = pd.read_sql_query(f'SELECT * FROM "{tabela}"', con)
        # (removido) st.caption da fonte
    except Exception as e:
        st.error(f"Falha ao ler do banco: {e}")
        return
    finally:
        try:
            con and con.close()
        except Exception:
            pass

    if not isinstance(df_full, pd.DataFrame) or df_full.empty:
        st.info("Nenhum empréstimo/financiamento encontrado (ou DataFrame vazio).")
        return

    # --------- Cabeçalho (ano) — agora calcula o total pelas PARCELAS do ano
    data_series = _best_date_series(df_full)
    cmap = {str(c).lower(): c for c in df_full.columns}
    if "valor_total" in cmap:
        valor_series = pd.to_numeric(df_full[cmap["valor_total"]], errors="coerce")
    elif "valor" in cmap:
        valor_series = pd.to_numeric(df_full[cmap["valor"]], errors="coerce")
    else:
        valor_series = pd.Series([0.0] * len(df_full))
    df_esq_header = pd.DataFrame({"Data": data_series, "Valor": valor_series})

    ano, df_ano = selecionar_ano(df_esq_header, key="empfin", label="Ano (Empréstimos/Financiamentos)")

    # Gera cronograma para o ano selecionado e usa a soma das parcelas como total do cabeçalho
    try:
        parcelas_df = _parcelas_calendar_from_contracts(df_full, int(ano))
    except Exception:
        resumo = resumo_por_mes(df_ano, valor_col="Valor")
        parcelas_df = resumo[["MesNome","Total"]].rename(columns={"MesNome":"Mês"})
        parcelas_df["Total"] = pd.to_numeric(parcelas_df["Total"], errors="coerce").round(2)

    total_ano = float(pd.to_numeric(parcelas_df["Total"], errors="coerce").sum())
    st.markdown(
        f"<div style='font-size:1.1rem;font-weight:700;margin:6px 0 10px;'>Ano selecionado: {ano} • Total gasto no ano selecionado: <span style='color:#00C853;'>{_fmt_moeda_str(total_ano)}</span></div>",
        unsafe_allow_html=True,
    )

    col_esq, col_dir = st.columns([0.36, 1.64])

    # --------- ESQUERDA: Parcelas por mês — usa o dataframe já calculado acima
    with col_esq:
        st.markdown(f"**Parcelas por mês — {ano}**")
        st.dataframe(
            _zebra(parcelas_df).format({"Total": _fmt_moeda_str}),
            use_container_width=True, hide_index=True,
            height=_auto_df_height(parcelas_df, row_px=34, header_px=44, pad_px=14, max_px=800)
        )

    # --------- DIREITA: TABELA COMPLETA — sem filtros, com formatações
    with col_dir:
        st.markdown("**Tabela de Empréstimos**")
        df_show = df_full.copy()
        df_show.columns = [str(c) for c in df_show.columns]
        df_show = _unique_cols_invisible(df_show)

        money_cols = _infer_currency_cols(df_show)
        percent_cols = _infer_percent_cols(df_show)

        for c in money_cols:
            df_show[c] = pd.to_numeric(df_show[c], errors="coerce").round(2)
        for c in percent_cols:
            df_show[c] = pd.to_numeric(df_show[c], errors="coerce").round(2)

        fmt_map = {c: _fmt_moeda_str for c in money_cols}
        fmt_map.update({c: _fmt_percent_str for c in percent_cols})

        st.dataframe(
            _zebra(df_show).format(fmt_map),
            use_container_width=True, hide_index=True,
            height=_auto_df_height(df_show, max_px=1200)
        )
