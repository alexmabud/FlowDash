# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
from datetime import date
from typing import Optional

import pandas as pd
import streamlit as st

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
        st.error("❌ Banco de dados não encontrado para Mercadorias.")
        st.caption(str(e))
        return None
    try:
        return sqlite3.connect(db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    except Exception as e:
        st.error("❌ Erro ao conectar no banco (Mercadorias).")
        st.exception(e)
        return None

# ================= Helpers =================
_MESES_PT_ABREV = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
}
_MESES_PT_NOME = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
    7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
}
_MESES_TOKEN_TO_NUM = {
    "jan":1, "janeiro":1, "fev":2, "fevereiro":2, "mar":3, "março":"3", "marco":"3",
    "abr":4, "abril":4, "mai":5, "maio":5, "jun":6, "junho":6, "jul":7, "julho":7,
    "ago":8, "agosto":8, "set":9, "setembro":9, "sep":9, "sept":9, "out":10, "outubro":10, "oct":10,
    "nov":11, "novembro":11, "dez":12, "dezembro":12,
}

_VALOR_CANDIDATAS = [
    "valor_mercadorias", "ValorMercadorias", "valorMercadorias",
    "valor_mercadoria", "ValorMercadoria",
    "valor", "Valor", "preco_total", "preço_total", "total",
]

def _fmt_moeda(v) -> str:
    try:
        return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v)

def _fmt_moeda_str(v) -> str:
    # alias para manter o mesmo nome utilizado em Entradas/Saídas
    return _fmt_moeda(v)

def _auto_df_height(df: pd.DataFrame, row_px: int = 30, header_px: int = 44, pad_px: int = 14, max_px: int = 10_000) -> int:
    n = int(len(df))
    h = header_px + (n * row_px) + pad_px
    return min(h, max_px)

def _height_exact_rows(n_rows: int) -> int:
    header_px = 38
    row_px = 34
    pad_px = 4
    return header_px + (n_rows * row_px) + pad_px

def _safe_to_datetime(s: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(s):
        return s
    return pd.to_datetime(s, errors="coerce")

def _zebra(df: pd.DataFrame, dark: str = "#12161d", light: str = "#1b212b") -> pd.io.formats.style.Styler:
    ncols = df.shape[1]
    def _row_style(row: pd.Series):
        bg = light if (row.name % 2) else dark
        return [f"background-color: {bg}"] * ncols
    return df.style.apply(_row_style, axis=1)

def _pick_valor_col(df: pd.DataFrame) -> Optional[str]:
    cols_lower = {c.lower(): c for c in df.columns}
    for k in _VALOR_CANDIDATAS:
        if k.lower() in cols_lower:
            return cols_lower[k.lower()]
    return None

def _ensure_valor(df_in: pd.DataFrame) -> pd.DataFrame:
    """Garante uma coluna 'Valor' numérica."""
    df = df_in.copy()
    if df.empty:
        return df
    cols = {c.lower(): c for c in df.columns}

    # Preferir o valor efetivamente recebido; se não houver, cair para o valor da compra
    col_val_rec = next((cols[n] for n in ["valor_recebido", "valorrecebido"] if n in cols), None)
    col_val_merc = next((cols[n] for n in ["valor_mercadoria", "valormercadoria", "valor_mercadorias", "valormercadorias"] if n in cols), None)
    if col_val_rec or col_val_merc:
        ser_rec = pd.to_numeric(df[col_val_rec], errors="coerce") if col_val_rec else None
        ser_merc = pd.to_numeric(df[col_val_merc], errors="coerce") if col_val_merc else None
        if ser_rec is not None and ser_merc is not None:
            prefer_rec = ser_rec.where((ser_rec.notna()) & (ser_rec != 0))
            df["Valor"] = prefer_rec.fillna(ser_merc.fillna(0.0)).fillna(0.0)
        elif ser_rec is not None:
            df["Valor"] = ser_rec.fillna(0.0)
        else:
            df["Valor"] = ser_merc.fillna(0.0)
        return df

    valor_col = _pick_valor_col(df)
    if valor_col is None:
        preco_col = next((cols[n] for n in ["preco", "preço", "valor_unit", "vl_unit", "unitario", "unit_price"] if n in cols), None)
        qtd_col   = next((cols[n] for n in ["quantidade", "qtd", "qtde", "qte", "qty"] if n in cols), None)
        if preco_col and qtd_col:
            df["Valor"] = pd.to_numeric(df[preco_col], errors="coerce").fillna(0.0) * \
                          pd.to_numeric(df[qtd_col],   errors="coerce").fillna(0.0)
        else:
            df["Valor"] = 0.0
    else:
        if valor_col != "Valor":
            df = df.rename(columns={valor_col: "Valor"})
        df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)
    return df

def _to_date_str(s: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(s):
        return s.dt.date.astype(str)
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    ser_num = pd.to_numeric(s, errors="coerce")
    mask_num = ser_num.notna()
    if mask_num.any():
        base = pd.Timestamp("1899-12-30")  # base Excel
        out.loc[mask_num] = base + pd.to_timedelta(ser_num.loc[mask_num], unit="D")
    mask_rest = out.isna()
    if mask_rest.any():
        try:
            out.loc[mask_rest] = pd.to_datetime(s.loc[mask_rest], errors="coerce", format="mixed")
        except TypeError:
            out.loc[mask_rest] = pd.to_datetime(s.loc[mask_rest], errors="coerce", infer_datetime_format=False)
    return out.dt.date.astype(str)

def _month_name_pt_from_any(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    try:
        dt = pd.to_datetime(x, errors="coerce")
        if pd.notna(dt):
            m = int(dt.month)
            return _MESES_PT_NOME.get(m, "")
    except Exception:
        pass
    try:
        n = int(float(x))
        if 1 <= n <= 12:
            return _MESES_PT_NOME[n]
    except Exception:
        pass
    try:
        token = str(x).strip().lower().replace("ç", "c")
        m = _MESES_TOKEN_TO_NUM.get(token)
        if m:
            m = int(m)
            return _MESES_PT_NOME.get(m, "")
    except Exception:
        pass
    return ""

def _to_month_name_pt_series(s: pd.Series) -> pd.Series:
    return s.apply(_month_name_pt_from_any)

# ---------------- Carregamento / SQL ----------------
def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    try:
        cur = conn.execute(f"PRAGMA table_info('{table}')")
        cols = [r[1] for r in cur.fetchall()]
        return any(c.lower() == column.lower() for c in cols)
    except Exception:
        return False

def _load_full_table(conn: sqlite3.Connection) -> Optional[pd.DataFrame]:
    try:
        tbls = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' AND name='mercadorias'", conn)
        if tbls.empty:
            return None
        df_db = pd.read_sql('SELECT * FROM "mercadorias";', conn)
        return df_db if not df_db.empty else None
    except Exception:
        return None

def _load_month(conn: sqlite3.Connection, ano: int, mes: int) -> Optional[pd.DataFrame]:
    """SELECT * por mês usando Recebimento (ou Data, se não existir)."""
    df_all = _load_full_table(conn)
    if df_all is None:
        return None

    col_base = None
    for cand in ("Recebimento", "recebimento"):
        if _has_column(conn, "mercadorias", cand):
            col_base = cand
            break
    if not col_base:
        for cand in ("Data", "data"):
            if _has_column(conn, "mercadorias", cand):
                col_base = cand
                break
    if not col_base:
        return df_all

    first_day = pd.Timestamp(year=ano, month=mes, day=1).date()
    last_day  = (pd.Timestamp(year=ano, month=mes, day=1) + pd.offsets.MonthEnd(1)).date()
    q = f"""
        SELECT *
        FROM "mercadorias"
        WHERE date("{col_base}") BETWEEN ? AND ?
        ORDER BY datetime("{col_base}") ASC
    """
    try:
        return pd.read_sql_query(q, conn, params=(str(first_day), str(last_day)))
    except Exception:
        df = df_all.copy()
        if col_base in df.columns:
            df[col_base] = pd.to_datetime(df[col_base], errors="coerce")
            mask = (df[col_base].dt.date >= first_day) & (df[col_base].dt.date <= last_day)
            df = df.loc[mask]
        return df

# ================= Página =================
def render(df_merc: pd.DataFrame) -> None:
    """
    📦 Mercadorias — 2 colunas:
      • Esquerda (1/4): Total por mês no ano (Jan..Dez sem scroll)
      • Direita  (3/4): Tabela completa do mês (todas as colunas)
      • Base de data: **Recebimento** (padrão); se não houver, cai para Data.
    """
    if not isinstance(df_merc, pd.DataFrame) or df_merc.empty:
        st.info("Nenhuma mercadoria encontrada (ou DataFrame inválido/vazio).")
        return

    # ====== Determina ano/mês a partir de Recebimento (ou Data) ======
    col_data_pref = "Recebimento" if "Recebimento" in df_merc.columns else ("Data" if "Data" in df_merc.columns else None)
    if not col_data_pref:
        st.warning("Nem 'Recebimento' nem 'Data' encontrados na amostra de Mercadorias.")
        return

    dt = _safe_to_datetime(df_merc[col_data_pref])
    anos_disponiveis = sorted([int(a) for a in dt.dropna().dt.year.unique()])
    if not anos_disponiveis:
        st.warning("Sem anos disponíveis em Mercadorias.")
        return

    hoje = date.today()
    ano_default = hoje.year if hoje.year in anos_disponiveis else anos_disponiveis[-1]
    ano = st.selectbox("Ano (Mercadorias)", options=anos_disponiveis, index=anos_disponiveis.index(ano_default), key="merc_ano")

    # Totais por mês (usando amostra; só para quadro da esquerda)
    dt_ano = dt[dt.dt.year == ano]
    base_mes = (
        df_merc.loc[dt.dt.year == ano]
        .assign(_Mes=dt_ano.dt.month)
        .pipe(_ensure_valor)
        .groupby("_Mes", dropna=True)["Valor"].sum()
        .reindex(range(1, 13), fill_value=0.0)
    )

    # ====== Banner com mesmo estilo de Entradas/Saídas ======
    total_ano = float(base_mes.sum())
    st.markdown(
        f"""
        <div style="font-size:1.25rem;font-weight:700;margin:6px 0 10px;">
            Ano selecionado: {ano} • Total no ano:
            <span style="color:#00C853;">{_fmt_moeda_str(total_ano)}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    meses_com_dado = [m for m in range(1, 13) if float(base_mes.iloc[m-1]) > 0]
    mes_default = hoje.month if (ano == hoje.year and float(base_mes.iloc[hoje.month-1]) > 0) else (meses_com_dado[0] if meses_com_dado else 1)

    # Botões de meses
    st.caption("Escolha um mês")
    mes_sel = st.session_state.get("merc_mes_sel", mes_default)
    for linha in (0, 1):
        cols = st.columns(6)
        for i, col in enumerate(cols, start=1):
            m = linha * 6 + i
            label = _MESES_PT_ABREV[m]
            has_data = float(base_mes.iloc[m-1]) > 0.0
            clicked = col.button(label, key=f"btn_merc_{ano}_{m}", use_container_width=True, disabled=not has_data)
            if clicked:
                mes_sel = m
                st.session_state["merc_mes_sel"] = mes_sel

    # ====== LAYOUT LADO A LADO (1/4 x 3/4) ======
    col_esq, col_dir = st.columns([1, 3])

    # ESQUERDA — Total por mês
    with col_esq:
        st.markdown(
            f"**Total por mês no ano** <span style='color:#60a5fa;'>{ano}</span>",
            unsafe_allow_html=True,
        )
        tabela_mes = pd.DataFrame({
            "Mês": [_MESES_PT_ABREV[m] for m in range(1, 13)],
            "Total": [float(base_mes.iloc[m-1]) for m in range(1, 13)],
        })
        st.dataframe(
            _zebra(tabela_mes).format({"Total": _fmt_moeda_str}),
            use_container_width=True,
            hide_index=True,
            height=_height_exact_rows(12),
        )

    # DIREITA — Tabela completa do mês
    with col_dir:
        st.markdown(
            f"**Mercadorias do mês** <span style='color:#60a5fa;'>{_MESES_PT_ABREV.get(mes_sel, '—')}</span>",
            unsafe_allow_html=True,
        )

        db_path = _resolve_db_path(None)
        if not db_path:
            st.error("Não foi possível localizar o banco de dados.")
            return

        con = _connect(db_path)
        if not con:
            return

        try:
            df_full = _load_month(con, ano, mes_sel)
        except Exception as e:
            st.error("Erro ao carregar Mercadorias do mês.")
            st.exception(e)
            try:
                con.close()
            except Exception:
                pass
            return
        finally:
            try:
                con.close()
            except Exception:
                pass

        if df_full is None or df_full.empty:
            st.caption("Selecione um mês com dados para visualizar a tabela completa.")
            st.dataframe(pd.DataFrame(), use_container_width=True, hide_index=True, height=180)
            return

        # Datas legíveis
        for col in ("Data", "Recebimento", "Faturamento"):
            if col in df_full.columns:
                df_full[col] = _to_date_str(df_full[col])

        # Previsões
        if "Previsao_Recebimento" in df_full.columns:
            df_full["Previsao_Recebimento"] = _to_month_name_pt_series(df_full["Previsao_Recebimento"])
        if "Previsao_Faturamento" in df_full.columns:
            df_full["Previsao_Faturamento"] = _to_date_str(df_full["Previsao_Faturamento"])

        # Formatações monetárias usuais
        fmt_map: dict[str, any] = {}
        for cand in ("Valor", "valor", "Frete", "frete", "Faturamento", "faturamento", "Valor_Recebido", "Frete_Cobrado"):
            if cand in df_full.columns:
                fmt_map[cand] = _fmt_moeda_str
        cols_lower = {c.lower(): c for c in df_full.columns}
        for k in _VALOR_CANDIDATAS:
            c = cols_lower.get(k.lower())
            if c:
                fmt_map[c] = _fmt_moeda_str

        st.dataframe(
            _zebra(df_full).format(fmt_map) if fmt_map else _zebra(df_full),
            use_container_width=True,
            hide_index=True,
            height=_auto_df_height(df_full, max_px=1200),
        )
