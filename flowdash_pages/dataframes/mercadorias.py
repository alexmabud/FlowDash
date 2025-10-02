# flowdash_pages/dataframes/mercadorias.py
from __future__ import annotations

import os
import sqlite3
import pandas as pd
import streamlit as st
from datetime import date

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
    "jan":1, "janeiro":1,
    "fev":2, "fevereiro":2,
    "mar":3, "março":"3", "marco":"3",
    "abr":4, "abril":4,
    "mai":5, "maio":5,
    "jun":6, "junho":6,
    "jul":7, "julho":7,
    "ago":8, "agosto":8,
    "set":9, "setembro":9, "sep":9, "sept":9,
    "out":10, "outubro":10, "oct":10,
    "nov":11, "novembro":11,
    "dez":12, "dezembro":12,
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

def _auto_df_height(
    df: pd.DataFrame,
    row_px: int = 30,
    header_px: int = 44,
    pad_px: int = 14,
    max_px: int = 10_000
) -> int:
    n = int(len(df))
    h = header_px + (n * row_px) + pad_px
    return min(h, max_px)

def _height_exact_rows(n_rows: int) -> int:
    """Altura exata para exibir n_rows sem scroll."""
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

def _pick_valor_col(df: pd.DataFrame) -> str | None:
    cols_lower = {c.lower(): c for c in df.columns}
    for k in _VALOR_CANDIDATAS:
        if k.lower() in cols_lower:
            return cols_lower[k.lower()]
    return None

def _ensure_valor(df_in: pd.DataFrame) -> pd.DataFrame:
    """Garante uma coluna 'Valor' numérica (sem criar outras colunas auxiliares)."""
    df = df_in.copy()
    if df.empty:
        return df
    cols = {c.lower(): c for c in df.columns}
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

# --- Datas: conversão robusta ---
def _to_date_str(s: pd.Series) -> pd.Series:
    """Converte série para string de data (YYYY-MM-DD) sem warnings."""
    if pd.api.types.is_datetime64_any_dtype(s):
        return s.dt.date.astype(str)

    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

    # 1) números (ex.: serial Excel)
    ser_num = pd.to_numeric(s, errors="coerce")
    mask_num = ser_num.notna()
    if mask_num.any():
        base = pd.Timestamp("1899-12-30")  # base Excel
        out.loc[mask_num] = base + pd.to_timedelta(ser_num.loc[mask_num], unit="D")

    # 2) restante: strings/mistos
    mask_rest = out.isna()
    if mask_rest.any():
        try:
            out.loc[mask_rest] = pd.to_datetime(s.loc[mask_rest], errors="coerce", format="mixed")
        except TypeError:
            out.loc[mask_rest] = pd.to_datetime(s.loc[mask_rest], errors="coerce", infer_datetime_format=False)

    return out.dt.date.astype(str)

def _month_name_pt_from_any(x) -> str:
    """Recebe data/numero/texto e retorna nome do mês em PT (ex: 'Julho'); vazio se não conseguir."""
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

# --- Descoberta do caminho do banco ---
def _get_db_path() -> str | None:
    cand = st.session_state.get("caminho_banco")
    if isinstance(cand, str) and os.path.exists(cand):
        return cand
    try:
        from shared import db as sdb  # type: ignore
        for name in ("get_db_path", "db_path", "DB_PATH"):
            if hasattr(sdb, name):
                obj = getattr(sdb, name)
                p = obj() if callable(obj) else obj
                if isinstance(p, str) and os.path.exists(p):
                    return p
    except Exception:
        pass
    try:
        p = st.secrets.get("DB_PATH", None)  # type: ignore[attr-defined]
        if isinstance(p, str) and os.path.exists(p):
            return p
    except Exception:
        pass
    for g in ("./data/flowdash_data.db", "data/flowdash_data.db", "./data/dashboard_rc.db", "data/dashboard_rc.db"):
        if os.path.exists(g):
            return g
    return None

def _load_full_table_if_possible(df_hint: pd.DataFrame) -> pd.DataFrame:
    """Tenta carregar SELECT * FROM mercadorias; se falhar, devolve df_hint."""
    db_path = _get_db_path()
    if not db_path:
        return df_hint
    try:
        with sqlite3.connect(db_path) as conn:
            tbls = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' AND name='mercadorias'", conn)
            if tbls.empty:
                return df_hint
            df_db = pd.read_sql("SELECT * FROM mercadorias", conn)
            if not df_db.empty:
                return df_db
    except Exception:
        return df_hint
    return df_hint

# ================= Página =================
def render(df_merc: pd.DataFrame) -> None:
    """
    📦 Mercadorias — agora em 2 colunas:
      • Esquerda (1/4): Total por mês no ano selecionado (Jan..Dez sem scroll)
      • Direita  (3/4): Tabela completa do mês selecionado (todas as colunas)
      • Abre por padrão no mês corrente, se houver dados no ano; senão, no 1º mês com dados.
    """
    if not isinstance(df_merc, pd.DataFrame) or df_merc.empty:
        st.info("Nenhuma mercadoria encontrada (ou DataFrame inválido/vazio).")
        return

    # Carrega TODAS as colunas, se possível
    df_merc = _load_full_table_if_possible(df_merc)

    # Garante coluna Valor
    df = _ensure_valor(df_merc)

    # ---------- BASE DE DATA: RECEBIMENTO ----------
    if "Recebimento" not in df.columns:
        st.warning("A coluna 'Recebimento' não foi encontrada em Mercadorias.")
        return

    dt = _safe_to_datetime(df["Recebimento"])

    # ----- seletor de ANO (derivado de Recebimento) -----
    anos_disponiveis = sorted([int(a) for a in dt.dropna().dt.year.unique()])
    if not anos_disponiveis:
        st.warning("Sem anos disponíveis em 'Recebimento'.")
        return

    ano_default = int(date.today().year) if int(date.today().year) in anos_disponiveis else int(anos_disponiveis[-1])
    ano = st.selectbox("Ano (Mercadorias)", options=anos_disponiveis, index=anos_disponiveis.index(ano_default), key="merc_ano")

    mask_ano = dt.dt.year == ano
    df_ano = df.loc[mask_ano].copy()
    dt_ano = dt.loc[mask_ano]

    # ----- botões de MESES (por Recebimento) -----
    meses = dt_ano.dt.month
    totais_por_mes = (
        df_ano.groupby(meses)["Valor"]
        .sum()
        .reindex(range(1, 13), fill_value=0.0)
    )

    meses_com_dado = [m for m in range(1, 13) if float(totais_por_mes.iloc[m-1]) > 0]
    hoje = date.today()
    mes_default = (
        hoje.month if (hoje.year == ano and float(totais_por_mes.iloc[hoje.month-1]) > 0)
        else (meses_com_dado[0] if meses_com_dado else 1)
    )

    sel_key = "merc_mes_sel"
    mes_sel = st.session_state.get(sel_key, mes_default)
    if mes_sel not in range(1, 13):
        mes_sel = mes_default
        st.session_state[sel_key] = mes_sel

    st.caption("Escolha um mês")
    for linha in (0, 1):
        cols = st.columns(6)
        for i, col in enumerate(cols, start=1):
            mes = linha * 6 + i
            label = f"{_MESES_PT_ABREV[mes]}"
            has_data = float(totais_por_mes.iloc[mes-1]) > 0.0
            clicked = col.button(label, key=f"btn_merc_{ano}_{mes}", use_container_width=True, disabled=not has_data)
            if clicked:
                st.session_state[sel_key] = mes
                mes_sel = mes

    # ----- Total do mês selecionado (por Recebimento) -----
    mask_mes = (dt_ano.dt.month == mes_sel)
    df_mes = df_ano.loc[mask_mes].copy()
    total_mes = float(df_mes["Valor"].sum())

    # ====== LAYOUT LADO A LADO (1/4 x 3/4) ======
    col_esq, col_dir = st.columns([1, 3])

    # ====== ESQUERDA — TABELA 1: Total por mês (ano selecionado) ======
    with col_esq:
        st.markdown(
            f"**Total por mês no ano** "
            f"<span style='color:#60a5fa;'>{ano}</span>",
            unsafe_allow_html=True,
        )

        tabela_mes = pd.DataFrame({
            "Mês": [_MESES_PT_ABREV[m] for m in range(1, 13)],
            "Total": [float(totais_por_mes.get(m, 0.0)) for m in range(1, 13)],
        })
        # 12 linhas sem scroll
        h_mes = _height_exact_rows(12)
        st.dataframe(
            _zebra(tabela_mes).format({"Total": _fmt_moeda}),
            use_container_width=True,
            hide_index=True,
            height=h_mes,
        )

    # ====== DIREITA — TABELA 2: Mercadorias do mês selecionado (todas as colunas) ======
    with col_dir:
        st.markdown(
            f"**Mercadorias do mês** "
            f"<span style='color:#60a5fa;'>{_MESES_PT_ABREV.get(mes_sel, '—')}</span> "
            f"— Total: <span style='color:#00C853;'>{_fmt_moeda(total_mes)}</span>",
            unsafe_allow_html=True,
        )

        if df_mes.empty:
            st.caption("Selecione um mês com dados para visualizar a tabela completa.")
            st.dataframe(pd.DataFrame(), use_container_width=True, hide_index=True, height=180)
        else:
            df_full = df_mes.copy()

            # Datas legíveis
            for col in ("Data", "Recebimento", "Faturamento"):
                if col in df_full.columns:
                    df_full[col] = _to_date_str(df_full[col])

            # 'Previsao_Recebimento' em mês por extenso (PT)
            if "Previsao_Recebimento" in df_full.columns:
                df_full["Previsao_Recebimento"] = _to_month_name_pt_series(df_full["Previsao_Recebimento"])

            if "Previsao_Faturamento" in df_full.columns:
                df_full["Previsao_Faturamento"] = _to_date_str(df_full["Previsao_Faturamento"])

            fmt_map: dict[str, any] = {}
            for cand in ("Valor", "frete", "Frete", "faturamento", "Faturamento", "recebimento", "Recebimento", "Valor_Recebido", "Frete_Cobrado"):
                if cand in df_full.columns:
                    fmt_map[cand] = _fmt_moeda

            h_full = _auto_df_height(df_full, max_px=1200)
            st.dataframe(
                _zebra(df_full).format(fmt_map) if fmt_map else _zebra(df_full),
                use_container_width=True,
                hide_index=True,
                height=h_full,
            )
