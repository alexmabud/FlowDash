# flowdash_pages/dataframes/entradas.py
from __future__ import annotations
import os
import sqlite3
import pandas as pd
import streamlit as st
from datetime import date  # << adicionado

from flowdash_pages.dataframes.filtros import (
    selecionar_ano,
    selecionar_mes,
    resumo_por_mes,
)

# ---------------- Helpers ----------------
def _auto_df_height(df: pd.DataFrame, row_px: int = 30, header_px: int = 36, pad_px: int = 6, max_px: int = 1200) -> int:
    """Altura aproximada (linhas compactas)."""
    n = int(len(df))
    h = header_px + (n * row_px) + pad_px
    return min(h, max_px)

def _safe_to_datetime(s: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(s):
        return s
    return pd.to_datetime(s, errors="coerce")

def _fmt_moeda_str(v) -> str:
    try:
        return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v)

def _fmt_int_str(v) -> str:
    """Formata como inteiro (sem vírgula). Em branco para valores nulos/invalidos."""
    try:
        if pd.isna(v):
            return ""
        return str(int(float(v)))
    except Exception:
        try:
            return str(int(str(v).strip()))
        except Exception:
            return str(v)

def _zebra(df: pd.DataFrame, dark: str = "#12161d", light: str = "#1b212b") -> pd.io.formats.style.Styler:
    ncols = df.shape[1]
    def _row_style(row: pd.Series):
        bg = light if (row.name % 2) else dark
        return [f"background-color: {bg}"] * ncols
    return df.style.apply(_row_style, axis=1)

def _discover_db_path(user_path: str | None = None) -> str | None:
    candidates = []
    if user_path:
        candidates.append(user_path)
    for k in ("caminho_banco", "db_path", "caminho_db", "_effective_path"):
        v = st.session_state.get(k)
        if isinstance(v, str):
            candidates.append(v)
    candidates += [
        "data/flowdash_data.db",
        "dashboard_rc.db",
        os.path.join("data", "dashboard_rc.db"),
        os.path.join("FlowDash", "data", "flowdash_data.db"),
    ]
    for p in candidates:
        try:
            if p and os.path.exists(p):
                return p
        except Exception:
            pass
    return None

def _detect_entrada_table_name(con: sqlite3.Connection) -> str:
    cur = con.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    names = {row[0].lower(): row[0] for row in cur.fetchall()}
    for candidate in ("entrada", "entradas"):
        if candidate in names:
            return names[candidate]
    for key, original in names.items():
        if "entrada" in key:
            return original
    raise RuntimeError("Tabela de entradas não encontrada (ex.: 'entrada' ou 'entradas').")

def _reorder_cols(df: pd.DataFrame, before_col_ci: str, target_before_ci: str) -> pd.DataFrame:
    cols_map = {c.lower(): c for c in df.columns}
    if before_col_ci.lower() not in cols_map or target_before_ci.lower() not in cols_map:
        return df
    before = cols_map[before_col_ci.lower()]
    target = cols_map[target_before_ci.lower()]
    if before == target:
        return df
    cols = list(df.columns)
    cols.remove(before)
    cols.insert(cols.index(target), before)
    return df[cols]

# Mapeamento p/ títulos
_MESES_PT_ABREV = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
}

# ---------------- Página ----------------
def render(df_entrada: pd.DataFrame, caminho_banco: str | None = None) -> None:
    """
    Visão simples:
      - Tabelas nativas do Streamlit (st.dataframe), índice oculto.
      - 1ª tabela (Faturamento por mês) SEM SCROLL (todas as linhas).
      - 2ª tabela com a MESMA ALTURA da 1ª (usa scroll quando precisar).
      - Tabela completa do mês lida via SELECT * (todas as colunas).
    """
    if not isinstance(df_entrada, pd.DataFrame) or df_entrada.empty:
        st.info("Nenhuma entrada encontrada (ou DataFrame inválido/vazio).")
        return

    # 1) Filtro por Ano
    ano, df_ano = selecionar_ano(df_entrada, key="entradas", label="Ano (Entradas)")
    if df_ano.empty:
        st.warning("Não há dados para o ano selecionado.")
        return

    # Cabeçalho com total do ano
    total_ano = float(pd.to_numeric(df_ano["Valor"], errors="coerce").sum())
    st.markdown(
        f"""
        <div style="font-size:1.25rem;font-weight:700;margin:6px 0 10px;">
            Ano selecionado: {ano} • Total no ano:
            <span style="color:#00C853;">{_fmt_moeda_str(total_ano)}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 2) Meses
    mes, df_mes = selecionar_mes(df_ano, key="entradas", label="Escolha um mês")

    # Auto-seleção mês atual (se houver dados); senão 1º mês com dados
    if ("Data" in df_ano.columns) and (mes is None or df_mes.empty):
        dt_all = _safe_to_datetime(df_ano["Data"])
        meses_com_dado = sorted(dt_all.dt.month.dropna().unique().tolist())
        hoje = date.today()
        mes_pref = None
        try:
            ano_int = int(ano)
        except Exception:
            ano_int = hoje.year
        if ano_int == hoje.year and hoje.month in meses_com_dado:
            mes_pref = hoje.month
        elif meses_com_dado:
            mes_pref = int(meses_com_dado[0])
        if mes_pref is not None:
            mes = mes_pref
            df_mes = df_ano[dt_all.dt.month == mes].copy()

    # Para títulos
    mes_nome = _MESES_PT_ABREV.get(int(mes), "—") if mes is not None else "—"
    total_mes = float(pd.to_numeric(df_mes.get("Valor", 0), errors="coerce").sum()) if mes is not None else 0.0

    # 3) Duas colunas
    col_esq, col_dir = st.columns(2)

    # 3.1) ESQUERDA — Faturamento por mês (SEM SCROLL)
    with col_esq:
        st.markdown(
            f"**Faturamento por mês no ano** "
            f"<span style='color:#60a5fa;'>{ano}</span>",
            unsafe_allow_html=True,
        )
        resumo = resumo_por_mes(df_ano, valor_col="Valor")  # Mes, MesNome, Total
        tabela_mes = resumo[["MesNome", "Total"]].rename(columns={"MesNome": "Mês"})
        tabela_mes["Total"] = pd.to_numeric(tabela_mes["Total"], errors="coerce").fillna(0.0)

        altura_esq = _auto_df_height(tabela_mes, row_px=34, header_px=44, pad_px=14, max_px=10_000)
        st.dataframe(
            _zebra(tabela_mes).format({"Total": _fmt_moeda_str}),
            use_container_width=True,
            hide_index=True,
            height=altura_esq,
        )

    # 3.2) DIREITA — Detalhe diário do mês (MESMA ALTURA DA 1ª)
    with col_dir:
        st.markdown(
            f"**Detalhe diário do mês** "
            f"<span style='color:#60a5fa;'>{mes_nome}</span> "
            f"— Total: <span style='color:#00C853;'>{_fmt_moeda_str(total_mes)}</span>",
            unsafe_allow_html=True,
        )
        if mes is None or df_mes.empty:
            detalhado = pd.DataFrame(columns=["Dia", "Total"])
        else:
            df_dia = df_mes.copy()
            if not pd.api.types.is_datetime64_any_dtype(df_dia["Data"]):
                df_dia["Data"] = _safe_to_datetime(df_dia["Data"])
            df_dia["Dia"] = df_dia["Data"].dt.date
            detalhado = (
                df_dia.groupby("Dia", dropna=True)["Valor"]
                .sum()
                .reset_index()
                .sort_values("Dia")
                .rename(columns={"Valor": "Total"})
            )
        detalhado["Total"] = pd.to_numeric(detalhado.get("Total", 0), errors="coerce").fillna(0.0)

        st.dataframe(
            _zebra(detalhado[["Dia", "Total"]]).format({"Total": _fmt_moeda_str}),
            use_container_width=True,
            hide_index=True,
            height=altura_esq,
        )

    # 4) Tabela completa do mês (SELECT *)
    st.divider()
    st.markdown("**Entradas do mês selecionado — Tabela completa**")

    if mes is None:
        st.info("Selecione um mês para visualizar a tabela completa.")
        return

    db_path = _discover_db_path(caminho_banco)
    if not db_path:
        st.error("Não foi possível localizar o banco de dados automaticamente. Informe 'caminho_banco'.")
        return

    ano_int, mes_int = int(ano), int(mes)
    first_day = pd.Timestamp(year=ano_int, month=mes_int, day=1).date()
    last_day  = (pd.Timestamp(year=ano_int, month=mes_int, day=1) + pd.offsets.MonthEnd(1)).date()

    try:
        con = sqlite3.connect(db_path)
        tabela = _detect_entrada_table_name(con)
        query = f"""
            SELECT *
            FROM {tabela}
            WHERE date(Data) BETWEEN ? AND ?
            ORDER BY datetime(Data) ASC
        """
        df_full = pd.read_sql_query(query, con, params=(str(first_day), str(last_day)))
    except Exception as e:
        st.error(f"Falha ao ler do banco '{db_path}': {e}")
        try:
            con and con.close()
        except Exception:
            pass
        return
    finally:
        try:
            con.close()
        except Exception:
            pass

    # --- Formatações mínimas ---
    cmap = {c.lower(): c for c in df_full.columns}

    if "data" in cmap:
        c = cmap["data"]
        try:
            df_full[c] = pd.to_datetime(df_full[c], errors="coerce").dt.date.astype(str)
        except Exception:
            pass

    for key in ("data_liq", "data_liquido", "data_liquidacao"):
        if key in cmap:
            df_full = df_full.drop(columns=[cmap[key]])
            break

    df_full = _reorder_cols(df_full, before_col_ci="maquineta", target_before_ci="usuario")

    fmt_map: dict[str, any] = {}
    if "valor" in cmap:
        fmt_map[cmap["valor"]] = _fmt_moeda_str
    for key in ("valor_liquido", "valorliquido", "valor_liq", "valorliq"):
        if key in cmap:
            fmt_map[cmap[key]] = _fmt_moeda_str
            break

    for key in ("parcela", "parcelas", "num_parcelas"):
        if key in cmap:
            fmt_map[cmap[key]] = _fmt_int_str
            break

    styled_full = _zebra(df_full).format(fmt_map) if fmt_map else _zebra(df_full)

    altura_full = _auto_df_height(df_full, max_px=1200)
    st.dataframe(
        styled_full,
        use_container_width=True,
        hide_index=True,
        height=altura_full,
    )
