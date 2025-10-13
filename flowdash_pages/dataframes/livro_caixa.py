# -*- coding: utf-8 -*-
# flowdash_pages/dataframes/livro_caixa.py
from __future__ import annotations

import os
import sqlite3
from typing import Optional, Tuple, List
from datetime import date

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

def _resolve_db_path(pref: Optional[str]) -> str:
    if isinstance(pref, str) and os.path.exists(pref):
        return pref
    if callable(_shared_get_db_path):
        try:
            p = _shared_get_db_path()
            if isinstance(p, str) and os.path.exists(p):
                return p
        except Exception:
            pass
    return ensure_db_path_or_raise(None)

# ================= Helpers =================
def _load_livro_caixa(conn: sqlite3.Connection) -> Tuple[pd.DataFrame, str]:
    table_candidates = ["movimentacoes_bancarias", "movimentaceos_bancarias"]
    last_err = None
    for t in table_candidates:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {t}", conn)
            return df, t
        except Exception as e:
            last_err = e
    raise RuntimeError(
        "Não foi possível ler a tabela de movimentações bancárias. "
        "Tente criar/validar a tabela 'movimentacoes_bancarias'. "
        f"Erro original: {last_err}"
    )

def _infer_date_column(df: pd.DataFrame) -> Optional[str]:
    candidates: List[str] = [
        "data", "data_mov", "data_movimento", "dt", "dt_mov", "competencia", "created_at", "evento_data"
    ]
    lower_cols = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in lower_cols:
            return lower_cols[c]
    for col in df.columns:
        lc = col.lower()
        if "data" in lc or lc.startswith("dt"):
            return col
    return None

def _coerce_datetime(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    out = df.copy()
    out["_dt"] = pd.to_datetime(out[date_col], errors="coerce")
    return out

def _infer_ref_col(df: pd.DataFrame) -> Optional[str]:
    """
    Detecta a coluna que indica se é entrada/saida ou a origem (ex.: contas_a_pagar_mov).
    Preferência: referencia_tabela -> tipo -> tipo_mov -> referencia -> origem
    (robusto a acentos/maiúsculas/espaços)
    """
    def _norm(s: str) -> str:
        return (
            s.lower()
             .replace("á","a").replace("à","a").replace("ã","a").replace("â","a")
             .replace("é","e").replace("ê","e").replace("í","i")
             .replace("ó","o").replace("ô","o").replace("õ","o")
             .replace("ú","u").replace("ç","c")
             .replace(" ", "_")
        )
    targets = ["referencia_tabela", "tipo", "tipo_mov", "referencia", "origem"]
    norm_map = {_norm(c): c for c in df.columns}
    for t in targets:
        if t in norm_map:
            return norm_map[t]
    for k, v in norm_map.items():
        if "referencia" in k:  # fallback: qualquer variação com 'referencia'
            return v
    return None

def _style_row_from_value(val: str, ncols: int) -> List[str]:
    """Retorna a lista de estilos para a linha, dado o valor normalizado da referência."""
    v = (val or "").strip().lower()
    if v == "entrada":
        style = "background-color: rgba(34,197,94,.12); color: #16a34a; font-weight: 600;"
        return [style]*ncols
    if v == "saida":
        style = "background-color: rgba(220,53,69,.12); color: #dc3545; font-weight: 600;"
        return [style]*ncols
    if v in {"contas_a_pagar_mov", "contas_a_pagar"}:
        style = "background-color: rgba(236,72,153,.18); color: #db2777; font-weight: 600;"
        return [style]*ncols
    if v in {"saldos_bancos", "saldos_bancarios", "saldos_bancários", "saldos_caixa", "saldos_caixas"}:
        style = "background-color: rgba(245,158,11,.18); color: #d97706; font-weight: 600;"
        return [style]*ncols
    if v in {"movimentacoes_bancarias", "movimentações_bancárias", "transferencias", "transferências", "transferencia", "transferência"}:
        style = "background-color: rgba(59,130,246,.18); color: #2563eb; font-weight: 600;"
        return [style]*ncols
    if v == "correcao_caixa":
        style = "background-color: rgba(139,92,246,.18); color: #7c3aed; font-weight: 600;"
        return [style]*ncols
    return [""]*ncols

def _infer_valor_col(df: pd.DataFrame) -> Optional[str]:
    candidates = ["valor", "amount", "valor_total", "valor_liquido"]
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in lower:
            return lower[c]
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            return c
    return None

def _fmt_moeda(v) -> str:
    try:
        x = float(v)
    except Exception:
        x = 0.0
    s = f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"

def _legend_html() -> str:
    return """
    <div style="margin: .25rem 0 1rem 0; display:flex; flex-wrap:wrap; gap:.5rem;">
      <span style="padding:.25rem .5rem; border-radius:999px; font-weight:600; background:rgba(34,197,94,.12); color:#16a34a;">Entrada</span>
      <span style="padding:.25rem .5rem; border-radius:999px; font-weight:600; background:rgba(220,53,69,.12); color:#dc3545;">Saídas</span>
      <span style="padding:.25rem .5rem; border-radius:999px; font-weight:600; background:rgba(236,72,153,.18); color:#db2777;">Obrigações futuras</span>
      <span style="padding:.25rem .5rem; border-radius:999px; font-weight:600; background:rgba(245,158,11,.18); color:#d97706;">Cadastro Saldos Banco/Caixa</span>
      <span style="padding:.25rem .5rem; border-radius:999px; font-weight:600; background:rgba(59,130,246,.18); color:#2563eb;">Transferência p/ caixa 2 e entre bancos</span>
      <span style="padding:.25rem .5rem; border-radius:999px; font-weight:600; background:rgba(139,92,246,.18); color:#7c3aed;">Correção de caixa</span>
    </div>
    """

# ================= Página =================
def render(db_path_pref: Optional[str] = None) -> None:
    """
    Página: Livro Caixa
    Exibe APENAS as colunas: data_hora, valor, observacao, banco, usuario.
    Colore a linha com base em 'referencia_tabela' (ou variação), sem exibir essa coluna.
    """
    st.title("📘 Livro Caixa")

    # Descoberta de banco
    try:
        db_path = _resolve_db_path(db_path_pref)
    except Exception as e:
        st.error(f"Erro ao localizar o banco de dados: {e}")
        return

    # Carrega dados
    try:
        with sqlite3.connect(db_path) as conn:
            df, table_used = _load_livro_caixa(conn)
    except Exception as e:
        st.error(str(e))
        return

    st.caption(f"Tabela utilizada: `{table_used}` • Banco: `{os.path.basename(db_path)}`")

    if df.empty:
        st.info("Nenhuma movimentação encontrada.")
        return

    # Detecta e converte coluna de data
    date_col = _infer_date_column(df)
    if not date_col:
        st.warning("Não encontrei coluna de data (ex.: 'data', 'data_movimento'). Exibindo tabela sem filtros.")
        st.dataframe(df, use_container_width=True, hide_index=True)
        return

    df_dt = _coerce_datetime(df, date_col)
    if df_dt["_dt"].isna().all():
        st.warning(f"Falha ao converter '{date_col}' para data. Exibindo tabela sem filtros.")
        st.dataframe(df, use_container_width=True, hide_index=True)
        return

    # =================== Filtros (UI) ===================
    st.markdown("#### 🔎 Filtros")

    anos_disponiveis = sorted(df_dt["_dt"].dropna().dt.year.unique().tolist())
    hoje = date.today()
    ano_padrao = (hoje.year if hoje.year in anos_disponiveis else (anos_disponiveis[-1] if anos_disponiveis else hoje.year))

    c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
    with c1:
        ano = st.selectbox("Ano", options=anos_disponiveis or [ano_padrao],
                           index=(anos_disponiveis.index(ano_padrao) if ano_padrao in anos_disponiveis else 0))
    meses = ["Todos os meses","Janeiro","Fevereiro","Março","Abril","Maio","Junho","Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]
    with c2:
        mes_idx_padrao = min(max(hoje.month, 1), 12)
        mes_nome = st.selectbox("Mês", options=meses, index=mes_idx_padrao)
    with c3:
        dia_escolhido = st.date_input("Dia", value=hoje, format="DD/MM/YYYY")
    with c4:
        usar_dia = st.checkbox("Filtrar pelo dia escolhido", value=False, help="Quando ligado, mostra somente o dia selecionado.")

    # =================== Aplicação dos filtros ===================
    df_filt = df_dt.dropna(subset=["_dt"]).copy()
    if usar_dia:
        df_filt = df_filt[df_filt["_dt"].dt.date == dia_escolhido]
        filtro_msg = f"Dia selecionado: **{dia_escolhido.strftime('%d/%m/%Y')}**"
    else:
        if mes_nome == "Todos os meses":
            df_filt = df_filt[df_filt["_dt"].dt.year == ano]
            filtro_msg = f"Ano selecionado: **{ano}** (todos os meses)"
        else:
            mes_idx = meses.index(mes_nome)
            df_filt = df_filt[(df_filt["_dt"].dt.year == ano) & (df_filt["_dt"].dt.month == mes_idx)]
            filtro_msg = f"Ano/Mês selecionado: **{ano} / {mes_nome}**"

    # ======= Preparação dos campos =======
    df_work = df_filt.copy()
    df_work["data_hora"] = df_work["_dt"].dt.strftime("%d/%m/%Y %H:%M")

    valor_col = _infer_valor_col(df_work)
    df_work["valor"] = df_work[valor_col].apply(_fmt_moeda) if valor_col else _fmt_moeda(0)

    # Apenas estas colunas na UI
    base_cols = ["data_hora", "valor", "observacao", "banco", "usuario"]
    show_cols = [c for c in base_cols if c in df_work.columns]

    # Ordena para exibição (mantém alinhamento para a série de referência)
    df_sorted = df_work.sort_values(by="data_hora", ascending=False, ignore_index=True)

    # Série de referência (usada só para colorir as linhas)
    ref_col = _infer_ref_col(df_work)
    ref_series = (df_sorted[ref_col].astype(str).str.strip().str.lower()) if ref_col and ref_col in df_sorted.columns else None

    # DataFrame final exibido — somente as colunas pedidas
    to_show = df_sorted[show_cols].copy()

    # Mensagem do filtro + legenda
    st.caption(filtro_msg)
    st.markdown(_legend_html(), unsafe_allow_html=True)

    # ======= Render com estilo por série (sem adicionar coluna) =======
    if ref_series is not None:
        def _apply_row_style(row: pd.Series):
            val = ref_series.iloc[row.name] if row.name < len(ref_series) else ""
            return _style_row_from_value(val, len(row.index))
        styled = to_show.style.apply(_apply_row_style, axis=1)
        st.dataframe(styled, use_container_width=True, hide_index=True)
    else:
        st.dataframe(to_show, use_container_width=True, hide_index=True)
