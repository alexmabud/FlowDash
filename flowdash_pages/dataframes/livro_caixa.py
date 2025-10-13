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

# -------- normalização simples (tira acentos/espacos) --------
def _norm(s: str) -> str:
    if not isinstance(s, str):
        return str(s)
    s = s.strip().lower()
    s = (s
         .replace("á","a").replace("à","a").replace("ã","a").replace("â","a")
         .replace("é","e").replace("ê","e")
         .replace("í","i")
         .replace("ó","o").replace("ô","o").replace("õ","o")
         .replace("ú","u")
         .replace("ç","c"))
    s = " ".join(s.split())
    return s

def _infer_ref_col(df: pd.DataFrame) -> Optional[str]:
    """Detecta a coluna de referência (para cores/filtros) com tolerância a acentos/maiusc./espaços."""
    targets = ["referencia_tabela", "tipo", "tipo_mov", "referencia", "origem"]
    norm_map = {_norm(c): c for c in df.columns}
    for t in targets:
        if t in norm_map:
            return norm_map[t]
    for k, v in norm_map.items():
        if "referencia" in k:
            return v
    return None

def _style_row_from_value(val: str, ncols: int) -> List[str]:
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
    lower = {c.lower(): c for c in df.columns}
    for c in ["valor", "amount", "valor_total", "valor_liquido"]:
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
    <div style="margin: .25rem 0 .5rem 0; display:flex; flex-wrap:wrap; gap:.5rem;">
      <span style="padding:.25rem .5rem; border-radius:999px; font-weight:600; background:rgba(34,197,94,.12); color:#16a34a;">Entradas</span>
      <span style="padding:.25rem .5rem; border-radius:999px; font-weight:600; background:rgba(220,53,69,.12); color:#dc3545;">Saídas</span>
      <span style="padding:.25rem .5rem; border-radius:999px; font-weight:600; background:rgba(236,72,153,.18); color:#db2777;">Obrigações Futuras</span>
      <span style="padding:.25rem .5rem; border-radius:999px; font-weight:600; background:rgba(245,158,11,.18); color:#d97706;">Cadastros Saldos Banco/Caixa</span>
      <span style="padding:.25rem .5rem; border-radius:999px; font-weight:600; background:rgba(59,130,246,.18); color:#2563eb;">Transferências Caixa 2/Bancos</span>
      <span style="padding:.25rem .5rem; border-radius:999px; font-weight:600; background:rgba(139,92,246,.18); color:#7c3aed;">Correções de Caixa</span>
    </div>
    """

# ---------- parser robusto p/ data_hora/data (resolve mix tz-aware/naive) ----------
def _parse_to_naive_local(s: pd.Series) -> pd.Series:
    """Converte strings de data/hora para datetime64[ns] **sem timezone**."""
    if s is None:
        return pd.Series(pd.NaT, index=[], dtype="datetime64[ns]")
    s_str = s.astype(str).str.strip()

    # detecta sufixo de fuso explícito
    mask_tz = s_str.str.contains(r'(?:[+-]\d{2}:\d{2}|Z)$', na=False)

    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

    if mask_tz.any():
        dt_off = pd.to_datetime(s_str[mask_tz], errors="coerce")
        try:
            dt_off = dt_off.dt.tz_convert("America/Sao_Paulo").dt.tz_localize(None)
        except Exception:
            dt_off = pd.to_datetime(dt_off, errors="coerce")
        out.loc[mask_tz] = dt_off

    if (~mask_tz).any():
        dt_naive = pd.to_datetime(s_str[~mask_tz], errors="coerce")
        try:
            if str(dt_naive.dtype).startswith("datetime64[ns,"):
                dt_naive = dt_naive.dt.tz_localize(None)
        except Exception:
            pass
        out.loc[~mask_tz] = dt_naive

    return out

# ===== _dt: prioriza 'data_hora' → 'data' e depois colunas "date-like" =====
def _build_dt(df: pd.DataFrame) -> pd.Series:
    norm_map = {_norm(c): c for c in df.columns}

    def _parse(colname: Optional[str]) -> pd.Series:
        if not colname or colname not in df.columns:
            return pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
        return _parse_to_naive_local(df[colname])

    out = _parse(norm_map.get("data_hora"))
    out = out.where(out.notna(), _parse(norm_map.get("data")))

    # colunas "date-like" restantes (nome contém 'data' ou começa com 'dt')
    date_like = []
    for original in df.columns:
        n = _norm(original)
        if n in {"data_hora", "data"}:
            continue
        if ("data" in n) or n.startswith("dt"):
            date_like.append(original)
    date_like = sorted(date_like, key=lambda x: _norm(x))  # determinístico

    for col in date_like:
        out = out.where(out.notna(), _parse(col))

    return out  # datetime64[ns] sem timezone

# ======== Grupos do filtro rápido (botões) ========
REF_FILTER_GROUPS = {
    "Entradas": {"entrada"},
    "Saídas": {"saida"},
    "Obrigações Futuras": {"contas_a_pagar_mov", "contas_a_pagar"},
    "Cadastros Saldos Banco/Caixa": {"saldos_bancos", "saldos_bancarios", "saldos_bancários", "saldos_caixa", "saldos_caixas"},
    "Transferências Caixa 2/Bancos": {"movimentacoes_bancarias", "movimentações_bancárias", "transferencias", "transferências", "transferencia", "transferência"},
    "Correções de caixa": {"correcao_caixa"},
}
REF_FILTER_GROUPS_NORM = {k: {_norm(v) for v in vals} for k, vals in REF_FILTER_GROUPS.items()}

def _apply_quick_filter(df: pd.DataFrame, ref_col: Optional[str]) -> tuple[pd.DataFrame, Optional[str]]:
    """
    Aplica o filtro rápido conforme seleção em session_state["lc_tipo_sel"].
    Retorna (df_filtrado, label_selecionado_ou_None).
    """
    label = st.session_state.get("lc_tipo_sel")
    if not label or label not in REF_FILTER_GROUPS_NORM or not ref_col or ref_col not in df.columns:
        return df, None

    allowed = REF_FILTER_GROUPS_NORM[label]
    mask = df[ref_col].astype(str).map(lambda x: _norm(x) in allowed)
    return df.loc[mask].copy(), label

# ================= Página =================
def render(db_path_pref: Optional[str] = None) -> None:
    """
    Página: Livro Caixa
    Exibe APENAS as colunas: data_hora, valor, observacao, banco, usuario.
    Colore a linha com base em 'referencia_tabela' (ou variação), sem exibir essa coluna.
    """

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

    if df.empty:
        st.info("Nenhuma movimentação encontrada.")
        return

    # ===== Monta _dt (data_hora → data → coalesce com colunas date-like) =====
    _dt = _build_dt(df)
    df_dt = df.copy()
    df_dt["_dt"] = _dt

    # =================== Filtros (UI) ===================
    st.markdown("#### 🔎 Filtros")

    anos_disponiveis = sorted(df_dt["_dt"].dropna().dt.year.unique().tolist()) or [date.today().year]
    hoje = date.today()
    ano_padrao = (hoje.year if hoje.year in anos_disponiveis else (anos_disponiveis[-1] if anos_disponiveis else hoje.year))

    c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
    with c1:
        ano = st.selectbox(
            "Ano",
            options=anos_disponiveis or [ano_padrao],
            index=(anos_disponiveis.index(ano_padrao) if ano_padrao in anos_disponiveis else 0)
        )
    meses = [
        "Todos os meses","Janeiro","Fevereiro","Março","Abril","Maio","Junho",
        "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"
    ]
    with c2:
        mes_idx_padrao = min(max(hoje.month, 1), 12)
        mes_nome = st.selectbox("Mês", options=meses, index=mes_idx_padrao)
    with c3:
        dia_escolhido = st.date_input("Dia", value=hoje, format="DD/MM/YYYY")
    with c4:
        usar_dia = st.checkbox("Filtrar pelo dia escolhido", value=False, help="Quando ligado, mostra somente o dia selecionado.")

    # =================== Aplicação dos filtros de período ===================
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

    # ======= Filtro rápido por TIPO (botões) =======
    st.markdown("##### Filtro rápido por tipo")
    # inicializa seleção
    if "lc_tipo_sel" not in st.session_state:
        st.session_state["lc_tipo_sel"] = None

    btn_cols = st.columns([1,1,1,1,1,1,1])  # + 1 para "Todos"
    labels = ["Todos"] + list(REF_FILTER_GROUPS.keys())
    for i, label in enumerate(labels):
        with btn_cols[i]:
            pressed = st.button(
                label,
                type=("primary" if st.session_state["lc_tipo_sel"] == label else "secondary"),
                use_container_width=True
            )
            if pressed:
                st.session_state["lc_tipo_sel"] = None if label == "Todos" else label

    # aplica filtro rápido
    ref_col = _infer_ref_col(df_filt)
    df_filt, sel = _apply_quick_filter(df_filt, ref_col)
    if sel:
        filtro_msg += f" • Tipo: **{sel}**"

    # ======= Preparação dos campos =======
    df_work = df_filt.copy()
    df_work["data_hora"] = df_work["_dt"].dt.strftime("%d/%m/%Y %H:%M")

    valor_col = _infer_valor_col(df_work)
    df_work["valor"] = df_work[valor_col].apply(_fmt_moeda) if valor_col else _fmt_moeda(0)

    base_cols = ["data_hora", "valor", "observacao", "banco", "usuario"]
    show_cols = [c for c in base_cols if c in df_work.columns]

    # Ordena para exibição (mantém alinhamento para a série de referência)
    df_sorted = df_work.sort_values(by="data_hora", ascending=False, ignore_index=True)

    # Série de referência (para colorir as linhas) — nunca exibida
    ref_col = _infer_ref_col(df_sorted)
    ref_series = (df_sorted[ref_col].astype(str).str.strip().str.lower()) if ref_col and ref_col in df_sorted.columns else None

    to_show = df_sorted[show_cols].copy()

    # Mensagem do filtro + legenda
    st.caption(filtro_msg)
    st.markdown(_legend_html(), unsafe_allow_html=True)

    # ===== Altura para ~30 linhas antes do scroll =====
    # Aproximação: 30 linhas * ~34px + cabeçalho/margens
    _rows_target = 30
    _row_px = 34
    _header_px = 52
    height_px = min(_rows_target, len(to_show)) * _row_px + _header_px

    # ======= Render com estilo por série (sem adicionar coluna) =======
    if ref_series is not None:
        def _apply_row_style(row: pd.Series):
            val = ref_series.iloc[row.name] if row.name < len(ref_series) else ""
            return _style_row_from_value(val, len(row.index))
        styled = to_show.style.apply(_apply_row_style, axis=1)

        # <<< Garantir quebra de linha na coluna 'observacao' >>>
        if "observacao" in to_show.columns:
            styled = styled.set_properties(
                subset=["observacao"],
                **{
                    "white-space": "pre-wrap",
                    "word-break": "break-word",
                },
            )

        st.dataframe(styled, use_container_width=True, hide_index=True, height=height_px)
    else:
        # Sem coloração por linha, ainda aplicamos a quebra de linha em 'observacao'
        if "observacao" in to_show.columns:
            styled = to_show.style.set_properties(
                subset=["observacao"],
                **{
                    "white-space": "pre-wrap",
                    "word-break": "break-word",
                },
            )
            st.dataframe(styled, use_container_width=True, hide_index=True, height=height_px)
        else:
            st.dataframe(to_show, use_container_width=True, hide_index=True, height=height_px)
