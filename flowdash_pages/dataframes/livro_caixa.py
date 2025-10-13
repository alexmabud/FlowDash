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
    # Usa a mesma camada segura das outras páginas (sem acessar session_state no import-time)
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
    """
    Tenta ler a tabela principal. Ordem:
      1) movimentacoes_bancarias
      2) movimentaceos_bancarias (possível digitação)
    Retorna (df, nome_tabela_utilizada).
    """
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
    """Detecta a provável coluna de data no DF."""
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
    """
    candidates = ["referencia_tabela", "tipo", "tipo_mov", "referencia", "origem"]
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in lower:
            return lower[c]
    return None

def _style_row_by_ref(row: pd.Series, ref_col: str) -> List[str]:
    """
    Estiliza a linha inteira baseado no valor de ref_col.
    - 'entrada'                              -> verde suave
    - 'saida'                                -> vermelho suave
    - 'contas_a_pagar_mov' (ou contas_a_pagar) -> rosa suave
    - 'saldos_bancos'/'saldos_caixa(s)'      -> laranja suave
    - 'movimentacoes_bancarias'/'transferencias'/'transferencia' -> azul suave
    - 'correcao_caixa'                       -> roxo suave
    """
    val = str(row.get(ref_col, "")).strip().lower()
    base_style = [""] * len(row.index)

    if val == "entrada":
        style = "background-color: rgba(34,197,94,.12); color: #16a34a; font-weight: 600;"
        return [style] * len(base_style)

    if val == "saida":
        style = "background-color: rgba(220,53,69,.12); color: #dc3545; font-weight: 600;"
        return [style] * len(base_style)

    if val in {"contas_a_pagar_mov", "contas_a_pagar"}:
        # rosa
        style = "background-color: rgba(236,72,153,.18); color: #db2777; font-weight: 600;"
        return [style] * len(base_style)

    if val in {"saldos_bancos", "saldos_bancarios", "saldos_bancários", "saldos_caixa", "saldos_caixas"}:
        # laranja
        style = "background-color: rgba(245,158,11,.18); color: #d97706; font-weight: 600;"
        return [style] * len(base_style)

    if val in {"movimentacoes_bancarias", "movimentações_bancárias", "transferencias", "transferências", "transferencia", "transferência"}:
        # azul
        style = "background-color: rgba(59,130,246,.18); color: #2563eb; font-weight: 600;"
        return [style] * len(base_style)

    if val == "correcao_caixa":
        # roxo
        style = "background-color: rgba(139,92,246,.18); color: #7c3aed; font-weight: 600;"
        return [style] * len(base_style)

    return base_style

def _infer_valor_col(df: pd.DataFrame) -> Optional[str]:
    """Detecta a coluna de valor provável."""
    candidates = ["valor", "amount", "valor_total", "valor_liquido"]
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in lower:
            return lower[c]
    # fallback: primeira coluna numérica plausível
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            return c
    return None

def _fmt_moeda(v) -> str:
    """Formata número em R$ pt-BR de forma local, sem depender de locale."""
    try:
        x = float(v)
    except Exception:
        x = 0.0
    s = f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"

# ================= Página =================
def render(db_path_pref: Optional[str] = None) -> None:
    """
    Página: Livro Caixa
    - Filtros:
        * Ano + Mês sempre juntos (default = mês/ano atuais).
        * Mês inclui “Todos os meses” para listar o ano inteiro.
        * Dia visível sempre; só filtra se "Filtrar pelo dia escolhido" estiver marcado.
    - Estilo: linha VERDE para 'entrada', VERMELHA para 'saida',
              ROSA para 'contas_a_pagar_mov', LARANJA para 'saldos_bancos/caixa',
              AZUL para 'movimentacoes_bancarias/transferencias' e ROXO para 'correcao_caixa'.
    - Colunas exibidas: data_hora, valor (R$), observacao, banco, usuario.
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

    # Opções de ANO com base nos dados
    anos_disponiveis = sorted(df_dt["_dt"].dropna().dt.year.unique().tolist())
    hoje = date.today()
    ano_padrao = (
        hoje.year if hoje.year in anos_disponiveis
        else (anos_disponiveis[-1] if anos_disponiveis else hoje.year)
    )

    c1, c2, c3, c4 = st.columns([1, 1, 1, 1])

    # 1) Ano (trabalha junto do Mês)
    with c1:
        ano = st.selectbox(
            "Ano",
            options=anos_disponiveis or [ano_padrao],
            index=(anos_disponiveis.index(ano_padrao) if ano_padrao in anos_disponiveis else 0)
        )

    # 2) Mês (trabalha junto do Ano) — inclui "Todos os meses"; default: mês atual
    meses = [
        "Todos os meses",
        "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
        "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
    ]
    with c2:
        mes_idx_padrao = min(max(hoje.month, 1), 12)  # 1..12
        # "Todos os meses" está no índice 0, mês atual vira índice = mês
        mes_nome = st.selectbox("Mês", options=meses, index=mes_idx_padrao)

    # 3) Dia (sempre visível)
    with c3:
        dia_escolhido = st.date_input("Dia", value=hoje, format="DD/MM/YYYY")

    # 4) Toggle para aplicar o dia
    with c4:
        usar_dia = st.checkbox(
            "Filtrar pelo dia escolhido",
            value=False,
            help="Quando ligado, mostra somente o dia selecionado."
        )

    # =================== Aplicação dos filtros ===================
    df_filt = df_dt.dropna(subset=["_dt"]).copy()

    if usar_dia:
        # Dia tem prioridade quando marcado
        df_filt = df_filt[df_filt["_dt"].dt.date == dia_escolhido]
        filtro_msg = f"Dia selecionado: **{dia_escolhido.strftime('%d/%m/%Y')}**"
    else:
        if mes_nome == "Todos os meses":
            # ano inteiro
            df_filt = df_filt[df_filt["_dt"].dt.year == ano]
            filtro_msg = f"Ano selecionado: **{ano}** (todos os meses)"
        else:
            # ano + mês específico
            mes_idx = meses.index(mes_nome)  # Janeiro=1 ... Dezembro=12
            df_filt = df_filt[(df_filt["_dt"].dt.year == ano) & (df_filt["_dt"].dt.month == mes_idx)]
            filtro_msg = f"Ano/Mês selecionado: **{ano} / {mes_nome}**"

    # ======= Reorganização e ocultação de colunas =======
    # 1) data_hora (a partir de _dt)
    df_filt = df_filt.copy()
    df_filt["data_hora"] = df_filt["_dt"].dt.strftime("%d/%m/%Y %H:%M")

    # 2) valor formatado -> coluna exibida "valor"
    valor_col = _infer_valor_col(df_filt)
    if valor_col:
        df_filt["valor"] = df_filt[valor_col].apply(_fmt_moeda)
    else:
        df_filt["valor"] = _fmt_moeda(0)

    # 3) definir lista de colunas ocultas
    ocultar_candidates = {
        date_col, "_dt",  # data original + coluna auxiliar
        "data", "tipo", "origem", "referencia id", "referencia_id", "referencia", "id",
        "trans_uid",  # ocultar identificador técnico
    }
    # se a coluna de valor original não for exatamente "valor", ocultamos ela
    if valor_col and valor_col != "valor":
        ocultar_candidates.add(valor_col)

    # garantir que só removemos colunas existentes
    ocultar = [c for c in ocultar_candidates if c in df_filt.columns]

    # 4) ordem solicitada
    ordem_base = ["data_hora", "valor", "observacao", "banco", "usuario"]
    presentes = [c for c in ordem_base if c in df_filt.columns]

    # adiciona o restante (exceto ocultas e já presentes), preservando ordem original
    resto = [c for c in df_filt.columns if c not in set(presentes) | set(ocultar)]
    col_order = presentes + resto

    # aplica ocultação + ordenação
    to_show = df_filt.drop(columns=ocultar, errors="ignore")[col_order]

    # Ordena por data desc (após criação de data_hora)
    to_show = to_show.sort_values(by="data_hora", ascending=False, ignore_index=True)

    st.caption(filtro_msg)

    # =================== Estilo por 'referencia_tabela' ===================
    ref_col = _infer_ref_col(df_filt)  # usa df_filt, pois to_show pode ter escondido cols
    if ref_col and ref_col in df_filt.columns:
        # Se a coluna de referência estiver oculta, adiciona ela só para o styler, sem exibir
        tmp = to_show.copy()
        if ref_col not in tmp.columns and ref_col in df_filt.columns:
            tmp[ref_col] = df_filt[ref_col].values
        styled = tmp.style.apply(lambda row: _style_row_by_ref(row, ref_col), axis=1)
        # esconder a coluna de referência se tiver sido adicionada só pro estilo
        try:
            styled = styled.hide(axis="columns", subset=[ref_col])
        except Exception:
            pass
        st.dataframe(styled, use_container_width=True, hide_index=True)
        st.caption(
            "Legendas: **verde = entrada**, **vermelho = saída**, "
            "**rosa = contas_a_pagar_mov**, **laranja = saldos_bancos/caixa**, "
            "**azul = movimentacoes_bancarias/transferencias**, **roxo = correcao_caixa**."
        )
    else:
        st.dataframe(to_show, use_container_width=True, hide_index=True)
