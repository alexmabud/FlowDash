"""
Página agregadora de **Lançamentos**: exibe o resumo do dia e renderiza as subpáginas
(Venda, Saída, Caixa 2, Depósito, Transferência e Mercadorias).
"""

from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
import importlib
from typing import Any, Callable, Optional
import sqlite3

import pandas as pd
import streamlit as st

from .actions_pagina import carregar_resumo_dia
from .ui_cards_pagina import render_card_row, render_card_rows, render_card_mercadorias
from flowdash_pages.utils_timezone import hoje_br


# ===================== Helpers =====================
def _get_default_data_lanc() -> Optional[str]:
    """Obtém ou inicializa `data_lanc` no session_state como string YYYY-MM-DD."""
    try:
        v = st.session_state.get("data_lanc")
        if not v:
            v = hoje_br().strftime("%Y-%m-%d")
            st.session_state["data_lanc"] = v
        return v
    except Exception:
        return None


def _brl(v: float | int | None) -> str:
    """Formata um número em BRL sem depender de locale."""
    try:
        n = float(v or 0.0)
    except Exception:
        n = 0.0
    return f"R$ {n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _safe_call(mod_path: str, func_name: str, state: Any) -> None:
    """Importa e executa com segurança `func_name(state)` do módulo `mod_path`."""
    try:
        mod = importlib.import_module(mod_path)
        fn: Optional[Callable[[Any], None]] = getattr(mod, func_name, None)
        if fn is None:
            st.warning(f"⚠️ A subpágina '{mod_path}' não expõe `{func_name}(state)`.")
            return
        fn(state)
    except Exception as e:
        st.error(f"❌ Falha ao renderizar '{mod_path}.{func_name}': {e}")


def _read_sql(conn: sqlite3.Connection, query: str, params=None) -> pd.DataFrame:
    return pd.read_sql(query, conn, params=params or ())




# ===================== Page =====================
def render_page(caminho_banco: str, data_default: date | None = None) -> None:
    """Renderiza a página agregadora de Lançamentos."""
    
    # [TRAVA DE SEGURANÇA - VERSÃO CORRIGIDA]
    try:
        from flowdash_pages.fechamento.lock_manager import verificar_pendencia_bloqueante
        
        data_pendente = verificar_pendencia_bloqueante(caminho_banco)
        
        if data_pendente:

            
            # Título grande e vermelho
            st.error(f"🚨 CAIXA DO DIA {data_pendente} NÃO FOI FECHADO!", icon="🚫")
            
            # Explicação clara
            st.warning(
                f"""
                O sistema identificou movimentações financeiras no dia **{data_pendente}** (Vendas, Saídas ou Transferências) que ainda não foram encerradas.
                
                ⚠️ **Ação Necessária:**
                Por favor, acesse o menu lateral esquerdo, vá em **Fechamento** e encerre o dia pendente para liberar novos lançamentos.
                """
            )
            
            # Bloqueia o resto da página
            st.stop()
            
    except Exception as e:
        # Loga o erro no terminal mas permite o sistema abrir se a trava falhar
        print(f"Aviso: Erro ao verificar trava de segurança: {e}")
    
    # --- MUDANÇA: Captura msg do state e exibe Toast ---
    if "msg_ok" in st.session_state:
        msg = st.session_state.pop("msg_ok")
        # Pega o tipo (success ou warning) ou define padrão success
        tipo = st.session_state.pop("msg_ok_type", "success")
        icon = "✅" if tipo == "success" else "⚠️"
        st.toast(msg, icon=icon)
    # ---------------------------------------------------

    # Data de referência do lançamento
    data_lanc = st.date_input(
        "🗓️ Data do Lançamento",
        value=data_default or hoje_br(),
        key="data_lanc",
    )
    st.markdown(f"## 🧾 Lançamentos do Dia — **{data_lanc}**")

    # =================================================================
    # [NOVO] TRAVA 2: BLOQUEIA EDIÇÃO SE O DIA SELECIONADO JÁ ESTIVER FECHADO
    # =================================================================
    from flowdash_pages.fechamento.lock_manager import verificar_se_dia_esta_fechado
    
    if verificar_se_dia_esta_fechado(caminho_banco, data_lanc):
        st.error(f"🔒 O DIA {data_lanc} JÁ ESTÁ FECHADO!", icon="🔒")
        st.info(
            f"Você não pode realizar novos lançamentos ou alterações em **{data_lanc}** "
            "porque o fechamento de caixa deste dia já foi realizado."
        )
        st.stop() # Bloqueia o carregamento dos formulários abaixo
    # =================================================================

    # Resumo agregado do dia
    resumo = carregar_resumo_dia(caminho_banco, data_lanc) or {}

    # ----- Resumo do Dia -----
    total_vendas = float(resumo.get("total_vendas", 0.0))
    total_saidas = float(resumo.get("total_saidas", 0.0))
    render_card_row(
        "📊 Resumo do Dia",
        [("Vendas", total_vendas, True), ("Saídas", total_saidas, True)],
    )

    # ----- Saldos (2 linhas no mesmo card) -----
    # 1) Caixa e Caixa 2 — EXIBIÇÃO com "saldo projetado" (acumulado real)
    from flowdash_pages.fechamento.fechamento import _ultimo_caixas_ate
    disp_caixa, disp_caixa2, disp_ref = _ultimo_caixas_ate(caminho_banco, data_lanc)

    # 2) Bancos (Inter, InfinitePay, Bradesco) com tolerância a chaves variantes
    saldos_bancos = resumo.get("saldos_bancos") or {}
    nb = {(str(k) or "").strip().lower(): float(v or 0.0) for k, v in saldos_bancos.items()}
    inter = nb.get("inter", 0.0)
    infinite = nb.get(
        "infinitepay",
        nb.get("infinite pay", nb.get("infinite_pay", nb.get("infinitiepay", 0.0))),
    )
    bradesco = nb.get("bradesco", 0.0)

    render_card_rows(
        "💵 Saldos",
        [
            [("Caixa", disp_caixa, True), ("Caixa 2", disp_caixa2, True)],  # linha 1 (2 colunas)
            [("Inter", inter, True), ("InfinitePay", infinite, True), ("Bradesco", bradesco, True)],  # linha 2 (3 colunas)
        ],
    )
    if disp_ref and disp_ref != data_lanc:
        st.caption(f"Mostrando último saldo salvo em **{disp_ref}** (sem movimento em {data_lanc}).")

    # ----- Transferências (card com 3 colunas) -----
    # 1) P/ Caixa 2 (número)
    transf_caixa2_total = float(resumo.get("transf_caixa2_total", 0.0))

    # 2) Depósitos (lista)
    dep_lin: list[str] = []
    for b, v in (resumo.get("depositos_list") or []):
        dep_lin.append(f"{_brl(v)} → {b or '—'}")

    # 3) Transferência entre bancos — TABELA real (Valor | Saída | Entrada)
    trf_raw = resumo.get("transf_bancos_list") or []  # List[Tuple[origem, destino, valor]]
    if trf_raw:
        try:
            trf_df = pd.DataFrame(trf_raw, columns=["Saída", "Entrada", "Valor"])
        except Exception:
            # fallback robusto se a estrutura vier diferente
            trf_df = pd.DataFrame(trf_raw)
            # tenta renomear se possível
            cols = {c.lower(): c for c in trf_df.columns}
            if "origem" in cols:
                trf_df.rename(columns={cols["origem"]: "Saída"}, inplace=True)
            if "destino" in cols:
                trf_df.rename(columns={cols["destino"]: "Entrada"}, inplace=True)
            if "valor" in cols:
                trf_df.rename(columns={cols["valor"]: "Valor"}, inplace=True)
            # garante colunas finais
            for c in ["Saída", "Entrada", "Valor"]:
                if c not in trf_df.columns:
                    trf_df[c] = ""
        trf_df["Saída"] = trf_df["Saída"].fillna("").astype(str).str.strip().replace("", "—")
        trf_df["Entrada"] = trf_df["Entrada"].fillna("").astype(str).str.strip().replace("", "—")
        trf_df["Valor"] = pd.to_numeric(trf_df["Valor"], errors="coerce").fillna(0.0)
        trf_df = trf_df[["Valor", "Saída", "Entrada"]]  # ordem exata solicitada
    else:
        trf_df = pd.DataFrame(columns=["Valor", "Saída", "Entrada"])

    render_card_row(
        "🔁 Transferências",
        [
            ("P/ Caixa 2", transf_caixa2_total, False),
            ("Depósito Bancário", dep_lin, False),
            ("Transferência entre bancos", trf_df, False),
        ],
    )

    # ----- Mercadorias -----
    render_card_mercadorias(resumo.get("compras_list") or [], resumo.get("receb_list") or [])

    # ----- Ações (subpáginas) -----
    state = SimpleNamespace(db_path=caminho_banco, caminho_banco=caminho_banco, data_lanc=data_lanc)
    st.markdown("### ➕ Ações")
    a1, a2 = st.columns(2)
    with a1:
        _safe_call("flowdash_pages.lancamentos.venda.page_venda", "render_venda", state)
    with a2:
        _safe_call("flowdash_pages.lancamentos.saida.page_saida", "render_saida", state)

    c1, c2, c3 = st.columns(3)
    with c1:
        _safe_call("flowdash_pages.lancamentos.caixa2.page_caixa2", "render_caixa2", state)
    with c2:
        _safe_call("flowdash_pages.lancamentos.deposito.page_deposito", "render_deposito", state)
    with c3:
        _safe_call("flowdash_pages.lancamentos.transferencia.page_transferencia", "render_transferencia", state)

    st.markdown("---")
    st.markdown("### 📦 Mercadorias — Lançamentos")
    _safe_call("flowdash_pages.lancamentos.mercadorias.page_mercadorias", "render_mercadorias", state)