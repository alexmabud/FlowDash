# ===================== Page: Saída =====================
"""
Página principal da Saída – monta layout e chama forms/actions.
"""

from __future__ import annotations

from typing import Any, Optional, Tuple
import datetime as _dt
import streamlit as st

from utils.utils import coerce_data

from .state_saida import toggle_form, form_visivel, invalidate_confirm
from .ui_forms_saida import render_form_saida
from .actions_saida import (
    carregar_listas_para_form,
    registrar_saida,
)

__all__ = ["render_saida"]


# ----------------- helpers -----------------
def _norm_date(d: Any) -> _dt.date:
    """Normaliza data para datetime.date."""
    return coerce_data(d)


def _coalesce_state(
    state: Any,
    caminho_banco: Optional[str],
    data_lanc: Optional[Any],
) -> Tuple[str, _dt.date]:
    """Extrai (db_path, data_lanc: date) de state com fallback para args diretos."""
    db = None
    dt = None
    if state is not None:
        db = getattr(state, "db_path", None) or getattr(state, "caminho_banco", None)
        dt = (
            getattr(state, "data_lanc", None)
            or getattr(state, "data_lancamento", None)
            or getattr(state, "data", None)
        )
    db = db or caminho_banco
    dt = dt or data_lanc
    if not db:
        raise ValueError("Caminho do banco não informado (state.db_path / caminho_banco).")
    return str(db), _norm_date(dt)


# ----------------- página -----------------
def render_saida(
    state: Any = None,
    caminho_banco: Optional[str] = None,
    data_lanc: Optional[Any] = None,
) -> None:
    """
    Renderiza a página de Saída.
    """
    # Resolver entradas
    try:
        _db_path, _data_lanc = _coalesce_state(state, caminho_banco, data_lanc)
    except Exception as e:
        st.error(f"❌ Configuração incompleta: {e}")
        return

    # Toggle do formulário
    if st.button("🔴 Saída", use_container_width=True, key="btn_saida_toggle"):
        toggle_form()

    if not form_visivel():
        return

    # Contexto do usuário
    usuario = st.session_state.get("usuario_logado", {"nome": "Sistema"})
    usuario_nome = usuario.get("nome", "Sistema")

    # Carrega listas/repos necessárias
    try:
        carregado = carregar_listas_para_form(_db_path)
        if isinstance(carregado, (list, tuple)) and len(carregado) >= 8:
            (
                nomes_bancos,
                nomes_cartoes,
                df_categorias,
                listar_subcategorias_fn,
                listar_destinos_fatura_em_aberto_fn,
                carregar_opcoes_pagamentos_fn,
                listar_boletos_em_aberto_fn,
                listar_empfin_em_aberto_fn,
            ) = carregado[:8]
        else:
            (
                nomes_bancos,
                nomes_cartoes,
                df_categorias,
                listar_subcategorias_fn,
                listar_destinos_fatura_em_aberto_fn,
                carregar_opcoes_pagamentos_fn,
            ) = carregado[:6]
            listar_boletos_em_aberto_fn = lambda: []
            listar_empfin_em_aberto_fn = lambda: []
    except Exception as e:
        st.error(f"❌ Falha ao preparar formulário: {e}")
        return

    # Render UI
    try:
        payload = render_form_saida(
            data_lanc=_data_lanc,
            invalidate_cb=invalidate_confirm,
            nomes_bancos=nomes_bancos,
            nomes_cartoes=nomes_cartoes,
            categorias_df=df_categorias,
            listar_subcategorias_fn=listar_subcategorias_fn,
            listar_destinos_fatura_em_aberto_fn=listar_destinos_fatura_em_aberto_fn,
            carregar_opcoes_pagamentos_fn=carregar_opcoes_pagamentos_fn,
            listar_boletos_em_aberto_fn=listar_boletos_em_aberto_fn,
            listar_empfin_em_aberto_fn=listar_empfin_em_aberto_fn,
        )
    except TypeError:
        # Fallback
        payload = render_form_saida(
            data_lanc=_data_lanc,
            invalidate_cb=invalidate_confirm,
            nomes_bancos=nomes_bancos,
            nomes_cartoes=nomes_cartoes,
            categorias_df=df_categorias,
            listar_subcategorias_fn=listar_subcategorias_fn,
            listar_destinos_fatura_em_aberto_fn=listar_destinos_fatura_em_aberto_fn,
            carregar_opcoes_pagamentos_fn=carregar_opcoes_pagamentos_fn,
        )

    # Botão salvar
    save_disabled = not st.session_state.get("confirmar_saida", False)
    if not st.button("💾 Salvar Saída", use_container_width=True, key="btn_salvar_saida", disabled=save_disabled):
        return

    # Segurança no servidor
    if not st.session_state.get("confirmar_saida", False):
        st.warning("⚠️ Confirme os dados antes de salvar.")
        return

    # ===== Compat: preencher aliases de valor =====
    try:
        if payload.get("is_pagamentos"):
            v = float(payload.get("valor_saida") or 0.0)
            tipo = (payload.get("tipo_pagamento_sel") or "").lower()
            if "fatura" in tipo:
                payload.setdefault("valor_a_pagar_fatura", v)
                payload.setdefault("valor_pagamento", v)
            elif ("boleto" in tipo) or ("emprést" in tipo) or ("emprest" in tipo) or ("financi" in tipo):
                payload.setdefault("valor_pagamento", v)
    except Exception:
        pass

    # Execução
    try:
        res = registrar_saida(
            caminho_banco=_db_path,
            data_lanc=_data_lanc,
            usuario_nome=usuario_nome,
            payload=payload,
        )

        raw_msg = (res or {}).get("msg") or (res or {}).get("mensagem") or ""
        msg = (raw_msg or "").strip()
        if "idempot" in msg.lower():
            msg = "Pagamento registrado com sucesso."

        # Feedbacks (Toast flow)
        st.session_state["msg_ok"] = msg or "Pagamento registrado com sucesso."
        st.session_state["msg_ok_type"] = "success"  # Define ícone verde no toast
        st.session_state.form_saida = False

        # Info de classificação (somente para Pagamentos fora de Boletos)
        if payload.get("is_pagamentos") and payload.get("tipo_pagamento_sel") != "Boletos":
            st.info(
                f"Destino classificado: {payload.get('tipo_pagamento_sel')} → "
                f"{payload.get('destino_pagamento_sel') or '—'}"
            )

        st.rerun()

    except ValueError as ve:
        st.warning(f"⚠️ {ve}")
    except Exception as e:
        st.error(f"❌ Erro ao salvar saída: {e}")