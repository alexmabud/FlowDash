# ===================== Page: Venda =====================
"""
Página principal de Venda — monta layout e chama forms/actions.
"""

from __future__ import annotations

from datetime import date
import time
import streamlit as st

from utils.utils import coerce_data  # normaliza a data recebida

from .state_venda import toggle_form, form_visivel
from .ui_forms_venda import render_form_venda
from .actions_venda import registrar_venda

__all__ = ["render_venda"]


def render_venda(state) -> None:
    """
    Renderiza a página de Venda.
    """
    # --- Extrai do state -----------------------------------------------------
    caminho_banco = getattr(state, "caminho_banco", getattr(state, "db_path", None))
    data_lanc_raw = getattr(state, "data_lanc", None)

    # --- Normaliza para datetime.date ----
    data_lanc: date = coerce_data(data_lanc_raw)

    # --- Toggle ---------------------------------------------------------------
    if st.button("🟢 Nova Venda", use_container_width=True, key="btn_venda_toggle"):
        toggle_form()

    if not form_visivel():
        return

    # --- Formulário -----------------------------------------------------------
    try:
        form = render_form_venda(caminho_banco, data_lanc)
    except Exception as e:
        st.error(f"❌ Falha ao montar formulário: {e}")
        return

    if not form:
        return

    # --- Botão Salvar ---------------------------------------------------------
    if not form.get("confirmado"):
        st.button("💾 Salvar Venda", use_container_width=True, key="venda_salvar", disabled=True)
        return

    if not st.button("💾 Salvar Venda", use_container_width=True, key="venda_salvar_ok"):
        return

    # --- Execução -------------------------------------------------------------
    try:
        res = registrar_venda(
            db_like=caminho_banco,
            data_lanc=data_lanc,
            payload=form,
        )

        if res.get("ok"):
            # Feedback rápido para não travar
            st.toast(f"Venda de R$ {form.get('valor')} Registrada!", icon='✅')

            # Define a mensagem no state para ser pega pelo page_lancamentos (Toast)
            st.session_state["msg_ok"] = res.get("msg", "Venda registrada.")
            st.session_state["msg_ok_type"] = "success"  # Opcional, reforça ícone verde
            st.session_state.form_venda = False
            
            # 🔄 força recarregar o Resumo do Dia / cards
            st.session_state["_resumo_dirty"] = time.time()

            # ✅ limpa caches
            st.cache_data.clear()

            st.rerun()
        else:
            st.error(res.get("msg") or "Erro ao salvar a venda.")

    except ValueError as ve:
        st.warning(f"⚠️ {ve}")
    except Exception as e:
        st.error(f"❌ Erro ao salvar venda: {e}")