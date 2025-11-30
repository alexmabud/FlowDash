from __future__ import annotations
import streamlit as st

# Chave usada no session_state para visibilidade do form
_KEY_FORM_CAIXA2 = "form_caixa2_visivel"

def toggle_form() -> None:
    """Alterna a visibilidade do formulário de Caixa 2."""
    st.session_state[_KEY_FORM_CAIXA2] = not st.session_state.get(_KEY_FORM_CAIXA2, False)

def close_form() -> None:
    """Força o fechamento do formulário."""
    st.session_state[_KEY_FORM_CAIXA2] = False

def form_visivel() -> bool:
    """Retorna True se o formulário deve ser exibido."""
    return st.session_state.get(_KEY_FORM_CAIXA2, False)

def invalidate_confirm() -> None:
    """(Opcional) Reseta flags de confirmação se necessário."""
    pass