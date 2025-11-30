from __future__ import annotations
import streamlit as st

_KEY_FORM_DEPOSITO = "form_deposito"
_KEY_CONFIRMADO = "deposito_confirmado"

def toggle_form() -> None:
    """Alterna a visibilidade do formulário de Depósito."""
    st.session_state[_KEY_FORM_DEPOSITO] = not st.session_state.get(_KEY_FORM_DEPOSITO, False)
    
    # Ao alternar (abrir/fechar), resetamos a confirmação.
    # Fazer isso aqui é seguro pois ocorre no início do script, antes do widget ser desenhado.
    invalidate_confirm()

def close_form() -> None:
    """Fecha o formulário de Depósito."""
    # Apenas esconde o formulário. Não altera o checkbox 'deposito_confirmado' aqui
    # para evitar erro de modificação de widget já instanciado.
    st.session_state[_KEY_FORM_DEPOSITO] = False

def form_visivel() -> bool:
    return st.session_state.get(_KEY_FORM_DEPOSITO, False)

def invalidate_confirm() -> None:
    """Reseta a confirmação do depósito (define como False no session_state)."""
    st.session_state[_KEY_CONFIRMADO] = False