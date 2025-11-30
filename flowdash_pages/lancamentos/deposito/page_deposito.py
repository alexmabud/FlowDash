# ===================== Page: Depósito =====================
"""
Página principal do Depósito.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any, Optional, Tuple

import streamlit as st

from utils.utils import coerce_data
from .actions_deposito import carregar_nomes_bancos, registrar_deposito
from .state_deposito import form_visivel, invalidate_confirm, toggle_form, close_form
from .ui_forms_deposito import render_form_deposito

def _norm_date(d: Any) -> _dt.date:
    return coerce_data(d)

def _coalesce_state(state: Any, caminho_banco: Optional[str], data_lanc: Optional[Any]) -> Tuple[str, _dt.date]:
    db = None
    dt = None
    if state is not None:
        db = getattr(state, "db_path", None) or getattr(state, "caminho_banco", None)
        dt = (getattr(state, "data_lanc", None) or getattr(state, "data_lancamento", None) or getattr(state, "data", None))
    db = db or caminho_banco
    dt = dt or data_lanc
    if not db:
        raise ValueError("Caminho do banco não informado.")
    return str(db), _norm_date(dt)

def _resolve_usuario(state: Any = None) -> str:
    # Tenta pegar usuário do session_state
    usuario = st.session_state.get("usuario_logado") or st.session_state.get("usuario") or "sistema"
    if isinstance(usuario, dict):
        return usuario.get("nome") or usuario.get("username") or "sistema"
    return str(usuario)

def _parse_valor_br(raw: Any) -> float:
    try:
        s = str(raw or "").strip().replace("R$", "").replace(" ", "")
        if "," in s and "." in s and s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", ".")
        return float(s)
    except:
        return 0.0

def render_deposito(state: Any = None, caminho_banco: Optional[str] = None, data_lanc: Optional[Any] = None) -> None:
    try:
        _db_path, _data_lanc = _coalesce_state(state, caminho_banco, data_lanc)
    except Exception as e:
        st.error(f"❌ Configuração incompleta: {e}")
        return

    if st.button("🏦 Depósito Bancário", use_container_width=True, key="btn_dep_toggle"):
        toggle_form()

    if not form_visivel():
        return

    try:
        nomes_bancos = carregar_nomes_bancos(_db_path)
    except Exception as e:
        st.error(f"❌ Falha ao carregar bancos: {e}")
        return

    form = render_form_deposito(_data_lanc, nomes_bancos, invalidate_confirm)
    confirmada = bool(form.get("confirmado", st.session_state.get("deposito_confirmado", False)))

    save_clicked = st.button("💾 Salvar Depósito", use_container_width=True, key="btn_salvar_deposito", disabled=not confirmada)

    if not confirmada:
        st.info("Confirme os dados para habilitar o botão de salvar.")

    if not (confirmada and save_clicked):
        return

    banco_dest = (form.get("banco_destino") or "").strip()
    valor = _parse_valor_br(form.get("valor", 0))

    if not banco_dest:
        st.warning("Informe o banco de destino.")
        return
    if valor <= 0:
        st.warning("Valor inválido.")
        return

    try:
        usuario_atual = _resolve_usuario(state)
        res = registrar_deposito(
            caminho_banco=_db_path,
            data_lanc=_data_lanc,
            valor=valor,
            banco_in=banco_dest,
            usuario=usuario_atual,
        )
        
        # Toast setup
        st.session_state["msg_ok"] = res.get("msg", "Depósito registrado.")
        st.session_state["msg_ok_type"] = "success"
        
        close_form() # Agora a função existe no state!
        st.rerun()
        
    except ValueError as ve:
        st.warning(f"⚠️ {ve}")
    except Exception as e:
        st.error(f"❌ Erro ao registrar depósito: {e}")