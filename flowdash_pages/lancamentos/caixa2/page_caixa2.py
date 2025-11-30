# ===================== Page: Caixa 2 =====================
"""
Page: Caixa 2
Resumo: Transferência para Caixa 2.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any, Optional

import streamlit as st

from .state_caixa2 import toggle_form, form_visivel, close_form
from .ui_forms_caixa2 import render_form
from .actions_caixa2 import transferir_para_caixa2

__all__ = ["render_caixa2"]


def _coalesce_state(
    state: Any,
    caminho_banco: Optional[str],
    data_lanc: Optional[Any],
) -> tuple[str, str]:
    """Extrai (caminho_banco, data_lanc_str)."""
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
    if dt is None:
        raise ValueError("Data do lançamento não informada (state.data_lanc / data_lanc).")

    if isinstance(dt, _dt.date):
        dt_str = dt.strftime("%Y-%m-%d")
    else:
        try:
            dt_str = _dt.datetime.strptime(str(dt), "%Y-%m-%d").strftime("%Y-%m-%d")
        except Exception:
            try:
                dt_iso = _dt.datetime.fromisoformat(str(dt)).date()
                dt_str = dt_iso.strftime("%Y-%m-%d")
            except Exception:
                raise ValueError(f"Data do lançamento inválida: {dt!r}")

    return str(db), dt_str


def _parse_valor_br(raw: Any) -> float:
    """Converte entradas tipo '60,00', '1.234,56' em float."""
    try:
        s = str(raw or "").strip()
        if not s:
            return 0.0
        s = s.replace("R$", "").replace(" ", "")
        if "," in s and "." in s and s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", ".")
        return float(s)
    except Exception:
        try:
            return float(raw or 0)
        except Exception:
            return 0.0


def render_caixa2(
    state: Any = None,
    caminho_banco: Optional[str] = None,
    data_lanc: Optional[Any] = None,
) -> None:
    """Renderiza a página de transferência para o Caixa 2."""
    # 1) Inputs
    try:
        _db_path, _data_lanc = _coalesce_state(state, caminho_banco, data_lanc)
    except Exception as e:
        st.error(f"❌ Configuração incompleta: {e}")
        return

    # 2) Toggle do formulário
    if st.button("📦 Transferência para Caixa 2", use_container_width=True, key="btn_caixa2_toggle"):
        toggle_form()

    if not form_visivel():
        return

    # 3) Form UI
    form = render_form(_data_lanc)
    if not form.get("submit"):
        return

    # 4) Validação do valor
    v = _parse_valor_br(form.get("valor", 0))
    if v <= 0:
        st.warning("⚠️ Valor inválido.")
        return

    # 5) Resolve usuário logado
    usuario = (
        st.session_state.get("usuario_logado")
        or st.session_state.get("usuario")
        or st.session_state.get("username")
        or st.session_state.get("user")
        or "sistema"
    )
    if isinstance(usuario, dict):
        usuario = (
            usuario.get("nome")
            or usuario.get("name")
            or usuario.get("username")
            or usuario.get("email")
            or "sistema"
        )

    # 6) Executa a ação
    try:
        res = transferir_para_caixa2(
            caminho_banco=_db_path,
            data_lanc=_data_lanc,
            valor=v,
            usuario=usuario,
        )
        if isinstance(res, dict) and res.get("ok"):
            st.session_state["msg_ok"] = res.get("msg", "Transferência realizada.")
            st.session_state["msg_ok_type"] = "success"  # Toast Verde
            close_form()
            
            # Limpa campos do form para evitar conflito
            for k in ("caixa2_confirma_widget", "caixa2_valor"):
                if k in st.session_state:
                    del st.session_state[k]

            st.rerun()
        else:
            msg = (res or {}).get("msg") if isinstance(res, dict) else str(res)
            st.warning(f"⚠️ Não foi possível confirmar a operação. {msg or ''}".strip())
    except ValueError as ve:
        st.warning(f"⚠️ {ve}")
    except Exception as e:
        st.error(f"❌ Erro ao transferir: {e}")