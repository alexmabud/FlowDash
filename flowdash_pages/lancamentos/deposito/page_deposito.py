# ===================== Page: Depósito =====================
"""
Página principal do Depósito – monta layout e chama forms/actions.

Comportamento esperado:
- O baseline diário (snapshot da véspera) é garantido DENTRO da ação de depósito,
  e não na renderização da página, para evitar a criação de linhas vazias.
- Toggle do formulário.
- Confirmação obrigatória para habilitar o botão Salvar.
- Mensagens de sucesso/erro.
- st.rerun() após sucesso.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any, Optional, Tuple

import streamlit as st

from utils.utils import coerce_data  # normaliza para datetime.date
from .actions_deposito import carregar_nomes_bancos, registrar_deposito
from .state_deposito import form_visivel, invalidate_confirm, toggle_form
from .ui_forms_deposito import render_form_deposito
# A importação de _ensure_snapshot_do_dia não é mais necessária aqui
# from ..caixa2.actions_caixa2 import _ensure_snapshot_do_dia
from shared.db import get_conn


# --- helpers (mesmo estilo da Transferência) ---
def _norm_date(d: Any) -> _dt.date:
    """Converte a entrada em uma data (datetime.date)."""
    return coerce_data(d)


def _coalesce_state(
    state: Any,
    caminho_banco: Optional[str],
    data_lanc: Optional[Any],
) -> Tuple[str, _dt.date]:
    """Extrai (db_path, data_lanc) a partir do state/args."""
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


def _resolve_usuario(state: Any = None) -> str:
    """Obtém o usuário logado de session_state/state."""
    for k in ("usuario_logado", "usuario", "user_name", "username", "nome_usuario", "user", "current_user", "email"):
        v = st.session_state.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, dict):
            for kk in ("nome", "name", "login", "email"):
                if isinstance(v.get(kk), str) and v[kk].strip():
                    return v[kk].strip()
    if state is not None:
        for k in ("usuario_logado", "usuario", "user_name", "username", "nome_usuario", "user", "current_user", "email"):
            v = getattr(state, k, None)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return "sistema"


def _parse_valor_br(raw: Any) -> float:
    """Converte '60', '60,00', '1.234,56', 'R$ 1.234,56' em float. Retorna 0.0 se falhar."""
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


def render_deposito(
    state: Any = None,
    caminho_banco: Optional[str] = None,
    data_lanc: Optional[Any] = None,
) -> None:
    """
    Renderiza a página de Depósito. Preferencialmente chame com `render_deposito(state)`;
    é compatível com a chamada via argumentos diretos.
    """
    # Resolver entradas
    try:
        _db_path, _data_lanc = _coalesce_state(state, caminho_banco, data_lanc)
    except Exception as e:
        st.error(f"❌ Configuração incompleta: {e}")
        return

    # Toggle do formulário
    if st.button("🏦 Depósito Bancário", use_container_width=True, key="btn_dep_toggle"):
        toggle_form()

    if not form_visivel():
        return

    # Carrega bancos e renderiza form
    try:
        nomes_bancos = carregar_nomes_bancos(_db_path)
    except Exception as e:
        st.error(f"❌ Falha ao carregar bancos: {e}")
        return

    form = render_form_deposito(_data_lanc, nomes_bancos, invalidate_confirm)

    # Confirmação vinda do form (fallback para session_state)
    confirmada = bool(form.get("confirmado", st.session_state.get("deposito_confirmado", False)))

    # Botão de salvar: desabilitado até confirmar
    save_clicked = st.button(
        "💾 Salvar Depósito",
        use_container_width=True,
        key="btn_salvar_deposito",
        disabled=not confirmada,
    )

    st.info("Confirme os dados para habilitar o botão de salvar.")

    if not (confirmada and save_clicked):
        return

    # ===================== Validações =====================
    banco_dest = (form.get("banco_destino") or "").strip()
    valor = _parse_valor_br(form.get("valor", 0))

    if not banco_dest:
        st.info("Informe o banco de destino.")
        return
    if valor <= 0:
        st.info("Valor inválido.")
        return

    # ===================== Execução =====================
    try:
        usuario_atual = _resolve_usuario(state)
        res = registrar_deposito(
            caminho_banco=_db_path,
            data_lanc=_data_lanc,
            valor=valor,
            banco_in=banco_dest,
            usuario=usuario_atual,
        )
        st.session_state["msg_ok"] = res.get("msg", "Depósito registrado.")
        st.session_state.form_deposito = False
        st.success(res.get("msg", "Depósito registrado com sucesso."))
        st.rerun()
    except ValueError as ve:
        st.info(f"{ve}")
    except Exception as e:
        st.error(f"❌ Erro ao registrar depósito: {e}")