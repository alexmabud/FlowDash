# -*- coding: utf-8 -*-
"""
auth.auth
=========

Funções de autenticação e controle de acesso com uso **seguro** do
Streamlit Session State (via `shared.safe_session`) para evitar o erro:
"Tried to use SessionInfo before it was initialized".
"""

from __future__ import annotations

from typing import Dict, Any, Optional, List
import sqlite3

import streamlit as st  # import permitido; evitamos apenas acessar session_state cedo
from shared.safe_session import (
    exists as _ss_exists,
    get as _ss_get,
)
from utils.utils import gerar_hash_senha


# -----------------------------------------------------------------------------
# Login / validação no banco
# -----------------------------------------------------------------------------

def validar_login(email: str, senha: str, caminho_banco: str) -> Dict[str, Any] | None:
    """
    Valida o login do usuário com base no banco de dados.

    Args:
        email: E-mail informado.
        senha: Senha em texto plano (será transformada em hash).
        caminho_banco: Caminho absoluto do banco SQLite.

    Returns:
        dict com {nome, email, perfil} se válido; caso contrário, None.
    """
    if not email or not senha or not caminho_banco:
        return None

    senha_hash = gerar_hash_senha(senha)
    query = """
        SELECT nome, email, perfil
        FROM usuarios
        WHERE email = ? AND senha = ? AND ativo = 1
    """

    with sqlite3.connect(caminho_banco) as conn:
        cur = conn.execute(query, (email, senha_hash))
        row = cur.fetchone()

    if row:
        return {"nome": row[0], "email": row[1], "perfil": row[2]}
    return None


# -----------------------------------------------------------------------------
# Controle de acesso (seguro ao runtime)
# -----------------------------------------------------------------------------

def verificar_acesso(perfis_permitidos: List[str]) -> None:
    """
    Verifica se o perfil do usuário logado permite acesso à página atual.

    Observação:
        Só interage com session_state se o runtime do Streamlit já existir.

    Efeitos:
        - Mostra aviso e interrompe execução da página (st.stop) quando negado.
    """
    if not _ss_exists():
        # Se chamado fora de uma página Streamlit (sem runtime), não faz nada.
        return

    usuario = _ss_get("usuario_logado")
    perfil = (usuario or {}).get("perfil")

    if not usuario or perfil not in (perfis_permitidos or []):
        st.warning("🚫 Acesso não autorizado.")
        st.stop()


def exibir_usuario_logado() -> None:
    """
    Exibe nome e perfil do usuário logado no topo da interface Streamlit.
    Não acessa session_state se o runtime não existir.
    """
    if not _ss_exists():
        return

    usuario = _ss_get("usuario_logado")
    if isinstance(usuario, dict) and usuario.get("nome"):
        st.markdown(f"👤 **{usuario['nome']}** — Perfil: `{usuario.get('perfil', '-')}`")
        st.markdown("---")


def limpar_todas_as_paginas() -> None:
    """
    Limpa os estados de exibição das páginas no session_state.
    Usado ao alternar de módulo no menu.
    """
    if not _ss_exists():
        return

    chaves = [
        "mostrar_metas", "mostrar_entradas", "mostrar_saidas", "mostrar_lancamentos_do_dia",
        "mostrar_mercadorias", "mostrar_cartao_credito", "mostrar_emprestimos_financiamentos",
        "mostrar_contas_pagar", "mostrar_taxas_maquinas", "mostrar_usuarios",
        "mostrar_fechamento_caixa", "mostrar_correcao_caixa", "mostrar_cadastrar_cartao",
        "mostrar_saldos_bancarios", "mostrar_cadastro_caixa", "mostrar_cadastro_meta",
    ]

    for chave in chaves:
        if chave in st.session_state:
            st.session_state[chave] = False


__all__ = [
    "validar_login",
    "verificar_acesso",
    "exibir_usuario_logado",
    "limpar_todas_as_paginas",
]
