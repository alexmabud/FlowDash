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
import secrets
import string
import hashlib

import bcrypt
import streamlit as st  # import permitido; evitamos apenas acessar session_state cedo
from shared.safe_session import (
    exists as _ss_exists,
    get as _ss_get,
)
from utils.utils import criar_hash_senha_bcrypt


# -----------------------------------------------------------------------------
# Helpers de segurança
# -----------------------------------------------------------------------------

def _is_bcrypt_hash(hash_str: str) -> bool:
    """
    Detecta se o hash é bcrypt ou SHA-256.

    Bcrypt hashes começam com $2b$ ou $2a$ e têm ~60 caracteres.
    SHA-256 hashes são hexadecimais com 64 caracteres.

    Args:
        hash_str: String do hash armazenado.

    Returns:
        True se for bcrypt, False se for SHA-256 ou outro formato.
    """
    if not isinstance(hash_str, str):
        return False
    return hash_str.startswith('$2b$') or hash_str.startswith('$2a$')


# -----------------------------------------------------------------------------
# Login / validação no banco
# -----------------------------------------------------------------------------

def validar_login(email: str, senha: str, caminho_banco: str) -> Dict[str, Any] | None:
    """
    Valida o login do usuário com migração híbrida SHA-256 → bcrypt.

    Suporta ambos métodos de hash durante período de migração:
    - Bcrypt: método atual e seguro (preferencial)
    - SHA-256: método legado (migra automaticamente para bcrypt no login bem-sucedido)

    Args:
        email: E-mail informado.
        senha: Senha em texto plano.
        caminho_banco: Caminho absoluto do banco SQLite.

    Returns:
        dict com {nome, email, perfil} se válido; caso contrário, None.

    Migração:
        Se a senha estiver em SHA-256 e o login for bem-sucedido,
        o hash será automaticamente convertido para bcrypt no banco.
    """
    if not email or not senha or not caminho_banco:
        return None

    try:
        with sqlite3.connect(caminho_banco) as conn:
            conn.row_factory = sqlite3.Row

            # Busca usuário pelo email
            cur = conn.execute(
                "SELECT nome, email, perfil, senha FROM usuarios WHERE email = ? AND ativo = 1",
                (email,)
            )
            usuario = cur.fetchone()

            if not usuario:
                return None

            hash_armazenado = usuario['senha']

            # 1. Tenta validação com bcrypt (método novo e preferencial)
            if _is_bcrypt_hash(hash_armazenado):
                try:
                    if bcrypt.checkpw(senha.encode('utf-8'), hash_armazenado.encode('utf-8')):
                        # Login bem-sucedido com bcrypt
                        return {
                            "nome": usuario['nome'],
                            "email": usuario['email'],
                            "perfil": usuario['perfil']
                        }
                except Exception:
                    # Falha na verificação bcrypt (senha incorreta ou hash corrompido)
                    return None

            # 2. Fallback: validação com SHA-256 (método legado) + migração automática
            else:
                hash_senha_sha256 = hashlib.sha256(senha.encode('utf-8')).hexdigest()

                if hash_senha_sha256 == hash_armazenado:
                    # ✅ Login bem-sucedido com SHA-256!
                    # Agora migra automaticamente para bcrypt
                    novo_hash_bcrypt = bcrypt.hashpw(senha.encode('utf-8'), bcrypt.gensalt())

                    try:
                        conn.execute(
                            "UPDATE usuarios SET senha = ? WHERE email = ?",
                            (novo_hash_bcrypt.decode('utf-8'), email)
                        )
                        conn.commit()

                        # Log da migração (opcional - pode comentar se não quiser logs)
                        print(f"[MIGRAÇÃO BCRYPT] Usuário '{email}' migrado de SHA-256 para bcrypt")

                    except Exception as e:
                        # Se falhar a migração, ainda permite login (não bloqueia usuário)
                        print(f"[AVISO] Falha ao migrar senha para bcrypt: {e}")

                    # Retorna usuário logado (independente do sucesso da migração)
                    return {
                        "nome": usuario['nome'],
                        "email": usuario['email'],
                        "perfil": usuario['perfil']
                    }

            # Senha incorreta (nem bcrypt nem SHA-256 bateram)
            return None

    except Exception as e:
        print(f"[ERRO] Falha no login: {e}")
        return None


def obter_usuario(email: str, caminho_banco: str) -> Dict[str, Any] | None:
    """
    Recupera os dados do usuário pelo e-mail (usado para login via cookie).
    """
    if not email or not caminho_banco:
        return None

    query = "SELECT nome, email, perfil FROM usuarios WHERE email = ? AND ativo = 1"
    with sqlite3.connect(caminho_banco) as conn:
        cur = conn.execute(query, (email,))
        row = cur.fetchone()

    if row:
        return {"nome": row[0], "email": row[1], "perfil": row[2]}
    return None


def criar_sessao(email: str, caminho_banco: str) -> str | None:
    """Gera um token de sessão, salva no banco e retorna o token."""
    if not email or not caminho_banco:
        return None
    
    # Gera token seguro de 32 chars
    alphabet = string.ascii_letters + string.digits
    token = ''.join(secrets.choice(alphabet) for i in range(32))
    
    try:
        with sqlite3.connect(caminho_banco) as conn:
            conn.execute("UPDATE usuarios SET token_sessao = ? WHERE email = ?", (token, email))
            conn.commit()
        return token
    except Exception:
        return None


def validar_sessao(token: str, caminho_banco: str) -> Dict[str, Any] | None:
    """Valida se o token existe no banco e retorna o usuário."""
    if not token or not caminho_banco:
        return None
        
    query = "SELECT nome, email, perfil FROM usuarios WHERE token_sessao = ? AND ativo = 1"
    try:
        with sqlite3.connect(caminho_banco) as conn:
            cur = conn.execute(query, (token,))
            row = cur.fetchone()
            
        if row:
            return {"nome": row[0], "email": row[1], "perfil": row[2]}
    except Exception:
        pass
    return None


def encerrar_sessao(email: str, caminho_banco: str) -> None:
    """Remove o token de sessão do usuário."""
    if not email or not caminho_banco:
        return
    try:
        with sqlite3.connect(caminho_banco) as conn:
            conn.execute("UPDATE usuarios SET token_sessao = NULL WHERE email = ?", (email,))
            conn.commit()
    except Exception:
        pass


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
    "obter_usuario",
    "verificar_acesso",
    "exibir_usuario_logado",
    "limpar_todas_as_paginas",
    "criar_sessao",
    "validar_sessao",
    "encerrar_sessao",
]
