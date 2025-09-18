# -*- coding: utf-8 -*-
"""
FlowDash — Página PDV (com integração do formulário de venda)

Resumo
------
Página para uso no balcão (PDV): mostra um resumo de metas (placeholder),
exige seleção de vendedor + validação de PIN e, após autenticação, libera o
formulário de venda existente (reuso do módulo atual de vendas).

Diferenciais
------------
- Não altera nenhuma página de Usuários.
- Reúso do form de venda atual via import dinâmico.
- Guarda o vendedor autenticado em sessão para registrar no banco.

Notas
-----
- Excluímos perfil 'PDV' do dropdown de vendedores.
- Tentativas de PIN limitadas por sessão (5).
- PIN comparado com hmac.compare_digest.

Próximos patches
----------------
- Ligar o painel de metas oficial (hoje é placeholder seguro).
"""

from __future__ import annotations
import hmac
import sqlite3
import importlib
from typing import List, Tuple, Optional, Dict

import streamlit as st
from utils.pin_utils import validar_pin


# ============================ Helpers DB / Sessão ============================

def _conn(caminho_banco: str) -> sqlite3.Connection:
    conn = sqlite3.connect(caminho_banco)
    conn.row_factory = sqlite3.Row
    return conn

@st.cache_data(show_spinner=False, ttl=30)
def _listar_usuarios_ativos_sem_pdv(caminho_banco: str) -> List[Tuple[int, str, str]]:
    """Retorna lista de (id, nome, perfil) de usuários ativos, EXCLUINDO perfil 'PDV'."""
    with _conn(caminho_banco) as conn:
        rows = conn.execute(
            "SELECT id, nome, perfil FROM usuarios "
            "WHERE ativo = 1 AND (perfil IS NULL OR perfil <> ?) "
            "ORDER BY nome ASC",
            ("PDV",),
        ).fetchall()
    return [(int(r["id"]), str(r["nome"]), str(r["perfil"] or "")) for r in rows]

def _buscar_pin_usuario(caminho_banco: str, usuario_id: int) -> Optional[str]:
    with _conn(caminho_banco) as conn:
        row = conn.execute(
            "SELECT pin FROM usuarios WHERE id = ? AND ativo = 1",
            (usuario_id,),
        ).fetchone()
    return None if not row else row["pin"]  # pode ser None

def _inc_tentativa(usuario_id: int) -> int:
    """Incrementa e retorna o número de tentativas de PIN na sessão para esse usuário."""
    key = "pdv_pin_tentativas"
    st.session_state.setdefault(key, {})
    st.session_state[key][usuario_id] = st.session_state[key].get(usuario_id, 0) + 1
    return st.session_state[key][usuario_id]

def _reset_tentativas(usuario_id: int) -> None:
    key = "pdv_pin_tentativas"
    if key in st.session_state and usuario_id in st.session_state[key]:
        st.session_state[key][usuario_id] = 0


# ============================ Painel de Metas (placeholder seguro) ============================

def _render_painel_metas_resumo() -> None:
    """Tenta reusar o renderer de metas; se não achar, mostra placeholder."""
    st.markdown("### 🎯 Metas — Resumo")
    try:
        from flowdash_pages.metas import metas as _metas
        for cand in ("render_metas_auto", "render_metas", "render"):
            fn = getattr(_metas, cand, None)
            if callable(fn):
                fn()  # se precisar de args, ajustamos no próximo patch
                break
        else:
            raise ImportError("Renderer de metas não encontrado.")
    except Exception:
        with st.container(border=True):
            st.write("Painel de metas (resumo) ainda não conectado aqui.")
            st.caption("Integraremos com o módulo de metas no próximo patch.")


# ============================ Integração do Formulário de Venda ============================

def _render_form_venda(caminho_banco: str, vendedor: Dict[str, object]) -> None:
    """
    Renderiza o formulário de venda existente, reaproveitando o seu módulo atual.

    Convenções suportadas (tentativas, em ordem):
      - flowdash_pages.lancamentos.venda.page_venda.page_venda(caminho_banco)
      - ... .render(caminho_banco)
      - ... .render_venda(caminho_banco)
      - ... .main(caminho_banco)
      - Se a função não aceitar argumento, tenta chamá-la sem argumentos.

    Também expõe o contexto do vendedor para o módulo de venda:
      st.session_state['pdv_context'] = {'vendedor_id', 'vendedor_nome'}
    """
    st.markdown("### 🧾 Nova Venda")
    st.session_state["pdv_context"] = {
        "vendedor_id": vendedor["id"],
        "vendedor_nome": vendedor["nome"],
        "origem": "PDV",
    }

    try:
        mod = importlib.import_module("flowdash_pages.lancamentos.venda.page_venda")
        for fn_name in ("page_venda", "render", "render_venda", "main"):
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                try:
                    fn(caminho_banco)  # assinatura mais comum no seu projeto
                except TypeError:
                    fn()  # fallback, caso não aceite argumento
                break
        else:
            raise ImportError("Nenhum renderer conhecido em page_venda.py")
    except Exception as e:
        with st.container(border=True):
            st.warning("Formulário de venda ainda não conectado aqui.")
            st.caption(f"Detalhe técnico: {e}")


# ============================ Página principal PDV ============================

def pagina_pdv(caminho_banco: str) -> None:
    """
    Renderiza a página do PDV: metas (resumo) + seleção de vendedor + PIN
    e, após autenticação, o formulário de venda (reuso do módulo atual).
    """
    # Flash
    msg = st.session_state.pop("pdv_flash_ok", None)
    if msg:
        st.success(msg)

    st.subheader("🧾 PDV — Ponto de Venda")

    # Metas (placeholder)
    _render_painel_metas_resumo()

    st.markdown("---")
    st.markdown("#### 👤 Identificação do Vendedor")

    usuarios = _listar_usuarios_ativos_sem_pdv(caminho_banco)
    if not usuarios:
        st.warning("Nenhum usuário ativo encontrado. Cadastre em **Cadastros › Usuários**.")
        return

    # Já autenticado?
    vendedor_auth: Optional[Dict] = st.session_state.get("pdv_vendedor")
    if vendedor_auth:
        with st.container(border=True):
            st.success(f"Vendedor autenticado: **{vendedor_auth['nome']}** — Perfil: `{vendedor_auth['perfil']}`")
            col_a, col_b = st.columns([1, 1])
            with col_a:
                if st.button("🔁 Trocar vendedor"):
                    st.session_state.pop("pdv_vendedor", None)
                    st.session_state["pdv_flash_ok"] = "Você pode selecionar outro vendedor."
                    st.rerun()
            with col_b:
                if st.button("➕ Nova Venda (atalho)"):
                    st.session_state["pdv_mostrar_form"] = True
                    st.rerun()

        # Se o atalho foi pedido, renderiza o form de venda abaixo
        if st.session_state.get("pdv_mostrar_form"):
            _render_form_venda(caminho_banco, vendedor_auth)
        return  # mantém tudo dentro do PDV

    # Seleção do vendedor (quando ainda não autenticado)
    label_to_id: Dict[str, int] = {}
    labels = []
    for uid, nome, perfil in usuarios:
        label = f"{nome} — {perfil or 'Sem perfil'}"
        label_to_id[label] = uid
        labels.append(label)

    escolha = st.selectbox("Selecione o vendedor", labels)
    usuario_id = label_to_id[escolha]

    pin_in = st.text_input("PIN do vendedor (4 dígitos)", type="password", max_chars=4)

    if st.button("✅ Confirmar PIN"):
        # Limite de tentativas
        if _inc_tentativa(usuario_id) > 5:
            st.error("Muitas tentativas. Troque o vendedor ou tente novamente mais tarde.")
            return

        # Validação de formato
        try:
            pin_digitado = validar_pin(pin_in)
        except ValueError as e:
            st.error(str(e))
            return
        if pin_digitado is None:
            st.error("Informe o PIN de 4 dígitos para prosseguir.")
            return

        # PIN do banco
        pin_db = _buscar_pin_usuario(caminho_banco, usuario_id)
        if pin_db is None:
            st.error("Este usuário não possui PIN cadastrado. Defina um PIN na página de Usuários.")
            return

        # Comparação segura
        if hmac.compare_digest(str(pin_db), pin_digitado):
            nome_sel = next(n for (uid, n, _) in usuarios if uid == usuario_id)
            perfil_sel = next(p for (uid, _, p) in usuarios if uid == usuario_id)
            st.session_state["pdv_vendedor"] = {"id": usuario_id, "nome": nome_sel, "perfil": perfil_sel}
            _reset_tentativas(usuario_id)
            st.session_state["pdv_flash_ok"] = f"Vendedor **{nome_sel}** autenticado com sucesso."
            st.rerun()
        else:
            st.error("PIN incorreto.")
