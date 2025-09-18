"""
FlowDash — Página de Cadastro de Usuários

Resumo
------
Tela para criar e listar usuários do sistema, com validações de e-mail/senha,
inclusão do PIN (4 dígitos) para identificação no PDV e mensagem de sucesso
persistente via `st.session_state`.

Responsabilidades
-----------------
- Renderizar formulário de cadastro (nome, e-mail, perfil, ativo, senha, PIN).
- Validar entradas (senha forte, e-mail válido, PIN 4 dígitos opcional).
- Persistir novo usuário em `usuarios` (SQL parametrizado).
- Exibir lista de usuários e permitir alternar ativo/ inativo e exclusão.
- Exibir “flash” de sucesso após salvar (sem piscar).

Entradas
--------
- (runtime) `caminho_banco: str` — caminho absoluto do banco SQLite.

Saídas
------
- (UI) Componentes Streamlit renderizados.
- (DB) Linhas persistidas/atualizadas em `usuarios`.

Dependências
------------
- `streamlit`, `sqlite3`, `pandas`
- `utils.utils`: `gerar_hash_senha`, `senha_forte`
- `utils.pin_utils`: `validar_pin`  (garante PIN com 4 dígitos ou None)
- `flowdash_pages.cadastros.cadastro_classes`: `Usuario`

Notas de segurança
------------------
- SQL sempre parametrizado (evita injeção).
- Senhas armazenadas com hash (`gerar_hash_senha`).
- PIN **não é criptografado** por requisito do PDV (apenas identificador),
  mas é validado (4 dígitos) e existe trigger no DB para reforço.
- PIN não é exibido em listagens nem logs de sucesso/erro.
- Uso de `st.session_state` para flash evita reenvio duplicado de formulário.

Alterações recentes
-------------------
- 2025-09-17: Adicionados campo PIN (4 dígitos) e flash persistente no topo.
- 2025-09-17: Adicionada opção de perfil **PDV** no dropdown.
"""

import streamlit as st
import sqlite3
import pandas as pd

from utils.utils import gerar_hash_senha, senha_forte
from utils.pin_utils import validar_pin  # util centralizado
from flowdash_pages.cadastros.cadastro_classes import Usuario


def pagina_usuarios(caminho_banco: str) -> None:
    """
    Renderiza a página de cadastro e listagem de usuários.

    Parâmetros
    ----------
    caminho_banco : str
        Caminho absoluto para o arquivo do banco SQLite.

    Efeitos colaterais
    ------------------
    - Lê/grava na tabela `usuarios`.
    - Usa `st.session_state['usuarios_flash_ok']` para mensagem de sucesso
      persistente entre reruns.
    - Chama `st.rerun()` após operações de escrita (salvar/alternar/excluir).

    Exceções tratadas
    -----------------
    - `sqlite3.IntegrityError`: e-mail duplicado -> feedback ao usuário.
    - `ValueError` de `validar_pin`: feedback ao usuário e interrupção (`st.stop()`).

    Regras de negócio
    -----------------
    - `ativo`: "Sim" -> 1, "Não" -> 0.
    - `perfil`: um de ["Administrador", "Gerente", "Vendedor", "PDV"].
      Use **PDV** para a conta fixa do balcão (PDV) que ficará aberta na loja.
    - `PIN`: opcional; se informado, deve conter exatamente 4 dígitos [0-9].
    - `senha`: precisa ser considerada "forte" por `senha_forte`.
    """
    # ---- Flash de sucesso persistente no topo (após rerun) ----
    msg_ok = st.session_state.pop("usuarios_flash_ok", None)
    if msg_ok:
        st.success(msg_ok)
    # -----------------------------------------------------------

    st.subheader("👥 Cadastro de Usuários")

    with st.form("form_usuarios"):
        col1, col2 = st.columns(2)

        with col1:
            nome = st.text_input("Nome Completo", max_chars=100)
            PERFIS = ["Administrador", "Gerente", "Vendedor", "PDV"]  # <- inclui PDV
            perfil = st.selectbox(
                "Perfil",
                PERFIS,
                help="Use 'PDV' para a conta do balcão (PDV) que ficará aberta na loja."
            )

        with col2:
            email = st.text_input("Email", max_chars=100)
            ativo = st.selectbox("Usuário Ativo?", ["Sim", "Não"])

        senha = st.text_input("Senha", type="password", max_chars=50)
        confirmar_senha = st.text_input("Confirmar Senha", type="password", max_chars=50)

        # ------------------ PIN ------------------
        pin_raw = st.text_input(
            "PIN (4 dígitos)",
            type="password",
            max_chars=4,
            help="Identificação rápida no PDV (não criptografado). Deixe em branco se não quiser definir agora.",
        )
        # ----------------------------------------

        submitted = st.form_submit_button("💾 Salvar Usuário")

        if submitted:
            if not nome or not email or not senha or not confirmar_senha:
                st.error("❗ Todos os campos são obrigatórios!")
            elif senha != confirmar_senha:
                st.warning("⚠️ As senhas não coincidem. Tente novamente.")
            elif not senha_forte(senha):
                st.warning("⚠️ A senha deve ter pelo menos 8 caracteres, com letra maiúscula, minúscula, número e símbolo.")
            elif "@" not in email or "." not in email:
                st.warning("⚠️ Digite um e-mail válido.")
            else:
                # Validação do PIN (None se vazio, ou 4 dígitos)
                try:
                    pin = validar_pin(pin_raw)
                except ValueError as e:
                    st.error(str(e))
                    st.stop()

                senha_hash = gerar_hash_senha(senha)
                ativo_valor = 1 if ativo == "Sim" else 0
                try:
                    with sqlite3.connect(caminho_banco) as conn:
                        conn.execute("""
                            INSERT INTO usuarios (nome, email, senha, perfil, ativo, pin)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (nome, email, senha_hash, perfil, ativo_valor, pin))
                        conn.commit()
                    # ---- grava flash e rerun para não “piscar” ----
                    st.session_state["usuarios_flash_ok"] = f"✅ Usuário '{nome}' cadastrado com sucesso!"
                    st.rerun()
                except sqlite3.IntegrityError:
                    st.error("⚠️ Email já cadastrado!")
                except Exception as e:
                    st.error(f"❌ Erro ao salvar usuário: {e}")

    st.markdown("### 📋 Usuários Cadastrados:")

    with sqlite3.connect(caminho_banco) as conn:
        df = pd.read_sql("SELECT id, nome, email, perfil, ativo FROM usuarios", conn)

    if not df.empty:
        for _, row in df.iterrows():
            usuario = Usuario(row["id"], row["nome"], row["email"], row["perfil"], row["ativo"])
            col1, col2, col3, col4, col5 = st.columns([2, 3, 2, 2, 2])

            with col1:
                st.write(f"👤 {usuario.nome}")
            with col2:
                st.write(usuario.email)
            with col3:
                st.write(usuario.exibir_info()[2])
            with col4:
                if st.button("🔁 ON/OFF", key=f"ativar_{usuario.id}"):
                    usuario.alternar_status(caminho_banco)
                    st.rerun()
            with col5:
                if st.session_state.get(f"confirmar_exclusao_{usuario.id}", False):
                    st.warning(f"❓ Tem certeza que deseja excluir o usuário '{usuario.nome}'?")
                    col_c, col_d = st.columns(2)
                    with col_c:
                        if st.button("✅ Confirmar", key=f"confirma_{usuario.id}"):
                            usuario.excluir(caminho_banco)
                            st.success(f"✅ Usuário '{usuario.nome}' excluído com sucesso!")
                            st.rerun()
                    with col_d:
                        if st.button("❌ Cancelar", key=f"cancelar_{usuario.id}"):
                            st.session_state[f"confirmar_exclusao_{usuario.id}"] = False
                            st.rerun()
                else:
                    if st.button("🗑️ Excluir", key=f"excluir_{usuario.id}"):
                        if st.session_state.usuario_logado["email"] == usuario.email:
                            st.warning("⚠️ Você não pode excluir seu próprio usuário enquanto estiver logado.")
                        else:
                            st.session_state[f"confirmar_exclusao_{usuario.id}"] = True
                            st.rerun()
    else:
        st.info("ℹ️ Nenhum usuário cadastrado.")
