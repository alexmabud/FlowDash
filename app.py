import streamlit as st
import pandas as pd
import extra_streamlit_components as stx
from streamlit_autorefresh import st_autorefresh
import time
import os
from datetime import datetime

# Configuração da Página
st.set_page_config(page_title="FlowDash", page_icon="🚀", layout="wide")

# ==============================================================================
# 1. PERSISTÊNCIA DE LOGIN & COOKIE MANAGER
# ==============================================================================
# O CookieManager deve ser instanciado no início para gerenciar a sessão
try:
    cookie_manager = stx.CookieManager()
except Exception as e:
    st.error(f"Erro ao inicializar CookieManager: {e}")
    st.stop()

# Simulação de base de usuários (Em produção, use um banco seguro/hash)
USERS = {
    "admin": "123", # Senha simples para teste, conforme contexto de dev
    "user": "123"
}

def login_section():
    st.columns([1, 2, 1])[1].title("🔐 FlowDash - Login")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("login_form"):
            username = st.text_input("Usuário")
            # REQUISITO 3: type="password" para gerenciadores de senha
            password = st.text_input("Senha", type="password")
            submit = st.form_submit_button("Entrar", use_container_width=True)
            
            if submit:
                if username in USERS and USERS[username] == password:
                    # Cria o cookie de autenticação com validade (ex: 7 dias)
                    cookie_manager.set("auth_token", username, key="set_auth")
                    st.success("Login realizado! Redirecionando...")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Usuário ou senha incorretos.")

def logout():
    cookie_manager.delete("auth_token", key="del_auth")
    st.rerun()

# ==============================================================================
# 5. SIMULAÇÃO DE BACKEND (CSV)
# ==============================================================================
CSV_FILE = "vendas.csv"

def carregar_dados():
    if not os.path.exists(CSV_FILE):
        return pd.DataFrame(columns=["Data", "Produto", "Valor", "Vendedor"])
    
    try:
        df = pd.read_csv(CSV_FILE)
        # Garantir tipos de dados corretos
        df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return pd.DataFrame(columns=["Data", "Produto", "Valor", "Vendedor"])

def salvar_venda(nova_venda):
    df = carregar_dados()
    # Adiciona a nova linha
    novo_df = pd.DataFrame([nova_venda])
    df_final = pd.concat([df, novo_df], ignore_index=True)
    df_final.to_csv(CSV_FILE, index=False)

# ==============================================================================
# LÓGICA PRINCIPAL DO APP
# ==============================================================================
def main():
    # Verifica Autenticação via Cookie
    # O get() do cookie manager pode retornar None na primeira execução enquanto carrega
    auth_user = cookie_manager.get("auth_token")

    if not auth_user:
        login_section()
        return

    # 4. ATUALIZAÇÃO DE DADOS EM TEMPO REAL
    # Executa apenas se logado
    # Atualiza a página a cada 30 segundos (30000ms)
    count = st_autorefresh(interval=30000, limit=None, key="f5_auto")

    # ==========================================================================
    # 6. ESTRUTURA VISUAL - USUÁRIO LOGADO
    # ==========================================================================
    
    # Sidebar
    with st.sidebar:
        st.title("FlowDash")
        st.markdown(f"Bem-vindo, **{auth_user}**!")
        st.markdown("---")
        if st.button("🚪 Sair", use_container_width=True):
            logout()
    
    st.title("🚀 FlowDash - Vendas em Tempo Real")
    st.markdown("---")

    # Carga de Dados
    df_vendas = carregar_dados()

    # Métricas de Topo
    if not df_vendas.empty:
        total_vendido = df_vendas["Valor"].sum()
        qtd_vendas = len(df_vendas)
    else:
        total_vendido = 0.0
        qtd_vendas = 0

    col_m1, col_m2, col_m3 = st.columns(3)
    col_m1.metric("💰 Total Vendido", f"R$ {total_vendido:,.2f}")
    col_m2.metric("📦 Quantidade de Vendas", f"{qtd_vendas}")
    
    # Botão manual de atualização
    col_m3.markdown("### ") # Espaçamento
    if col_m3.button("🔄 Atualizar Lista", use_container_width=True):
        st.rerun()

    st.markdown("---")

    # Layout Duas Colunas
    col_form, col_data = st.columns([1, 2])

    # ESQUERDA: Formulário
    with col_form:
        st.subheader("📝 Nova Venda")
        with st.form("form_venda", clear_on_submit=True):
            produto = st.text_input("Produto")
            valor = st.number_input("Valor (R$)", min_value=0.0, step=0.01, format="%.2f")
            vendedor = st.text_input("Vendedor", value=auth_user) 
            
            submit_venda = st.form_submit_button("💾 Salvar Venda", use_container_width=True)

            if submit_venda:
                if produto and valor > 0 and vendedor:
                    nova_venda = {
                        "Data": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "Produto": produto,
                        "Valor": valor,
                        "Vendedor": vendedor
                    }
                    salvar_venda(nova_venda)
                    st.toast("✅ Venda registrada com sucesso!")
                    time.sleep(1) # Pequena pausa para toast aparecer
                    st.rerun() # Atualização imediata
                else:
                    st.warning("Preencha todos os campos obrigatórios.")

    # DIREITA: Tabela de Dados
    with col_data:
        st.subheader("📊 Últimas Vendas")
        
        if not df_vendas.empty:
            # Ordenar da mais recente para a mais antiga
            df_display = df_vendas.sort_values(by="Data", ascending=False)
            
            st.dataframe(
                df_display,
                use_container_width=True,
                column_config={
                    "Valor": st.column_config.NumberColumn(format="R$ %.2f"),
                    "Data": st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm")
                },
                hide_index=True
            )
        else:
            st.info("Nenhuma venda registrada ainda.")

if __name__ == "__main__":
    main()
