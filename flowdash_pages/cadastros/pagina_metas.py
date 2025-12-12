
import streamlit as st
import pandas as pd
from datetime import datetime
from utils.utils import formatar_valor, formatar_percentual
from flowdash_pages.cadastros.cadastro_classes import MetaManager, DIAS_SEMANA
from flowdash_pages.dashboard.prophet_engine import criar_grafico_previsao, calcular_sazonalidade_semanal, calcular_share_usuario
from flowdash_pages.dataframes.dataframes import carregar_df_entrada # Certifique-se que o import está correto

# Página de Cadastro de Metas =====================================================================================
def pagina_metas_cadastro(caminho_banco: str):
    st.markdown("## 🎯 Cadastro de Metas")
    manager = MetaManager(caminho_banco)

    # --- Inicialização do Session State ---
    # Isso garante que os campos possam ser atualizados pelo botão da IA
    defaults = {
        "meta_mensal": 0.0,
        "perc_prata": 87.5,
        "perc_bronze": 75.0,
        "semanal_percentual": 25.0,
        "dias_percentuais": [0.0] * 7
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val

    try:
        lista_usuarios = manager.carregar_usuarios_ativos()
    except Exception as e:
        st.error(f"Erro ao carregar usuários: {e}")
        return

    nomes = [nome for nome, _ in lista_usuarios]
    vendedor_selecionado = st.selectbox("Selecione o usuário para cadastro de meta", nomes)
    id_usuario = dict(lista_usuarios)[vendedor_selecionado]
    mes_atual = datetime.today().strftime("%Y-%m")
    
    col_info1, col_info2 = st.columns([1, 3])
    with col_info1:
        st.markdown(f"#### 📆 Mês: `{mes_atual}`")

    # --- ÁREA DA INTELIGÊNCIA ARTIFICIAL ---
    with st.expander("🔮 Inteligência Artificial (Prophet)", expanded=True):
        st.info("A IA analisa o histórico para sugerir valores Otimistas (Ouro), Realistas (Prata) e Pessimistas (Bronze).")
        
        # Carrega dados para IA
        df_entrada = carregar_df_entrada() 
        
        # Calcula share sugerido
        share_sugerido = 100.0
        if not df_entrada.empty:
            share_sugerido = calcular_share_usuario(df_entrada, vendedor_selecionado)
        
        col_ia1, col_ia2 = st.columns([2, 1])
        with col_ia1:
            share_input = st.slider(
                f"Participação de **{vendedor_selecionado}** na meta da Loja (%)", 
                0.0, 100.0, float(share_sugerido), 1.0,
                help="Se for 100%, assume a meta total prevista para a loja. Se for vendedor, ajusta proporcionalmente."
            )
        
        with col_ia2:
            st.write("") # Espaçamento
            if st.button("✨ Sugerir Meta com IA", use_container_width=True):
                with st.spinner("Consultando o Oráculo..."):
                    try:
                        # 1. Previsão Prophet (Loja Total)
                        _, _, metricas = criar_grafico_previsao(df_entrada, meses_futuro=1)
                        
                        if metricas and metricas.get('otimista', 0) > 0:
                            v_otimista_loja = metricas['otimista']
                            v_realista_loja = metricas['previsto']
                            v_pessimista_loja = metricas['pessimista']
                            
                            # 2. Aplica Share do Vendedor
                            fator = share_input / 100.0
                            meta_ouro_user = v_otimista_loja * fator
                            meta_prata_user = v_realista_loja * fator
                            meta_bronze_user = v_pessimista_loja * fator
                            
                            # 3. Calcula Percentuais Reversos
                            # Evita divisão por zero
                            p_prata = (meta_prata_user / meta_ouro_user * 100.0) if meta_ouro_user else 87.5
                            p_bronze = (meta_bronze_user / meta_ouro_user * 100.0) if meta_ouro_user else 75.0
                            
                            # 4. Calcula Sazonalidade Diária
                            distribuicao_semanal = calcular_sazonalidade_semanal(df_entrada)

                            # 5. Atualiza Session State
                            st.session_state['meta_mensal'] = float(meta_ouro_user)
                            st.session_state['perc_prata'] = float(p_prata)
                            st.session_state['perc_bronze'] = float(p_bronze)
                            st.session_state['dias_percentuais'] = distribuicao_semanal
                            
                            st.success(f"Sugestão aplicada! Meta Loja: {formatar_valor(v_otimista_loja)} → Meta {vendedor_selecionado}: {formatar_valor(meta_ouro_user)}")
                            st.rerun() # Recarrega para mostrar novos valores
                        else:
                            st.warning("Dados insuficientes para previsão da IA.")
                    except Exception as e:
                        st.error(f"Erro na IA: {e}")

    # --- FORMULÁRIO (Conectado ao Session State) ---
    st.markdown("### 💰 Meta Mensal (Ouro)")
    # O value vem do session_state, mas o usuário pode editar (o que atualiza o session_state via key)
    meta_mensal = st.number_input(
        "Valor da meta mensal (R$)", 
        min_value=0.0, step=100.0, format="%.2f",
        key="meta_mensal"
    )

    st.markdown("### 🧮 Níveis de Meta")
    col_p1, col_p2 = st.columns(2)
    with col_p1:
        perc_prata = st.number_input("Percentual Prata (%)", 0.0, 100.0, step=0.5, format="%.1f", key="perc_prata")
        st.info(f"🥈 Prata: {formatar_valor(meta_mensal * perc_prata / 100)}")
    with col_p2:
        perc_bronze = st.number_input("Percentual Bronze (%)", 0.0, 100.0, step=0.5, format="%.1f", key="perc_bronze")
        st.warning(f"🥉 Bronze: {formatar_valor(meta_mensal * perc_bronze / 100)}")

    st.markdown("### 📅 Meta Semanal")
    semanal_percentual = st.number_input("Percentual da meta mensal para a meta semanal (%)", 0.0, 100.0, step=1.0, format="%.1f", key="semanal_percentual")
    meta_semanal_valor = meta_mensal * (semanal_percentual / 100)
    st.success(f"Meta Semanal Base: {formatar_valor(meta_semanal_valor)}")

    st.markdown("### 📆 Distribuição Diária (% da meta semanal)")
    st.caption("A IA calcula isso com base no peso histórico de cada dia da semana.")
    
    col1, col2, col3 = st.columns(3)
    percentuais_finais = []
    
    # Recupera a lista atual do session state
    lista_dias_atual = st.session_state.get('dias_percentuais', [0.0]*7)

    for i, dia in enumerate(DIAS_SEMANA):
        col = [col1, col2, col3][i % 3]
        with col:
            # Usamos um key único para cada dia para persistir edição manual
            val_dia = st.number_input(
                f"{dia} (%)", 
                min_value=0.0, max_value=100.0, 
                value=float(lista_dias_atual[i]),
                step=1.0, format="%.1f",
                key=f"dia_{i}" 
            )
            percentuais_finais.append(val_dia)
            st.caption(f"→ {formatar_valor(meta_semanal_valor * (val_dia / 100))}")

    st.divider()

    if st.button("💾 Salvar Metas Definidas", type="primary"):
        soma_dias = sum(percentuais_finais)
        # Tolerância pequena para erro de ponto flutuante
        if not (99.9 <= soma_dias <= 100.1):
            st.warning(f"A soma dos percentuais diários deve ser 100%. Está em {soma_dias:.1f}%")
        else:
            try:
                sucesso = manager.salvar_meta(
                    id_usuario=id_usuario,
                    vendedor=vendedor_selecionado,
                    mensal=meta_mensal,
                    semanal_percentual=semanal_percentual,
                    dias_percentuais=percentuais_finais,
                    perc_bronze=perc_bronze,
                    perc_prata=perc_prata,
                    mes=mes_atual
                )
                if sucesso:
                    st.toast("✅ Metas salvas com sucesso!")
                    # Opcional: Limpar session state ou manter
            except Exception as e:
                st.error(f"Erro ao salvar metas: {e}")

    # ... Restante do código (exibição da tabela de metas cadastradas) mantém igual ...
    st.divider()
    st.markdown("### 📋 Todas as Metas Cadastradas")
    try:
        metas = manager.carregar_metas_cadastradas()
        if metas:
            df = pd.DataFrame(metas)
            df["Meta Mensal"] = df["Meta Mensal"].apply(formatar_valor)
            df["Meta Semanal"] = df["Meta Semanal"].apply(formatar_percentual)
            df["% Prata"] = df["% Prata"].apply(formatar_percentual)
            df["% Bronze"] = df["% Bronze"].apply(formatar_percentual)
            for dia in DIAS_SEMANA:
                if dia in df.columns:
                    df[dia] = df[dia].apply(formatar_percentual)
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("Nenhuma meta cadastrada ainda.")
    except Exception as e:
        st.error(f"Erro ao exibir metas: {e}")