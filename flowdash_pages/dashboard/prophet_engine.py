import pandas as pd
pd.set_option('future.no_silent_downcasting', True)
import plotly.graph_objects as go
import streamlit as st
try:
    from prophet import Prophet
    HAS_PROPHET = True
except ImportError:
    Prophet = None
    HAS_PROPHET = False
    print("AVISO: Prophet não instalado. Previsões desativadas.")

try:
    from bcb import sgs
    HAS_BCB = True
except ImportError:
    HAS_BCB = False

try:
    import sidrapy
    HAS_SIDRAPY = True
except ImportError:
    HAS_SIDRAPY = False

import numpy as np
from typing import Tuple, Optional, Dict

def _buscar_regressores_macro(data_inicio: pd.Timestamp, meses_futuro: int) -> pd.DataFrame:
    """
    Busca os 8 indicadores macroeconômicos combinados.
    Retorna DataFrame mensal (MS) alinhado.
    """
    hoje = pd.Timestamp.now().normalize()
    data_fim = hoje + pd.DateOffset(months=meses_futuro + 6)
    start_date = data_inicio - pd.DateOffset(months=6) # Margem de segurança

    # 1. Dados do BCB (SGS)
    # IBC-Br (24363) é o "PIB Mensal"
    codigos_bcb = {
        'selic': 432,
        'ipca': 433,
        'dolar': 1,
        'pib_mensal': 24363, # IBC-Br (Proxy do PIB)
        'confianca': 4393    # Índice de Confiança de Serviços (FGV via BCB) ou similar disponível
    }
    
    try:
        if HAS_BCB:
            df_macro = sgs.get(codigos_bcb, start=start_date)
            df_macro = df_macro.resample('MS').mean()
        else:
            raise ImportError("BCB nao instalado")
    except Exception:
        # Fallback seguro
        idx = pd.date_range(start=start_date, end=hoje, freq='MS')
        df_macro = pd.DataFrame(index=idx, columns=codigos_bcb.keys())

    # 2. Dados do IBGE (Sidrapy) - PMC, Desemprego, Renda
    def buscar_sidra(table_code, variable, classification=None):
        if not HAS_SIDRAPY:
            return None
        try:
            if classification:
                raw = sidrapy.get_table(table_code=table_code, territorial_level="1", ibge_territorial_code="all", variable=variable, classification=classification)
            else:
                raw = sidrapy.get_table(table_code=table_code, territorial_level="1", ibge_territorial_code="all", variable=variable)
            
            if not raw.empty and 'V' in raw.columns:
                df = raw.iloc[1:].copy()
                df['data'] = pd.to_datetime(df['D2C'], format="%Y%m", errors='coerce')
                df = df.dropna(subset=['data'])
                s = df.set_index('data')['V'].astype(float)
                return s.resample('MS').mean()
        except:
            return None
        return None

    # PMC (Varejo Ampliado) - Tabela 3416
    s_pmc = buscar_sidra("3416", "564", "11046/40311")
    if s_pmc is not None: df_macro['pmc'] = s_pmc
    else: df_macro['pmc'] = 0

    # Taxa de Desocupação (PNAD) - Tabela 6381
    s_desemprego = buscar_sidra("6381", "4099") 
    if s_desemprego is not None: df_macro['desemprego'] = s_desemprego
    else: df_macro['desemprego'] = 0

    # Rendimento Médio Real - Tabela 6381 (Var 5932)
    s_renda = buscar_sidra("6381", "5932")
    if s_renda is not None: df_macro['renda_media'] = s_renda
    else: df_macro['renda_media'] = 0

    # 3. Consolidação
    idx_full = pd.date_range(start=df_macro.index.min(), end=data_fim, freq='MS')
    df_macro = df_macro.reindex(idx_full)
    df_macro = df_macro.ffill().bfill().fillna(0).infer_objects(copy=False)
    
    return df_macro

# from typing import Tuple, Optional, Dict # already imported above

@st.cache_data(ttl=3600, show_spinner=False)
def criar_grafico_previsao(df_vendas_bruto: pd.DataFrame, meses_futuro: int = 12) -> Tuple[go.Figure, pd.DataFrame, Dict]:
    if not HAS_PROPHET:
        fig = go.Figure()
        fig.add_annotation(
            text="Biblioteca 'Prophet' não instalada.<br>Instale para ver previsões (pip install prophet).",
            showarrow=False,
            font=dict(size=14, color="red")
        )
        return fig, pd.DataFrame(), {}

    # --- Parte 1: Tratamento de Vendas (Mantenha a lógica existente de resample) ---
    if df_vendas_bruto.empty: return go.Figure(), pd.DataFrame(), {}
    df = df_vendas_bruto.copy()
    
    # Identificar colunas de data e valor
    cols_lower = {c.lower(): c for c in df.columns}
    
    # Tenta achar coluna de data
    col_data = None
    for cand in ['data', 'data_venda', 'dt_venda', 'date']:
        if cand in cols_lower:
            col_data = cols_lower[cand]
            break
            
    # Tenta achar coluna de valor
    col_valor = None
    for cand in ['valor', 'valor_total', 'vlr', 'total']:
        if cand in cols_lower:
            col_valor = cols_lower[cand]
            break
            
    if not col_data or not col_valor:
        return go.Figure(), pd.DataFrame(), {}

    df['ds'] = pd.to_datetime(df[col_data], errors='coerce')
    df['y'] = pd.to_numeric(df[col_valor], errors='coerce').fillna(0)
    df = df.dropna(subset=['ds'])
    
    # FILTRO DE QUALIDADE: Removemos 2021 (dados de implantação/incompletos)
    df = df[df['ds'].dt.year >= 2022]
    
    # Agrupa por mês (MS)
    df_mensal = df.set_index('ds').resample('MS')['y'].sum().reset_index()
    
    first_valid = df_mensal[df_mensal['y'] > 0].index.min()
    if pd.notna(first_valid): df_mensal = df_mensal.loc[first_valid:].copy()
    
    if len(df_mensal) < 6:
        fig = go.Figure()
        fig.add_annotation(text="Dados insuficientes para previsão (mínimo 6 meses)", showarrow=False)
        return fig, pd.DataFrame(), {}

    # --- AJUSTE CIRÚRGICO: Separar Treino vs Mês Atual ---
    hoje = pd.Timestamp.now().normalize()
    data_atual = hoje.replace(day=1) # Primeiro dia do mês atual
    
    # Treino: Tudo ANTES do mês atual
    df_treino = df_mensal[df_mensal['ds'] < data_atual].copy()
    
    # Dados do Mês Atual (para comparação)
    df_mes_atual = df_mensal[df_mensal['ds'] == data_atual]
    valor_realizado_atual = float(df_mes_atual['y'].iloc[0]) if not df_mes_atual.empty else 0.0
    
    if len(df_treino) < 6:
         # Fallback se sobrar poucos dados após remover o mês atual
         df_treino = df_mensal.copy()

    # --- Parte 2: Buscar 8 Regressores ---
    try:
        data_inicio = df_treino['ds'].min()
        df_macro = _buscar_regressores_macro(data_inicio, meses_futuro)
        
        # --- Parte 3: Merge (Usando df_treino) ---
        df_prophet = df_treino.merge(df_macro, left_on='ds', right_index=True, how='left')
        df_prophet = df_prophet.ffill().bfill().fillna(0).infer_objects(copy=False)
        
        # --- Parte 4: Treino ---
        modelo = Prophet(
            yearly_seasonality=True, 
            weekly_seasonality=False, 
            daily_seasonality=False, 
            growth='linear',
            seasonality_prior_scale=20.0
        )
        modelo.add_country_holidays(country_name='BR')
        
        # LISTA ATUALIZADA - APENAS OS PILARES ESTÁVEIS
        regressores_alvo = [
            'selic',       # Custo do dinheiro/crédito
            'ipca',        # Poder de compra
            'desemprego'   # Renda disponível
        ]
        
        for reg in regressores_alvo:
            if reg in df_prophet.columns:
                modelo.add_regressor(reg)
                
        modelo.fit(df_prophet)
        
        # --- Parte 5: Previsão (Incluindo Mês Atual como "Futuro") ---
        futuro = modelo.make_future_dataframe(periods=meses_futuro + 1, freq='MS')
        futuro = futuro.merge(df_macro, left_on='ds', right_index=True, how='left')
        futuro = futuro.ffill().bfill().fillna(0).infer_objects(copy=False)
        
        previsao = modelo.predict(futuro)
        
        # Extrair previsão para o mês atual
        prev_atual_row = previsao[previsao['ds'] == data_atual]
        valor_previsto_atual = float(prev_atual_row['yhat'].iloc[0]) if not prev_atual_row.empty else 0.0
        valor_pessimista_atual = float(prev_atual_row['yhat_lower'].iloc[0]) if not prev_atual_row.empty else 0.0
        valor_otimista_atual = float(prev_atual_row['yhat_upper'].iloc[0]) if not prev_atual_row.empty else 0.0
        
        metricas_mes_atual = {
            'data': data_atual,
            'realizado': valor_realizado_atual,
            'previsto': valor_previsto_atual,
            'pessimista': valor_pessimista_atual,
            'otimista': valor_otimista_atual
        }
        
        # --- Parte 6: Visualização (Plotly) ---
        fig = go.Figure()
        
        def _fmt(x):
            try:
                return f"R$ {float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            except:
                return "R$ 0,00"
    
        # Histórico (Treino)
        df_prophet['y_fmt'] = df_prophet['y'].apply(_fmt)
        fig.add_trace(go.Scatter(
            x=df_prophet['ds'], y=df_prophet['y'],
            mode='lines+markers', name='Histórico Realizado',
            text=df_prophet['y_fmt'], hovertemplate='%{text}<extra></extra>',
            line=dict(color='black', width=2), marker=dict(size=6)
        ))
        
        # Ponto do Mês Atual (Realizado) - Comparação Visual
        if valor_realizado_atual > 0:
            fig.add_trace(go.Scatter(
                x=[data_atual], y=[valor_realizado_atual],
                mode='markers', name='Realizado (Mês Atual)',
                text=[_fmt(valor_realizado_atual)], hovertemplate='%{text}<extra>Atual</extra>',
                marker=dict(color='#27ae60', size=10, symbol='diamond')
            ))
        
        # Previsão (Começando do Mês Atual em diante)
        # Filtra apenas o futuro para plotar pontilhado
        data_corte_treino = df_prophet['ds'].max()
        df_futuro_plot = previsao[previsao['ds'] > data_corte_treino].copy()
        df_futuro_plot['yhat_fmt'] = df_futuro_plot['yhat'].apply(_fmt)
        
        fig.add_trace(go.Scatter(
            x=df_futuro_plot['ds'], y=df_futuro_plot['yhat'],
            mode='lines', name='Previsão (IA)',
            text=df_futuro_plot['yhat_fmt'], hovertemplate='%{text}<extra>Previsão</extra>',
            line=dict(color='#2980b9', width=2, dash='dot')
        ))
        
        # Intervalo de Confiança
        fig.add_trace(go.Scatter(
            x=df_futuro_plot['ds'], y=df_futuro_plot['yhat_upper'],
            mode='lines', line=dict(width=0), showlegend=False, hoverinfo='skip'
        ))
        fig.add_trace(go.Scatter(
            x=df_futuro_plot['ds'], y=df_futuro_plot['yhat_lower'],
            mode='lines', line=dict(width=0), fill='tonexty',
            fillcolor='rgba(41, 128, 185, 0.2)', name='Incerteza',
            showlegend=True, hoverinfo='skip'
        ))
        
        fig.update_layout(
            title="Previsão de Faturamento (IA + 8 Indicadores Macro)",
            xaxis_title="Data",
            yaxis_title="Valor (R$)",
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=20, r=20, t=60, b=20)
        )
        
        # Remove o mês atual do df_futuro_plot retornado para não duplicar nos cards de "Próximo Mês"
        df_futuro_cards = df_futuro_plot[df_futuro_plot['ds'] > data_atual].copy()
        
        return fig, df_futuro_cards, metricas_mes_atual

    except Exception as e:
        print(f"ERRO PROPHET: {e}")
        fig = go.Figure()
        fig.add_annotation(
            text=f"Erro no motor de previsão: {str(e)[:100]}...<br>Verifique logs.",
            showarrow=False,
            font=dict(size=14, color="red")
        )
        return fig, pd.DataFrame(), {'error': str(e)}
