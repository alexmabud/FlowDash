import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from prophet import Prophet

from typing import Tuple

@st.cache_data
def criar_grafico_previsao(df_input: pd.DataFrame, meses_futuro: int = 12) -> Tuple[go.Figure, pd.DataFrame]:
    """
    Gera um gráfico de previsão de faturamento usando Prophet.
    
    Args:
        df_input: DataFrame com colunas 'data' e 'valor'.
        meses_futuro: Quantidade de meses para prever.
        
    Returns:
        go.Figure: Gráfico Plotly com histórico e previsão.
    """
    # 1. Preparação dos dados para o Prophet (ds, y)
    df_prophet = df_input.rename(columns={'data': 'ds', 'valor': 'y'}).copy()
    
    # Garante que as datas estejam ordenadas
    df_prophet = df_prophet.sort_values('ds')
    
    # 2. Configuração e Treinamento do Modelo
    # growth='linear' é o padrão, mas deixamos explícito. 
    # yearly_seasonality=True para capturar padrões anuais.
    modelo = Prophet(yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False)
    
    # Adiciona feriados brasileiros
    modelo.add_country_holidays(country_name='BR')
    
    modelo.fit(df_prophet)
    
    # 3. Previsão
    # Cria dataframe futuro mensal (freq='MS' = Month Start)
    futuro = modelo.make_future_dataframe(periods=meses_futuro, freq='MS')
    previsao = modelo.predict(futuro)
    
    # 4. Visualização com Plotly
    fig = go.Figure()
    
    # Helper de formatação
    def _fmt(x):
        return f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    # Histórico (Realizado)
    df_prophet['y_fmt'] = df_prophet['y'].apply(_fmt)
    
    fig.add_trace(go.Scatter(
        x=df_prophet['ds'],
        y=df_prophet['y'],
        mode='lines+markers',
        name='Realizado',
        text=df_prophet['y_fmt'],
        hovertemplate='%{text}<extra></extra>',
        line=dict(color='black', width=2),
        marker=dict(size=6)
    ))
    
    # Previsão (Futuro) - Linha azul pontilhada
    # Filtramos apenas a parte futura para não sobrepor o histórico visualmente, 
    # ou plotamos tudo. O padrão do Prophet é plotar tudo, mas aqui vamos destacar o futuro.
    df_futuro_plot = previsao[previsao['ds'] > df_prophet['ds'].max()].copy()
    df_futuro_plot['yhat_fmt'] = df_futuro_plot['yhat'].apply(_fmt)
    
    fig.add_trace(go.Scatter(
        x=df_futuro_plot['ds'],
        y=df_futuro_plot['yhat'],
        mode='lines',
        name='Previsão',
        text=df_futuro_plot['yhat_fmt'],
        hovertemplate='%{text}<extra>Previsão</extra>',
        line=dict(color='#2980b9', width=2, dash='dot')
    ))
    
    # Intervalo de Confiança (Área Sombreada)
    # Upper Bound
    fig.add_trace(go.Scatter(
        x=df_futuro_plot['ds'],
        y=df_futuro_plot['yhat_upper'],
        mode='lines',
        line=dict(width=0),
        showlegend=False,
        hoverinfo='skip'
    ))
    
    # Lower Bound (com preenchimento 'tonexty')
    fig.add_trace(go.Scatter(
        x=df_futuro_plot['ds'],
        y=df_futuro_plot['yhat_lower'],
        mode='lines',
        line=dict(width=0),
        fill='tonexty',
        fillcolor='rgba(41, 128, 185, 0.2)', # Azul translúcido
        name='Incerteza',
        showlegend=True,
        hoverinfo='skip'
    ))
    
    # Layout
    fig.update_layout(
        title="Previsão de Faturamento (Prophet)",
        xaxis_title="Data",
        yaxis_title="Valor (R$)",
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    
    return fig, df_futuro_plot
