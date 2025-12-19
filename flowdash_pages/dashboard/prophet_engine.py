import pandas as pd
pd.set_option('future.no_silent_downcasting', True)
import plotly.graph_objects as go
import streamlit as st
import sqlite3
from datetime import datetime
from typing import Tuple, Optional, Dict

# Tenta importar Prophet
try:
    from prophet import Prophet
    HAS_PROPHET = True
except ImportError:
    Prophet = None
    HAS_PROPHET = False
    print("AVISO: Prophet não instalado. Previsões desativadas.")

# Tenta importar bibliotecas de dados macro
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

# ================= HELPER DE BANCO DE DADOS (PERSISTÊNCIA) =================
def _init_tabela_previsoes(conn):
    """Cria a tabela se não existir."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS historico_previsoes_ia (
            mes_referencia TEXT PRIMARY KEY, -- Formato YYYY-MM
            realista REAL,
            pessimista REAL,
            otimista REAL,
            data_calculo DATETIME
        );
    """)
    conn.commit()

def _buscar_previsao_congelada(db_path: str, mes_ref: str) -> Optional[Dict]:
    """Busca se já existe uma meta congelada para aquele mês."""
    if not db_path: return None
    try:
        with sqlite3.connect(db_path) as conn:
            _init_tabela_previsoes(conn)
            cursor = conn.cursor()
            cursor.execute("SELECT realista, pessimista, otimista FROM historico_previsoes_ia WHERE mes_referencia = ?", (mes_ref,))
            row = cursor.fetchone()
            if row:
                return {
                    'previsto': row[0],
                    'pessimista': row[1],
                    'otimista': row[2],
                    'congelado': True
                }
    except Exception as e:
        print(f"Erro ao buscar previsão congelada: {e}")
    return None

def _salvar_previsao_congelada(db_path: str, mes_ref: str, dados: Dict):
    """Salva a previsão para não mudar mais."""
    if not db_path: return
    try:
        with sqlite3.connect(db_path) as conn:
            _init_tabela_previsoes(conn)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR IGNORE INTO historico_previsoes_ia (mes_referencia, realista, pessimista, otimista, data_calculo)
                VALUES (?, ?, ?, ?, ?)
            """, (mes_ref, dados['yhat'], dados['yhat_lower'], dados['yhat_upper'], datetime.now()))
            conn.commit()
    except Exception as e:
        print(f"Erro ao salvar previsão: {e}")

# ================= LÓGICA MACROECONÔMICA =================
def _buscar_regressores_macro(data_inicio: pd.Timestamp, meses_futuro: int) -> pd.DataFrame:
    """
    Busca os indicadores macroeconômicos combinados.
    """
    hoje = pd.Timestamp.now().normalize()
    data_fim = hoje + pd.DateOffset(months=meses_futuro + 6)
    start_date = data_inicio - pd.DateOffset(months=6) # Margem de segurança

    codigos_bcb = {
        'selic': 432,
        'ipca': 433,
        'dolar': 1,
        'pib_mensal': 24363, # IBC-Br (Proxy do PIB)
        'confianca': 4393    # Índice de Confiança
    }
    
    try:
        if HAS_BCB:
            df_macro = sgs.get(codigos_bcb, start=start_date)
            df_macro = df_macro.resample('MS').mean()
        else:
            raise ImportError("BCB nao instalado")
    except Exception:
        idx = pd.date_range(start=start_date, end=hoje, freq='MS')
        df_macro = pd.DataFrame(index=idx, columns=codigos_bcb.keys())

    # Dados do IBGE (Sidrapy)
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

    s_pmc = buscar_sidra("3416", "564", "11046/40311")
    if s_pmc is not None: df_macro['pmc'] = s_pmc
    else: df_macro['pmc'] = 0

    s_desemprego = buscar_sidra("6381", "4099") 
    if s_desemprego is not None: df_macro['desemprego'] = s_desemprego
    else: df_macro['desemprego'] = 0

    s_renda = buscar_sidra("6381", "5932")
    if s_renda is not None: df_macro['renda_media'] = s_renda
    else: df_macro['renda_media'] = 0

    # Consolidação
    idx_full = pd.date_range(start=df_macro.index.min(), end=data_fim, freq='MS')
    df_macro = df_macro.reindex(idx_full)
    df_macro = df_macro.ffill().bfill().fillna(0).infer_objects(copy=False)
    
    return df_macro

# ================= MOTOR PROPHET =================
def _treinar_e_prever_prophet(df_treino, meses_futuro, regressores_macro=None):
    """Função interna isolada para treinar o modelo."""
    if df_treino.empty: return None, None
    
    data_inicio = df_treino['ds'].min()
    # Busca macro apenas se não foi passado
    if regressores_macro is None:
        regressores_macro = _buscar_regressores_macro(data_inicio, meses_futuro)
    
    df_prophet = df_treino.merge(regressores_macro, left_on='ds', right_index=True, how='left')
    df_prophet = df_prophet.ffill().bfill().fillna(0).infer_objects(copy=False)
    
    modelo = Prophet(
        yearly_seasonality=True, 
        weekly_seasonality=False, 
        daily_seasonality=False, 
        growth='linear',
        seasonality_prior_scale=20.0
    )
    modelo.add_country_holidays(country_name='BR')
    
    regressores_alvo = ['selic', 'ipca', 'desemprego']
    for reg in regressores_alvo:
        if reg in df_prophet.columns:
            modelo.add_regressor(reg)
            
    modelo.fit(df_prophet)
    
    futuro = modelo.make_future_dataframe(periods=meses_futuro + 1, freq='MS')
    futuro = futuro.merge(regressores_macro, left_on='ds', right_index=True, how='left')
    futuro = futuro.ffill().bfill().fillna(0).infer_objects(copy=False)
    
    previsao = modelo.predict(futuro)
    return previsao, df_prophet 

@st.cache_data(ttl=3600, show_spinner=False)
def criar_grafico_previsao(df_vendas_bruto: pd.DataFrame, meses_futuro: int = 12, db_path: str = None) -> Tuple[go.Figure, pd.DataFrame, Dict]:
    if not HAS_PROPHET:
        fig = go.Figure()
        fig.add_annotation(text="Instale 'prophet' (pip install prophet)", showarrow=False, font=dict(color="red"))
        return fig, pd.DataFrame(), {}

    # --- Tratamento Inicial ---
    if df_vendas_bruto.empty: return go.Figure(), pd.DataFrame(), {}
    df = df_vendas_bruto.copy()
    
    cols_lower = {c.lower(): c for c in df.columns}
    col_data = next((cols_lower[c] for c in ['data', 'data_venda', 'dt_venda', 'date'] if c in cols_lower), None)
    col_valor = next((cols_lower[c] for c in ['valor', 'valor_total', 'vlr', 'total'] if c in cols_lower), None)
            
    if not col_data or not col_valor:
        return go.Figure(), pd.DataFrame(), {}

    df['ds'] = pd.to_datetime(df[col_data], errors='coerce')
    df['y'] = pd.to_numeric(df[col_valor], errors='coerce').fillna(0)
    df = df.dropna(subset=['ds'])
    df = df[df['ds'].dt.year >= 2022] # Filtro de qualidade
    
    df_mensal = df.set_index('ds').resample('MS')['y'].sum().reset_index()
    first_valid = df_mensal[df_mensal['y'] > 0].index.min()
    if pd.notna(first_valid): df_mensal = df_mensal.loc[first_valid:].copy()
    
    if len(df_mensal) < 6:
        fig = go.Figure()
        fig.add_annotation(text="Dados insuficientes (mínimo 6 meses)", showarrow=False)
        return fig, pd.DataFrame(), {}

    hoje = pd.Timestamp.now().normalize()
    data_mes_atual = hoje.replace(day=1) # Ex: 01/12/2025
    mes_ref_str = data_mes_atual.strftime('%Y-%m') # "2025-12"

    # ================= 1. LÓGICA DO "ACOMPANHAMENTO" (Mês Atual) =================
    
    metricas_mes_atual = {'data': data_mes_atual}
    
    # Busca o Realizado (isso sempre atualiza, é a venda real)
    df_real_atual = df_mensal[df_mensal['ds'] == data_mes_atual]
    valor_realizado = float(df_real_atual['y'].iloc[0]) if not df_real_atual.empty else 0.0
    metricas_mes_atual['realizado'] = valor_realizado

    # Busca a Meta (congelada ou calcula agora com dados passados)
    meta_congelada = _buscar_previsao_congelada(db_path, mes_ref_str)
    
    if meta_congelada:
        # USA A CONGELADA (DO BANCO)
        metricas_mes_atual.update(meta_congelada)
    else:
        # GERA UMA NOVA (Baseada APENAS no passado) E CONGELA
        # Treina usando tudo estritamente ANTES do mês atual
        df_treino_passado = df_mensal[df_mensal['ds'] < data_mes_atual].copy()
        
        if len(df_treino_passado) >= 6:
            try:
                # Prevemos apenas o próximo passo (mês atual)
                prev_temp, _ = _treinar_e_prever_prophet(df_treino_passado, 1, None)
                row_prev = prev_temp[prev_temp['ds'] == data_mes_atual]
                
                if not row_prev.empty:
                    dados_save = {
                        'yhat': float(row_prev['yhat'].iloc[0]),
                        'yhat_lower': float(row_prev['yhat_lower'].iloc[0]),
                        'yhat_upper': float(row_prev['yhat_upper'].iloc[0])
                    }
                    # SALVA NO BANCO PARA SEMPRE
                    _salvar_previsao_congelada(db_path, mes_ref_str, dados_save)
                    
                    metricas_mes_atual['previsto'] = dados_save['yhat']
                    metricas_mes_atual['pessimista'] = dados_save['yhat_lower']
                    metricas_mes_atual['otimista'] = dados_save['yhat_upper']
            except Exception as e:
                metricas_mes_atual['error'] = str(e)
        else:
            # Dados insuficientes no passado para gerar meta
            metricas_mes_atual['previsto'] = 0.0
            
    # ================= 2. LÓGICA DO GRÁFICO (Futuro Dinâmico) =================
    # Para o gráfico, queremos usar TODO o dado disponível para projetar o futuro distante
    
    df_treino_full = df_mensal[df_mensal['ds'] <= data_mes_atual].copy()
    
    try:
        previsao_full, df_prophet_full = _treinar_e_prever_prophet(df_treino_full, meses_futuro, None)
        
        # --- Montagem do Gráfico ---
        fig = go.Figure()
        
        def _fmt(x):
            try: return f"R$ {float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            except: return "R$ 0,00"
    
        # Histórico
        df_prophet_full['y_fmt'] = df_prophet_full['y'].apply(_fmt)
        fig.add_trace(go.Scatter(
            x=df_prophet_full['ds'], y=df_prophet_full['y'],
            mode='lines+markers', name='Histórico Realizado',
            text=df_prophet_full['y_fmt'], hovertemplate='%{text}<extra></extra>',
            line=dict(color='black', width=2), marker=dict(size=6)
        ))
        
        # Ponto do Mês Atual (Realizado)
        if valor_realizado > 0:
            fig.add_trace(go.Scatter(
                x=[data_mes_atual], y=[valor_realizado],
                mode='markers', name='Realizado (Mês Atual)',
                text=[_fmt(valor_realizado)], hovertemplate='%{text}<extra>Atual</extra>',
                marker=dict(color='#27ae60', size=10, symbol='diamond')
            ))

        # Ponto da Meta Congelada (Visualização no gráfico)
        if 'previsto' in metricas_mes_atual and metricas_mes_atual['previsto'] > 0:
             fig.add_trace(go.Scatter(
                x=[data_mes_atual], y=[metricas_mes_atual['previsto']],
                mode='markers', name='Meta (Congelada)',
                text=[f"Meta: {_fmt(metricas_mes_atual['previsto'])}"],
                hovertemplate='%{text}<extra>Meta IA</extra>',
                marker=dict(color='#9b59b6', size=8, symbol='x')
            ))
        
        # Linha de Previsão (Futuro APÓS mês atual)
        df_futuro_plot = previsao_full[previsao_full['ds'] > data_mes_atual].copy()
        df_futuro_plot['yhat_fmt'] = df_futuro_plot['yhat'].apply(_fmt)
        
        fig.add_trace(go.Scatter(
            x=df_futuro_plot['ds'], y=df_futuro_plot['yhat'],
            mode='lines', name='Previsão Futura',
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
            title="Previsão de Faturamento (IA + Macroeconomia)",
            xaxis_title="Data",
            yaxis_title="Valor (R$)",
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=20, r=20, t=60, b=20)
        )
        
        return fig, df_futuro_plot, metricas_mes_atual

    except Exception as e:
        print(f"ERRO PROPHET: {e}")
        return go.Figure(), pd.DataFrame(), {'error': str(e)}


def calcular_sazonalidade_semanal(df_vendas_bruto: pd.DataFrame) -> list[float]:
    """
    Calcula a % de representatividade média de cada dia da semana.
    """
    if df_vendas_bruto.empty: return [14.3] * 7
    df = df_vendas_bruto.copy()
    cols_lower = {c.lower(): c for c in df.columns}
    col_data = next((cols_lower[c] for c in ['data', 'data_venda', 'dt_venda', 'date'] if c in cols_lower), None)
    col_valor = next((cols_lower[c] for c in ['valor', 'valor_total', 'vlr', 'total'] if c in cols_lower), None)
    if not col_data or not col_valor: return [14.3] * 7
    df['ds'] = pd.to_datetime(df[col_data], errors='coerce')
    df['y'] = pd.to_numeric(df[col_valor], errors='coerce').fillna(0)
    df = df.dropna(subset=['ds'])
    data_corte = df['ds'].max() - pd.DateOffset(months=12)
    df = df[df['ds'] >= data_corte]
    df['weekday'] = df['ds'].dt.weekday
    agrupado = df.groupby('weekday')['y'].sum()
    total = agrupado.sum()
    percentuais = []
    for dia in range(7):
        val = agrupado.get(dia, 0.0)
        pct = (val / total * 100.0) if total > 0 else 0.0
        percentuais.append(pct)
    soma_atual = sum(percentuais)
    if soma_atual > 0:
        fator = 100.0 / soma_atual
        percentuais = [p * fator for p in percentuais]
    else: percentuais = [100/7] * 7
    return percentuais

def calcular_share_usuario(df_vendas_bruto: pd.DataFrame, nome_usuario: str) -> float:
    """
    Calcula a % que este usuário representa do total de vendas.
    """
    if df_vendas_bruto.empty or not nome_usuario: return 100.0
    if nome_usuario.upper() == "LOJA": return 100.0
    df = df_vendas_bruto.copy()
    cols_lower = {c.lower(): c for c in df.columns}
    col_user = next((cols_lower[c] for c in ['usuario', 'vendedor', 'user'] if c in cols_lower), None)
    col_valor = next((cols_lower[c] for c in ['valor', 'valor_total'] if c in cols_lower), None)
    if not col_user or not col_valor: return 100.0
    total_loja = df[col_valor].sum()
    total_user = df[df[col_user].astype(str) == nome_usuario][col_valor].sum()
    if total_loja == 0: return 0.0
    return (total_user / total_loja) * 100.0