# flowdash_pages/fechamento/fechamento.py
from __future__ import annotations

import re
import sqlite3
import json
from datetime import date, datetime, timedelta
import pandas as pd
import streamlit as st
from flowdash_pages.utils_timezone import hoje_br

# ==============================================================================
# 1. IMPORTS & UTILS
# ==============================================================================

# Formatação de moeda
try:
    from utils.utils import formatar_moeda as _fmt
except Exception:
    def _fmt(v):
        try:
            return f"R$ {float(v or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except Exception:
            return "R$ 0,00"

# Dependência opcional (Workalendar)
try:
    from workalendar.america import BrazilDistritoFederal
    _HAS_WORKACALENDAR = True
except Exception:
    _HAS_WORKACALENDAR = False


from flowdash_pages.finance_logic import (
    _read_sql, _carregar_tabela, _norm, _find_col, _parse_date_col,
    _get_bancos_ativos, _sincronizar_colunas_saldos_bancos,
    _get_saldos_bancos_acumulados, _calcular_saldo_projetado,
    _somar_bancos_totais, _ultimo_caixas_ate,
    _dinheiro_e_pix_por_data, _cartao_d1_liquido_por_data_liq,
    _saidas_total_do_dia, _correcoes_caixa_do_dia,
    _carregar_fechamento_existente, _verificar_fechamento_dia
)

# ========= Componente visual compartilhado =========
try:
    from flowdash_pages.lancamentos.pagina.ui_cards_pagina import render_card_row  # noqa: F401
except Exception:
    def render_card_row(title: str, items: list[tuple[str, object, bool]]) -> None:
        st.subheader(title)
        cols = st.columns(len(items))
        for col, (label, value, number_always) in zip(cols, items):
            with col:
                st.write(f"**{label}**")
                st.write(str(value))

def _garantir_colunas_fechamento(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fechamento_caixa (
                data DATE PRIMARY KEY,
                saldo_esperado REAL,
                valor_informado REAL,
                diferenca REAL
            )
        """)
        cursor = conn.execute("PRAGMA table_info(fechamento_caixa)")
        colunas_existentes = {row[1] for row in cursor.fetchall()}
        for col, tipo in [
            ("observacao", "TEXT"), ("historico_ajuste", "TEXT"), ("bancos_detalhe", "TEXT"),
            ("caixa", "REAL"), ("caixa_2", "REAL"),
            ("entradas_confirmadas", "REAL"), ("saidas", "REAL"), ("correcao", "REAL"),
             ("banco_1", "REAL"), ("banco_2", "REAL"), ("banco_3", "REAL"), ("banco_4", "REAL"),
             ("caixa_informado", "REAL"), ("caixa2_informado", "REAL")
        ]:
            if col not in colunas_existentes:
                try:
                    conn.execute(f"ALTER TABLE fechamento_caixa ADD COLUMN {col} {tipo}")
                except Exception:
                    pass 
    except Exception as e:
        st.error(f"Erro ao migrar schema de fechamento_caixa: {e}")


# ==============================================================================
# 3. PAGE RENDERER
# ==============================================================================

def pagina_fechamento_caixa(caminho_banco: str):
    if "dt_fechamento" not in st.session_state:
        st.session_state["dt_fechamento"] = hoje_br() # Timezone corrected

    # Feedback Toast (recupera do session_state após rerun)
    if "fechamento_msg" in st.session_state:
        msg, icon = st.session_state.pop("fechamento_msg")
        st.toast(msg, icon=icon)

    # Uso de key='dt_fechamento' gerencia o state automaticamente, evitando o bug do duplo clique
    data_sel = st.date_input("📅 Data do Fechamento", key="dt_fechamento")
    st.markdown(f"**🗓️ Fechamento do dia — {data_sel}**")

    
    conn = sqlite3.connect(caminho_banco)
    _garantir_colunas_fechamento(conn)
    
    bancos_ativos = _get_bancos_ativos(conn)
    _sincronizar_colunas_saldos_bancos(conn, bancos_ativos)
    
    valor_dinheiro, valor_pix = _dinheiro_e_pix_por_data(caminho_banco, data_sel)
    total_cartao_liquido = _cartao_d1_liquido_por_data_liq(caminho_banco, data_sel)
    entradas_total_dia = valor_dinheiro + valor_pix + total_cartao_liquido
    saidas_total_dia = _saidas_total_do_dia(caminho_banco, data_sel)
    corr_dia, corr_acum = _correcoes_caixa_do_dia(caminho_banco, data_sel)
    
    sys_caixa, sys_caixa2 = _calcular_saldo_projetado(conn, data_sel)
    # Alterado: Usa data_sel (hoje) para mostrar saldo acumulado até o momento, igual à pág. Lançamentos.
    sys_bancos = _get_saldos_bancos_acumulados(conn, data_sel, bancos_ativos)
    total_bancos = sum(sys_bancos.values())
    saldo_total_consolidado = sys_caixa + sys_caixa2 + total_bancos
    
    ja_fechado = _verificar_fechamento_dia(conn, data_sel)
    
    dados_salvos = None
    # Feedback de Status do Dia (Fechado ou Aberto)
    if ja_fechado:
        st.toast("⚠️ Este dia já foi fechado. Visualizando histórico.", icon="🔒")
        dados_salvos = _carregar_fechamento_existente(conn, data_sel)
        
        bancos_salvos_dict = {}
        if dados_salvos and dados_salvos.get('bancos_detalhe'):
             try:
                 bancos_salvos_dict = json.loads(dados_salvos['bancos_detalhe'])
             except: pass
    else:
        st.toast("🔓 Dia aberto para fechamento.", icon="📝")

    # ========================== LAYOUT EM CARDS ==========================
    render_card_row("💰 Valores que Entraram Hoje", [
        ("Dinheiro", valor_dinheiro, True),
        ("Pix", valor_pix, True),
        ("Cartão D-1 (Líquido)", total_cartao_liquido, True)
    ])
    
    df_corr = pd.DataFrame([{"Descrição": "Correção do Dia", "Valor": corr_dia}, {"Descrição": "Correção Acumulada", "Valor": corr_acum}])
    render_card_row("📊 Resumo das Movimentações de Hoje", [
        ("Entradas", entradas_total_dia, True),
        ("Saídas", saidas_total_dia, True),
        ("Correções de Caixa", df_corr, False)
    ])
    
    render_card_row("🧾 Saldo em Caixa (Sistema)", [
        ("Caixa (loja)", sys_caixa, True),
        ("Caixa 2 (casa)", sys_caixa2, True)
    ])
    
    if sys_bancos:
        render_card_row("🏦 Saldos em Bancos (Sistema)", [(k, v, True) for k, v in sys_bancos.items()])


        
    render_card_row("💰 Saldo Total (Sistema)", [("Total consolidado", saldo_total_consolidado, True)])
    
    st.markdown("---")
    
    # ========================== CONFERÊNCIA E AJUSTE ==========================
    st.subheader("📝 Conferência e Ajuste (Valores Reais)")
    
    with st.form("form_fechamento_real"):
        c1, c2 = st.columns(2)
        
        def_caixa = float(dados_salvos['caixa_informado']) if dados_salvos and dados_salvos.get('caixa_informado') is not None else float(sys_caixa)
        def_caixa2 = float(dados_salvos['caixa2_informado']) if dados_salvos and dados_salvos.get('caixa2_informado') is not None else float(sys_caixa2)
        
        real_caixa = c1.number_input("Caixa (Loja) Real (R$)", value=def_caixa, step=10.0, format="%.2f")
        real_caixa2 = c2.number_input("Caixa 2 (Casa) Real (R$)", value=def_caixa2, step=10.0, format="%.2f")
        
        st.markdown("**Bancos Real**")
        real_bancos = {}
        if bancos_ativos:
            cols_b = st.columns(3)
            for i, b_nome in enumerate(bancos_ativos):
                val_ini = float(sys_bancos.get(b_nome, 0.0))
                if dados_salvos:
                    if b_nome in bancos_salvos_dict:
                        val_ini = float(bancos_salvos_dict[b_nome])
                    elif b_nome == 'Inter' and dados_salvos.get('banco_1') is not None: val_ini = float(dados_salvos['banco_1'])
                    elif b_nome == 'Bradesco' and dados_salvos.get('banco_2') is not None: val_ini = float(dados_salvos['banco_2'])
                    elif 'Infinite' in b_nome and dados_salvos.get('banco_3') is not None: val_ini = float(dados_salvos['banco_3'])

                with cols_b[i % 3]:
                    real_bancos[b_nome] = st.number_input(f"{b_nome} (R$)", value=val_ini, step=10.0, format="%.2f", key=f"input_real_{b_nome}")
        else:
            st.info("Nenhum banco para conferir.")
        
        def_obs = dados_salvos.get('observacao', "") if dados_salvos else ""
        obs = st.text_area("Observações", value=def_obs, placeholder="Justificativa para diferenças...")
        confirmar = st.checkbox("Confirmo que os valores estão corretos.", value=False)
        
        
        btn_label = "Fechamento Já Realizado" if ja_fechado else "Salvar Fechamento (Ajustar Saldos)"
        salvar = st.form_submit_button(btn_label, disabled=ja_fechado)
        
    if salvar:
        if not confirmar:
            st.toast("Confirme os valores antes de salvar.", icon="⚠️")
        else:
            try:
                total_real = real_caixa + real_caixa2 + sum(real_bancos.values())
                diferenca = total_real - saldo_total_consolidado
                
                ajustes = []
                # Caixas
                diff_caixa = real_caixa - sys_caixa
                if abs(diff_caixa) > 0.01: 
                    cor = "🟢" if diff_caixa > 0 else "🔴"
                    ajustes.append(f"Caixa: {cor} diferença {_fmt(diff_caixa)}")
                
                diff_caixa2 = real_caixa2 - sys_caixa2
                if abs(diff_caixa2) > 0.01: 
                    cor = "🟢" if diff_caixa2 > 0 else "🔴"
                    ajustes.append(f"Caixa 2: {cor} diferença {_fmt(diff_caixa2)}")
                
                bancos_deltas = {}
                for b_col, val_real in real_bancos.items():
                    val_sys = sys_bancos.get(b_col, 0)
                    delta = val_real - val_sys
                    bancos_deltas[b_col] = delta

                    if abs(delta) > 0.01: 
                        cor = "🟢" if delta > 0 else "🔴"
                        ajustes.append(f"{b_col}: {cor} diferença {_fmt(delta)}")
                
                detalhe_bancos = json.dumps(real_bancos, ensure_ascii=False)
                
                cursor = conn.cursor()
                cursor.execute("BEGIN TRANSACTION")
                
                cursor.execute("DELETE FROM fechamento_caixa WHERE DATE(data)=DATE(?)", (str(data_sel),))
                
                v_b_legado = [0.0] * 4
                for k, v in real_bancos.items():
                    if 'Inter' in k: v_b_legado[0] = v
                    elif 'Bradesco' in k: v_b_legado[1] = v
                    elif 'Infinite' in k: v_b_legado[2] = v
                    else: v_b_legado[3] += v

                cursor.execute("""
                    INSERT INTO fechamento_caixa (
                        data, saldo_esperado, valor_informado, diferenca,
                        observacao, historico_ajuste, bancos_detalhe,
                        caixa, caixa_2, banco_1, banco_2, banco_3, banco_4,
                        entradas_confirmadas, saidas, correcao,
                        caixa_informado, caixa2_informado
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    str(data_sel), saldo_total_consolidado, total_real, diferenca,
                    obs, json.dumps(ajustes, ensure_ascii=False), detalhe_bancos,
                    sys_caixa, sys_caixa2, v_b_legado[0], v_b_legado[1], v_b_legado[2], v_b_legado[3],
                    entradas_total_dia, saidas_total_dia, corr_dia,
                    real_caixa, real_caixa2
                ))
                
                cursor.execute("DELETE FROM saldos_caixas WHERE DATE(data)=DATE(?)", (str(data_sel),))
                cursor.execute("""
                    INSERT INTO saldos_caixas (data, caixa, caixa_2, caixa_total, caixa2_total) 
                    VALUES (?, ?, ?, ?, ?)
                """, (str(data_sel), real_caixa, real_caixa2, real_caixa, real_caixa2))
                    
                cursor.execute("DELETE FROM saldos_bancos WHERE DATE(data)=DATE(?)", (str(data_sel),))
                cursor.execute("INSERT INTO saldos_bancos (data) VALUES (?)", (str(data_sel),))
                
                for b_col, delta in bancos_deltas.items():
                    if abs(delta) > 0.0001:
                        cursor.execute(f'UPDATE saldos_bancos SET "{b_col}" = COALESCE("{b_col}",0) + ? WHERE DATE(data)=DATE(?)', (delta, str(data_sel)))
                         
                conn.commit()
                st.session_state["fechamento_msg"] = ("✅ Fechamento Registrado com Sucesso!", "✅")
                st.balloons()
                st.rerun()
                
            except Exception as e:
                conn.rollback()
                st.toast(f"Erro ao salvar: {e}", icon="❌")
            
    conn.close()

    # ========================== HISTÓRICO ==========================
    st.markdown("### 📋 Histórico Completo de Fechamentos")
    try:
        with sqlite3.connect(caminho_banco) as conn:
            df_fech = _read_sql(
                conn,
                """
                SELECT 
                    data as 'Data',
                    banco_1 as 'Inter (Real)',
                    banco_3 as 'InfinitePay (Real)',
                    banco_2 as 'Bradesco (Real)',
                    COALESCE(caixa_informado, caixa) as 'Caixa',
                    COALESCE(caixa2_informado, caixa_2) as 'Caixa 2',
                    entradas_confirmadas as 'Entradas',
                    saidas as 'Saídas',
                    correcao as 'Correções',
                    saldo_esperado as 'Saldo Sistema',
                    valor_informado as 'Saldo Real',
                    diferenca as 'Diferença',
                    historico_ajuste as 'Histórico de Ajustes',
                    observacao as 'Observação'
                FROM fechamento_caixa
                ORDER BY data DESC
                LIMIT 30
                """
            )
    except Exception as e:
        st.error(f"Erro ao ler histórico: {e}")
        df_fech = pd.DataFrame()

    if not df_fech.empty:
        if "Histórico de Ajustes" in df_fech.columns:
            def _fmt_hist(val):
                try:
                    if not val: return "-"
                    lista = json.loads(val)
                    if not lista: return "-"
                    return " | ".join(lista)
                except: return str(val)
            df_fech["Histórico de Ajustes"] = df_fech["Histórico de Ajustes"].apply(_fmt_hist)

        def _style(df):
            try:
                if "Data" in df.columns:
                    df["Data"] = pd.to_datetime(df["Data"]).dt.strftime("%d/%m/%Y")

                cols_moeda = [
                    "Inter (Real)", "InfinitePay (Real)", "Bradesco (Real)",
                    "Caixa", "Caixa 2",
                    "Entradas", "Saídas", "Correções",
                    "Saldo Sistema", "Saldo Real", "Diferença"
                ]
                cols_fmt = [c for c in cols_moeda if c in df.columns]
                fmt_dict = {c: _fmt for c in cols_fmt}
                
                styler = df.style.format(fmt_dict, na_rep="-")
                
                def static_color(color):
                    return f'color: {color}; font-weight: bold;'

                cols_blue = ["Inter (Real)", "InfinitePay (Real)", "Bradesco (Real)", "Caixa", "Caixa 2", "Saldo Real"]
                styler.map(lambda v: static_color('#2980b9'), subset=[c for c in cols_blue if c in df.columns])

                if "Correções" in df.columns:
                    styler.map(lambda v: static_color('#e91e63'), subset=["Correções"])
                if "Observação" in df.columns:
                     styler.map(lambda v: static_color('#8e44ad'), subset=["Observação"])
                if "Diferença" in df.columns:
                    def color_diff(val):
                        try:
                            v = float(val)
                            if v < -0.009: return 'color: #ff4b4b; font-weight: bold'
                            return 'color: #27ae60; font-weight: bold'
                        except: return ''
                    styler.map(color_diff, subset=["Diferença"])
                return styler
            except: return df

        st.dataframe(_style(df_fech), use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum fechamento realizado ainda.")

def render(caminho_banco: str | None = None):
    if caminho_banco:
        st.session_state["caminho_banco"] = caminho_banco
    pagina_fechamento_caixa(caminho_banco or st.session_state.get("caminho_banco"))