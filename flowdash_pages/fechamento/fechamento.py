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


def _read_sql(conn: sqlite3.Connection, query: str, params=None) -> pd.DataFrame:
    """Helper para ler SQL retornando DataFrame."""
    return pd.read_sql(query, conn, params=params or ())


def _carregar_tabela(caminho_banco: str, nome: str) -> pd.DataFrame:
    """Carrega uma tabela do SQLite como DataFrame. Retorna vazio se não existir."""
    with sqlite3.connect(caminho_banco) as conn:
        try:
            return _read_sql(conn, f"SELECT * FROM {nome}")
        except Exception:
            return pd.DataFrame()

# ========= Normalização tolerante de nomes de coluna =========
_TRANSLATE = str.maketrans(
    "áàãâäéêèëíìîïóòõôöúùûüçÁÀÃÂÄÉÊÈËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ",
    "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC",
)

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s).translate(_TRANSLATE).lower())

def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    if df is None or df.empty:
        return None
    norm_map = {_norm(c): c for c in df.columns}
    for c in candidates:
        hit = norm_map.get(_norm(c))
        if hit:
            return hit
    return None

def _parse_date_col(df: pd.DataFrame, col: str) -> pd.Series:
    s = df[col].astype(str)
    out = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    
    # ISO com T
    mask_iso = s.str.contains("T", na=False)
    if mask_iso.any():
        parsed = pd.to_datetime(s[mask_iso], utc=True, errors="coerce")
        out.loc[mask_iso] = parsed.dt.tz_localize(None)

    mask_ymd = (~mask_iso) & s.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    if mask_ymd.any():
        out.loc[mask_ymd] = pd.to_datetime(s[mask_ymd], format="%Y-%m-%d", errors="coerce")

    rest = out.isna()
    if rest.any():
        out.loc[rest] = pd.to_datetime(s[rest], dayfirst=True, errors="coerce")

    return out


# ========= Componente visual compartilhado =========
try:
    from flowdash_pages.lancamentos.pagina.ui_cards_pagina import render_card_row  # noqa: F401
except Exception:
    def render_card_row(title: str, items: list[tuple[str, object, bool]]) -> None:
        st.subheader(title)
        cols = st.columns(len(items))
        for col, (label, value, number_always) in zip(cols, items):
            with col:
                if isinstance(value, pd.DataFrame):
                    st.markdown(f"**{label}**")
                    st.dataframe(value, use_container_width=True, hide_index=True)
                else:
                    try:
                        num = float(value or 0.0)
                    except Exception:
                        num = 0.0
                    if number_always:
                        st.metric(label, _fmt(num))
                    else:
                        st.write(label, _fmt(num))


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
# 2. DATA FETCHING & LOGIC
# ==============================================================================

def _get_bancos_ativos(conn: sqlite3.Connection) -> list[str]:
    """Busca os nomes dos bancos na tabela de cadastro."""
    try:
        check = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='bancos_cadastrados'").fetchone()
        if not check:
            return []
        
        rows = conn.execute("SELECT nome FROM bancos_cadastrados ORDER BY id").fetchall()
        return [r[0] for r in rows if r[0]]
    except Exception:
        return []

def _sincronizar_colunas_saldos_bancos(conn: sqlite3.Connection, bancos_ativos: list[str]):
    """Garante que a tabela saldos_bancos tenha uma coluna para cada banco cadastrado."""
    try:
        conn.execute('CREATE TABLE IF NOT EXISTS saldos_bancos (data TEXT)')
        cursor = conn.execute("SELECT * FROM saldos_bancos LIMIT 0")
        colunas_existentes = {d[0] for d in cursor.description}
        
        for banco in bancos_ativos:
            if banco not in colunas_existentes:
                conn.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{banco}" REAL DEFAULT 0.0')
    except Exception as e:
        st.error(f"Erro ao sincronizar colunas de bancos: {e}")

def _dinheiro_e_pix_por_data(caminho_banco: str, data_sel: date) -> tuple[float, float]:
    df = _carregar_tabela(caminho_banco, "entrada")
    if df.empty: return 0.0, 0.0
    c_data = _find_col(df, ["Data", "data", "data_venda"])
    c_forma = _find_col(df, ["Forma_de_Pagamento", "forma"])
    c_val = _find_col(df, ["valor_liquido", "Valor", "valor"])
    if not (c_data and c_forma and c_val): return 0.0, 0.0
    
    df[c_data] = _parse_date_col(df, c_data)
    df_day = df[df[c_data].dt.date == data_sel].copy()
    if df_day.empty: return 0.0, 0.0
    
    formas = df_day[c_forma].astype(str).str.upper().str.strip()
    vals = pd.to_numeric(df_day[c_val], errors="coerce").fillna(0.0)
    
    return float(vals[formas == "DINHEIRO"].sum()), float(vals[formas == "PIX"].sum())

def _cartao_d1_liquido_por_data_liq(caminho_banco: str, data_sel: date) -> float:
    df = _carregar_tabela(caminho_banco, "entrada")
    if df.empty: return 0.0
    c_data_liq = _find_col(df, ["Data_Liq", "data_liq", "dt_liq"])
    c_forma = _find_col(df, ["Forma_de_Pagamento", "forma"])
    c_val = _find_col(df, ["valor_liquido", "Valor", "valor"])
    if not (c_data_liq and c_forma and c_val): return 0.0
    
    df[c_data_liq] = _parse_date_col(df, c_data_liq)
    df_day = df[df[c_data_liq].dt.date == data_sel].copy()
    if df_day.empty: return 0.0
    
    formas = df_day[c_forma].astype(str).str.upper().str.strip()
    vals = pd.to_numeric(df_day[c_val], errors="coerce").fillna(0.0)
    is_cartao = formas.isin(["DEBITO", "CREDITO", "DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"])
    return float(vals[is_cartao].sum())

def _saidas_total_do_dia(caminho_banco: str, data_sel: date) -> float:
    df = _carregar_tabela(caminho_banco, "saida")
    if df.empty: return 0.0
    c_data = _find_col(df, ["data", "dt"])
    c_val = _find_col(df, ["valor", "Valor"])
    if not (c_data and c_val): return 0.0
    df[c_data] = _parse_date_col(df, c_data)
    dia = df[df[c_data].dt.date == data_sel]
    return float(pd.to_numeric(dia[c_val], errors="coerce").fillna(0.0).sum())

def _correcoes_caixa_do_dia(caminho_banco: str, data_sel: date) -> tuple[float, float]:
    df = _carregar_tabela(caminho_banco, "correcao_caixa")
    if df.empty: return 0.0, 0.0
    c_data = _find_col(df, ["data", "dt"])
    c_val = _find_col(df, ["valor", "Valor"])
    if not (c_data and c_val): return 0.0, 0.0
    df[c_data] = _parse_date_col(df, c_data)
    
    dia = df[df[c_data].dt.date == data_sel]
    ate = df[df[c_data].dt.date <= data_sel]
    
    return (
        float(pd.to_numeric(dia[c_val], errors="coerce").sum()),
        float(pd.to_numeric(ate[c_val], errors="coerce").sum())
    )

def _calcular_saldo_projetado(conn: sqlite3.Connection, data_alvo: date) -> tuple[float, float]:
    data_alvo_str = str(data_alvo)
    
    row = conn.execute("""
        SELECT DATE(data), caixa_total, caixa2_total FROM saldos_caixas
        WHERE DATE(data) <= DATE(?) ORDER BY DATE(data) DESC, ROWID DESC LIMIT 1
    """, (data_alvo_str,)).fetchone()
    
    if row:
        data_base, base_caixa, base_caixa2 = row[0], float(row[1] or 0), float(row[2] or 0)
    else:
        data_base, base_caixa, base_caixa2 = '2000-01-01', 0.0, 0.0

    vendas_dinheiro = conn.execute("""
        SELECT SUM(Valor) FROM entrada WHERE TRIM(UPPER(Forma_de_Pagamento)) = 'DINHEIRO'
        AND DATE(Data) > DATE(?) AND DATE(Data) <= DATE(?)
    """, (data_base, data_alvo_str)).fetchone()[0] or 0.0
    
    saidas_caixa = conn.execute("""
        SELECT SUM(Valor) FROM saida WHERE Origem_Dinheiro = 'Caixa'
        AND DATE(Data) > DATE(?) AND DATE(Data) <= DATE(?)
    """, (data_base, data_alvo_str)).fetchone()[0] or 0.0
    
    saidas_caixa2 = conn.execute("""
        SELECT SUM(Valor) FROM saida WHERE Origem_Dinheiro IN ('Caixa 2', 'Caixa 2 (Casa)')
        AND DATE(Data) > DATE(?) AND DATE(Data) <= DATE(?)
    """, (data_base, data_alvo_str)).fetchone()[0] or 0.0

    def _calc_movs(nome):
        e = conn.execute("""
            SELECT SUM(valor) FROM movimentacoes_bancarias WHERE banco = ? AND tipo = 'entrada'
            AND DATE(data) > DATE(?) AND DATE(data) <= DATE(?)
        """, (nome, data_base, data_alvo_str)).fetchone()[0] or 0.0
        s = conn.execute("""
            SELECT SUM(valor) FROM movimentacoes_bancarias WHERE banco = ? AND tipo = 'saida'
            AND DATE(data) > DATE(?) AND DATE(data) <= DATE(?)
        """, (nome, data_base, data_alvo_str)).fetchone()[0] or 0.0
        return e - s

    movs_caixa = _calc_movs('Caixa')
    movs_caixa2 = _calc_movs('Caixa 2')

    sys_caixa = base_caixa + float(vendas_dinheiro) - float(saidas_caixa) + float(movs_caixa)
    sys_caixa2 = base_caixa2 - float(saidas_caixa2) + float(movs_caixa2)
    
    return round(sys_caixa, 2), round(sys_caixa2, 2)

def _get_saldos_bancos_acumulados(conn: sqlite3.Connection, data_alvo: date, bancos_ativos: list[str]) -> dict[str, float]:
    """
    Calcula o saldo acumulado dos bancos considerando:
    1. Base: Saldos/Ajustes da tabela saldos_bancos (soma de todas as linhas <= data)
    2. Movimentações: Transferências (Entrada/Saída) em movimentacoes_bancarias
    3. Saídas: Pagamentos onde Banco_Saida = banco
    4. Entradas (Vendas): Lookup na tabela taxas_maquinas para rotear vendas p/ banco correto
    """
    if not bancos_ativos:
        return {}
    
    data_alvo_str = str(data_alvo)
    
    # Inicializa dicionário com 0.0
    saldos = {b: 0.0 for b in bancos_ativos}
    # Mapa auxiliar para normalização (chave normalizada -> nome real)
    bancos_map = {_norm(b): b for b in bancos_ativos}

    # ================= 1. SALDOS BASE (saldos_bancos) =================
    # Soma TODAS as linhas até a data (pois a tabela armazena deltas/ajustes)
    try:
        df_base = _read_sql(conn, "SELECT * FROM saldos_bancos WHERE DATE(data) <= DATE(?)", (data_alvo_str,))
        if not df_base.empty:
            for col in df_base.columns:
                if col not in ["data", "id"]:
                    col_norm = _norm(col)
                    if col_norm in bancos_map:
                        val = pd.to_numeric(df_base[col], errors='coerce').fillna(0.0).sum()
                        saldos[bancos_map[col_norm]] += float(val)
    except Exception as e:
        print(f"Erro ao calcular base de saldos: {e}")

    # ================= 2. MOVIMENTAÇÕES BANCÁRIAS =================
    try:
        df_mov = _read_sql(conn, 
            """
            SELECT banco, tipo, valor 
            FROM movimentacoes_bancarias 
            WHERE DATE(data) <= DATE(?)
            """, 
            (data_alvo_str,)
        )
        if not df_mov.empty:
            df_mov['banco_norm'] = df_mov['banco'].apply(_norm)
            df_mov['valor'] = pd.to_numeric(df_mov['valor'], errors='coerce').fillna(0.0)
            
            for _, row in df_mov.iterrows():
                bn = row['banco_norm']
                tipo = (row['tipo'] or '').lower().strip()
                val = float(row['valor'])
                
                if bn in bancos_map:
                    real_name = bancos_map[bn]
                    if tipo == 'entrada':
                        saldos[real_name] += val
                    elif tipo == 'saida':
                        saldos[real_name] -= val
    except Exception as e:
        print(f"Erro ao calcular movimentações: {e}")

    # ================= 3. SAÍDAS (DESPESAS PAGAS PELO BANCO) =================
    try:
        df_saida = _read_sql(conn, 
            """
            SELECT Banco_Saida, Valor 
            FROM saida 
            WHERE DATE(data) <= DATE(?) 
              AND Banco_Saida IS NOT NULL 
              AND TRIM(Banco_Saida) <> ''
            """, 
            (data_alvo_str,)
        )
        if not df_saida.empty:
            df_saida['banco_norm'] = df_saida['Banco_Saida'].apply(_norm)
            df_saida['Valor'] = pd.to_numeric(df_saida['Valor'], errors='coerce').fillna(0.0)
            
            # Agrupa por banco e subtrai
            agrupado = df_saida.groupby('banco_norm')['Valor'].sum()
            for bn, val_total in agrupado.items():
                if bn in bancos_map:
                    saldos[bancos_map[bn]] -= float(val_total)
    except Exception as e:
        print(f"Erro ao calcular saídas bancárias: {e}")

    # ================= 4. ENTRADAS (VENDAS) -> LOOKUP TAXAS =================
    try:
        # Carrega Vendas
        df_vendas = _read_sql(conn, 
            """
            SELECT maquineta, Forma_de_Pagamento, Bandeira, Parcelas, valor_liquido 
            FROM entrada 
            WHERE DATE(Data) <= DATE(?)
            """, 
            (data_alvo_str,)
        )
        
        # Carrega Regras de Roteamento (Taxas/Bancos)
        df_taxas = _read_sql(conn, "SELECT maquineta, forma_pagamento, bandeira, parcelas, banco_destino FROM taxas_maquinas")
        
        if not df_vendas.empty and not df_taxas.empty:
            # Normalização p/ Join (Vendas)
            df_vendas['k_maq'] = df_vendas['maquineta'].astype(str).str.strip().str.upper()
            df_vendas['k_forma'] = df_vendas['Forma_de_Pagamento'].astype(str).str.strip().str.upper()
            df_vendas['k_band'] = df_vendas['Bandeira'].astype(str).str.strip().str.upper()
            df_vendas['k_parc'] = pd.to_numeric(df_vendas['Parcelas'], errors='coerce').fillna(1).astype(int)
            
            # Normalização p/ Join (Taxas)
            df_taxas['k_maq'] = df_taxas['maquineta'].astype(str).str.strip().str.upper()
            df_taxas['k_forma'] = df_taxas['forma_pagamento'].astype(str).str.strip().str.upper()
            df_taxas['k_band'] = df_taxas['bandeira'].astype(str).str.strip().str.upper()
            df_taxas['k_parc'] = pd.to_numeric(df_taxas['parcelas'], errors='coerce').fillna(1).astype(int)
            
            # Left Join para descobrir o banco destino de cada venda
            df_merged = pd.merge(
                df_vendas, 
                df_taxas[['k_maq', 'k_forma', 'k_band', 'k_parc', 'banco_destino']], 
                on=['k_maq', 'k_forma', 'k_band', 'k_parc'], 
                how='left'
            )
            
            # Vendas sem match ficam com banco NaN. Vamos ignorar ou logar se necessário.
            # Normaliza o banco de destino encontrado
            df_merged['banco_dest_norm'] = df_merged['banco_destino'].apply(lambda x: _norm(x) if pd.notnull(x) else None)
            
            # Soma valor líquido por banco
            vendas_por_banco = df_merged.groupby('banco_dest_norm')['valor_liquido'].sum()
            
            for bn, val_total in vendas_por_banco.items():
                if bn in bancos_map:
                    saldos[bancos_map[bn]] += float(val_total)
                    
    except Exception as e:
        print(f"Erro ao calcular entradas (vendas reconciliadas): {e}")

    # Arredonda tudo para 2 casas
    return {k: round(v, 2) for k, v in saldos.items()}

def _carregar_fechamento_existente(conn: sqlite3.Connection, data_alvo: date) -> dict | None:
    try:
        row = conn.execute("SELECT * FROM fechamento_caixa WHERE DATE(data)=DATE(?) ORDER BY id DESC LIMIT 1", (str(data_alvo),)).fetchone()
        if not row: return None
        cols = [d[0] for d in conn.execute("SELECT * FROM fechamento_caixa LIMIT 0").description]
        return dict(zip(cols, row))
    except:
        return None

def _verificar_fechamento_dia(conn: sqlite3.Connection, data_alvo: date) -> bool:
    row = conn.execute("SELECT 1 FROM fechamento_caixa WHERE DATE(data) = DATE(?) LIMIT 1", (str(data_alvo),)).fetchone()
    return bool(row)

# ========================= COMPATIBILIDADE DASHBOARD =========================
def _somar_bancos_totais(caminho_banco: str, data_sel: date) -> dict[str, float]:
    """Compatibilidade: Retorna totais acumulados dos bancos."""
    with sqlite3.connect(caminho_banco) as conn:
        bancos = _get_bancos_ativos(conn)
        return _get_saldos_bancos_acumulados(conn, data_sel, bancos)

def _ultimo_caixas_ate(caminho_banco: str, data_sel: date) -> tuple[float, float, date | None]:
    """Compatibilidade: Retorna saldo de caixa (sistema) e data ref."""
    try:
        with sqlite3.connect(caminho_banco) as conn:
            c1, c2 = _calcular_saldo_projetado(conn, data_sel)
            return (c1, c2, data_sel)
    except Exception:
        return (0.0, 0.0, None)

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