# flowdash_pages/finance_logic.py
from __future__ import annotations

import sqlite3
import json
import pandas as pd
from datetime import date, datetime, timedelta

# ==============================================================================
# 1. HELPERS GENÉRICOS DE SQL E DADOS
# ==============================================================================

def _read_sql(conn: sqlite3.Connection, query: str, params=None) -> pd.DataFrame:
    """Helper curto para pandas read_sql."""
    return pd.read_sql(query, conn, params=params or ())

def _verificar_fechamento_dia(conn: sqlite3.Connection, data_ref: date) -> bool:
    """Verifica se existe fechamento para a data."""
    try:
        return bool(conn.execute("SELECT 1 FROM fechamento_caixa WHERE DATE(data)=DATE(?)", (str(data_ref),)).fetchone())
    except:
        return False

def _carregar_tabela(conn: sqlite3.Connection, tabela: str) -> pd.DataFrame:
    """Carrega tabela inteira (uso com cuidado em tabelas grandes)."""
    return pd.read_sql(f"SELECT * FROM {tabela}", conn)

def _norm(s: str) -> str:
    """Normaliza strings para comparação (upper, strip)."""
    return (s or "").strip().upper()

def _find_col(cols: list[str], candidates: list[str]) -> str | None:
    """Encontra primeira coluna candidata existente na lista cols."""
    c_lower = [c.lower() for c in cols]
    for cand in candidates:
        if cand.lower() in c_lower:
            idx = c_lower.index(cand.lower())
            return cols[idx]
    return None

def _parse_date_col(df: pd.DataFrame, col: str) -> pd.Series:
    """Converte coluna para datetime, tratando erros."""
    if col not in df.columns:
        return pd.Series([None]*len(df), index=df.index)
    return pd.to_datetime(df[col], errors="coerce")


# ==============================================================================
# 2. GESTÃO DE COLUNAS DE BANCOS (DINÂMICO)
# ==============================================================================

def _get_bancos_ativos(conn: sqlite3.Connection) -> list[str]:
    """
    Retorna lista de nomes de bancos cadastrados.
    Exclui variantes de 'Caixa' físico para evitar card duplicado.
    """
    try:
        query = """
            SELECT nome FROM bancos 
            WHERE UPPER(TRIM(nome)) NOT IN ('CAIXA', 'CAIXA 2', 'CAIXA LOJA', 'CAIXA FISICO') 
            ORDER BY nome
        """
        df_b = pd.read_sql(query, conn)
        if not df_b.empty:
            return df_b["nome"].tolist()
    except Exception:
        pass
    return ["Inter", "Bradesco", "InfinitePay"]

def _sincronizar_colunas_saldos_bancos(conn: sqlite3.Connection, bancos: list[str]) -> None:
    try:
        cursor = conn.execute("PRAGMA table_info(saldos_bancos)")
        cols_existentes = {row[1] for row in cursor.fetchall()}
        
        for banco in bancos:
            if banco not in cols_existentes:
                try:
                    conn.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{banco}" REAL DEFAULT 0.0')
                except Exception:
                    pass
    except Exception:
        pass


# ==============================================================================
# 3. CÁLCULO DE SALDOS ACUMULADOS (CORE LOGIC)
# ==============================================================================

def _somar_entradas_liquidas_banco(conn: sqlite3.Connection, banco_alvo: str, data_corte: date) -> float:
    """
    Soma entradas (Vendas).
    ESTRATÉGIA HÍBRIDA (SEM MIGRAR BANCO):
    1. Tenta buscar na tabela 'entrada' pela coluna 'banco_destino'.
    2. Se der erro (coluna não existe), busca na tabela 'movimentacoes_bancarias' (fallback).
    """
    data_iso = data_corte.strftime("%Y-%m-%d")
    
    # Tentativa 1: Tabela entrada (Ideal)
    try:
        query = """
            SELECT SUM(COALESCE(valor_liquido, valor, 0)) 
            FROM entrada 
            WHERE 
                COALESCE(banco_destino, 
                    CASE 
                        WHEN UPPER(maquineta) LIKE '%INFINITE%' THEN 'InfinitePay'
                        WHEN UPPER(maquineta) LIKE '%INTER%' THEN 'Inter'
                        WHEN UPPER(maquineta) LIKE '%BRADESCO%' THEN 'Bradesco'
                        WHEN UPPER(maquineta) LIKE '%PAG%' THEN 'PagBank'
                        WHEN UPPER(maquineta) LIKE '%MERCADO%' THEN 'Mercado Pago'
                        WHEN UPPER(maquineta) LIKE '%STONE%' OR UPPER(maquineta) LIKE '%TON%' THEN 'Stone'
                        ELSE NULL 
                    END
                ) = ? 
                AND DATE(COALESCE(Data_Liq, Data)) <= DATE(?)
        """
        cur = conn.execute(query, (banco_alvo, data_iso))
        val = cur.fetchone()[0]
        return float(val or 0.0)
        
    except Exception: # Fallback para qualquer erro de esquema
        # Fallback: A coluna banco_destino não existe no banco do usuário.
        # Vamos somar pela movimentacoes_bancarias, que sabemos que tem a coluna 'banco'.
        # Filtramos origem 'venda' ou 'entrada' para pegar as vendas.
        try:
            q_fallback = """
                SELECT SUM(valor) FROM movimentacoes_bancarias
                WHERE banco = ?
                AND tipo = 'entrada'
                AND LOWER(COALESCE(origem,'')) IN ('venda', 'entrada', 'pix')
                AND DATE(data) <= DATE(?)
            """
            val = conn.execute(q_fallback, (banco_alvo, data_iso)).fetchone()[0]
            return float(val or 0.0)
        except Exception:
            return 0.0

def _somar_saidas_banco(conn: sqlite3.Connection, banco_alvo: str, data_corte: date) -> float:
    """Soma saídas da tabela 'saida'."""
    try:
        data_iso = data_corte.strftime("%Y-%m-%d")
        cols = [r[1] for r in conn.execute("PRAGMA table_info(saida)")]
        # Usa helper para check case-insensitive
        col_banco = _find_col(cols, ["banco", "conta"])
        
        if not col_banco:
            return 0.0
            
        query = f"SELECT SUM(valor) FROM saida WHERE {col_banco} = ? AND DATE(data) <= DATE(?)"
        cur = conn.execute(query, (banco_alvo, data_iso))
        val = cur.fetchone()[0]
        return float(val or 0.0)
        
    except Exception:
        return 0.0

def _somar_movimentacoes_bancarias(conn: sqlite3.Connection, banco_alvo: str, data_corte: date) -> float:
    """
    Soma movimentações extras (transferências, ajustes).
    IMPORTANTE: Exclui 'venda', 'entrada' e 'saida' para não duplicar com as funções acima.
    """
    try:
        data_iso = data_corte.strftime("%Y-%m-%d")
        
        # ENTRADAS: Ignora o que já contamos como venda (seja na tabela entrada ou no fallback)
        q_in = """
            SELECT SUM(valor) FROM movimentacoes_bancarias 
            WHERE 
                banco = ? 
                AND tipo = 'entrada'
                AND DATE(data) <= DATE(?)
                AND LOWER(COALESCE(origem,'')) NOT IN ('entrada', 'venda', 'saida', 'pix', 'lancamentos') 
        """
        val_in = conn.execute(q_in, (banco_alvo, data_iso)).fetchone()[0] or 0.0

        # SAÍDAS: Ignora o que já veio da tabela saida
        q_out = """
            SELECT SUM(valor) FROM movimentacoes_bancarias 
            WHERE 
                banco = ? 
                AND tipo = 'saida'
                AND DATE(data) <= DATE(?)
                AND LOWER(COALESCE(origem,'')) != 'saida'
        """
        val_out = conn.execute(q_out, (banco_alvo, data_iso)).fetchone()[0] or 0.0
        
        return float(val_in - val_out)
    except Exception:
        return 0.0


def _get_saldos_bancos_acumulados(conn: sqlite3.Connection, data_ref: date, bancos_ativos: list[str]) -> dict[str, float]:
    """Calcula o saldo acumulado de cada banco até data_ref."""
    bancos_reais = [b for b in bancos_ativos if b.upper() not in ('CAIXA', 'CAIXA 2')]
    saldos = {b: 0.0 for b in bancos_reais}
    data_iso = data_ref.strftime("%Y-%m-%d")

    try:
        # Busca snapshot
        query_snap = "SELECT * FROM saldos_bancos WHERE DATE(data) <= DATE(?) ORDER BY data DESC LIMIT 1"
        df_snap = pd.read_sql(query_snap, conn, params=(data_iso,))
        
        data_inicio_calc = date(2000, 1, 1)

        if not df_snap.empty:
            row = df_snap.iloc[0]
            snap_date = pd.to_datetime(str(row['data'])).date()
            
            for b in bancos_reais:
                saldos[b] = float(row[b] or 0.0) if b in df_snap.columns else 0.0
            
            
            # CORREÇÃO CRÍTICA: Só aceita o snapshot da PRÓPRIA data se o dia estiver FECHADO.
            # Se estiver ABERTO, significa que o snapshot é velho/stale (ex: fechamento cancelado ou em andamento).
            is_closed = _verificar_fechamento_dia(conn, data_ref)
            
            if snap_date == data_ref:
                if is_closed:
                    return saldos
                # Se não está fechado, IGNORA esse snapshot e busca um anterior (ou calcula do zero se não houver anterior)
                # Como o loop abaixo começa de 'data_inicio_calc', precisamos recuar a busca?
                # A query original pegou o "LIMIT 1". Se esse é o stale, ele atrapalha.
                # Solução: Buscar snapshot ANTERIOR a data_ref se o dia estiver aberto.
                pass
            
            # Se o snapshot for menor que data_ref, usamos ele como base normalmente.
            if snap_date < data_ref:
                data_inicio_calc = snap_date 

        str_inicio = data_inicio_calc.strftime("%Y-%m-%d")
        
        # Loop de cálculo delta
        for b in bancos_reais:
            # Atenção: Passamos conn diretamente para usar a lógica de fallback
            val_ent = _somar_entradas_liquidas_delta(conn, b, str_inicio, data_iso)
            val_sai = _somar_saidas_delta(conn, b, str_inicio, data_iso)
            val_mov = _somar_movimentacoes_delta(conn, b, str_inicio, data_iso)
            
            saldos[b] = saldos[b] + val_ent - val_sai + val_mov

    except Exception as e:
        print(f"Erro calculo saldos: {e}")
    
    return saldos

# Helpers Internos para Delta (Replicando a lógica de Try/Except com maior robustez)
def _somar_entradas_liquidas_delta(conn, banco, inicio, fim):
    try:
        q = """SELECT SUM(COALESCE(valor_liquido, valor, 0)) FROM entrada
               WHERE 
                COALESCE(banco_destino, 
                    CASE 
                        WHEN UPPER(maquineta) LIKE '%INFINITE%' THEN 'InfinitePay'
                        WHEN UPPER(maquineta) LIKE '%INTER%' THEN 'Inter'
                        WHEN UPPER(maquineta) LIKE '%BRADESCO%' THEN 'Bradesco'
                        WHEN UPPER(maquineta) LIKE '%PAG%' THEN 'PagBank'
                        WHEN UPPER(maquineta) LIKE '%MERCADO%' THEN 'Mercado Pago'
                        WHEN UPPER(maquineta) LIKE '%STONE%' OR UPPER(maquineta) LIKE '%TON%' THEN 'Stone'
                        ELSE NULL 
                    END
                ) = ? 
                AND DATE(COALESCE(Data_Liq, Data)) > DATE(?)
                AND DATE(COALESCE(Data_Liq, Data)) <= DATE(?)"""
        return conn.execute(q, (banco, inicio, fim)).fetchone()[0] or 0.0
    except Exception: # Catch-all para evitar crash se banco_destino nao existir
        # Fallback Delta
        try:
            q_f = """SELECT SUM(valor) FROM movimentacoes_bancarias
                     WHERE banco = ? AND tipo = 'entrada'
                     AND LOWER(COALESCE(origem,'')) IN ('venda', 'entrada', 'pix')
                     AND DATE(data) > DATE(?) AND DATE(data) <= DATE(?)"""
            return conn.execute(q_f, (banco, inicio, fim)).fetchone()[0] or 0.0
        except Exception:
            return 0.0

def _somar_saidas_delta(conn, banco, inicio, fim):
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(saida)")]
        # Corrigido: Uso de _find_col para achar 'banco' ou 'conta' de forma case-insensitive
        col = _find_col(cols, ["banco", "conta"])
        if not col: return 0.0
        
        q = f"SELECT SUM(valor) FROM saida WHERE {col} = ? AND DATE(data) > DATE(?) AND DATE(data) <= DATE(?)"
        return conn.execute(q, (banco, inicio, fim)).fetchone()[0] or 0.0
    except Exception:
        return 0.0

def _somar_movimentacoes_delta(conn, banco, inicio, fim):
    try:
        qi = """SELECT SUM(valor) FROM movimentacoes_bancarias WHERE banco=? AND tipo='entrada'
                AND DATE(data)>DATE(?) AND DATE(data)<=DATE(?)
                AND LOWER(COALESCE(origem,'')) NOT IN ('entrada','venda','saida', 'pix', 'lancamentos')"""
        vi = conn.execute(qi, (banco, inicio, fim)).fetchone()[0] or 0.0
        
        qo = """SELECT SUM(valor) FROM movimentacoes_bancarias WHERE banco=? AND tipo='saida'
                AND DATE(data)>DATE(?) AND DATE(data)<=DATE(?)
                AND LOWER(COALESCE(origem,'')) != 'saida'"""
        vo = conn.execute(qo, (banco, inicio, fim)).fetchone()[0] or 0.0
        return vi - vo
    except Exception:
        return 0.0

# ==============================================================================
# 4. FUNÇÃO HÍBRIDA DE SOMA (COMPATIBILIDADE DASHBOARD)
# ==============================================================================

def _somar_bancos_totais(obj: dict | str, *args) -> float | dict:
    if isinstance(obj, dict):
        return sum(obj.values())
    if isinstance(obj, str):
        caminho_banco = obj
        try:
            data_ref = args[0] if args else date.today()
            with sqlite3.connect(caminho_banco) as conn:
                bancos = _get_bancos_ativos(conn)
                return _get_saldos_bancos_acumulados(conn, data_ref, bancos)
        except Exception:
            return {}
    return 0.0

# ==============================================================================
# 5. CÁLCULO DE CAIXA / CAIXA 2
# ==============================================================================

def _ultimo_caixas_ate(caminho_banco: str, data_limite: date) -> tuple:
    with sqlite3.connect(caminho_banco) as conn:
        row = conn.execute("SELECT caixa_total, caixa2_total, data FROM saldos_caixas WHERE DATE(data)<=DATE(?) ORDER BY data DESC LIMIT 1", (str(data_limite),)).fetchone()
        if row: return (float(row[0] or 0), float(row[1] or 0), pd.to_datetime(row[2]).date() if row[2] else None)
    return (0.0, 0.0, None)

def _calcular_saldo_projetado(conn, data_ref):
    data_iso = data_ref.strftime("%Y-%m-%d")
    row = conn.execute("SELECT caixa_total, caixa2_total, data FROM saldos_caixas WHERE DATE(data)<=DATE(?) ORDER BY data DESC LIMIT 1", (data_iso,)).fetchone()
    saldo_cx, saldo_cx2, inicio = 0.0, 0.0, date(2000,1,1)
    if row:
        saldo_cx, saldo_cx2 = float(row[0] or 0), float(row[1] or 0)
        snap = pd.to_datetime(row[2]).date()
        if snap == data_ref: return saldo_cx, saldo_cx2
        inicio = snap
    
    si = inicio.strftime("%Y-%m-%d")
    v_din = conn.execute("SELECT SUM(valor) FROM entrada WHERE UPPER(Forma_de_Pagamento)='DINHEIRO' AND DATE(Data)>DATE(?) AND DATE(Data)<=DATE(?)", (si, data_iso)).fetchone()[0] or 0.0
    s_cx = conn.execute("SELECT SUM(valor) FROM saida WHERE origem_dinheiro='Caixa' AND DATE(data)>DATE(?) AND DATE(data)<=DATE(?)", (si, data_iso)).fetchone()[0] or 0.0
    s_cx2 = conn.execute("SELECT SUM(valor) FROM saida WHERE origem_dinheiro='Caixa 2' AND DATE(data)>DATE(?) AND DATE(data)<=DATE(?)", (si, data_iso)).fetchone()[0] or 0.0
    
    def delta_mov(bn):
        i = conn.execute("SELECT SUM(valor) FROM movimentacoes_bancarias WHERE banco=? AND tipo='entrada' AND DATE(data)>DATE(?) AND DATE(data)<=DATE(?) AND LOWER(COALESCE(origem,'')) NOT IN ('entrada','venda','saida','pix')", (bn, si, data_iso)).fetchone()[0] or 0
        o = conn.execute("SELECT SUM(valor) FROM movimentacoes_bancarias WHERE banco=? AND tipo='saida' AND DATE(data)>DATE(?) AND DATE(data)<=DATE(?) AND LOWER(COALESCE(origem,'')) != 'saida'", (bn, si, data_iso)).fetchone()[0] or 0
        return i - o
    
    return saldo_cx + v_din - s_cx + delta_mov('Caixa'), saldo_cx2 - s_cx2 + delta_mov('Caixa 2')

# Helpers de UI
def _dinheiro_e_pix_por_data(caminho_banco, data_ref):
    with sqlite3.connect(caminho_banco) as conn:
        d = str(data_ref)
        vd = conn.execute("SELECT SUM(valor) FROM entrada WHERE UPPER(Forma_de_Pagamento)='DINHEIRO' AND DATE(Data)=DATE(?)", (d,)).fetchone()[0] or 0
        vp = conn.execute("SELECT SUM(valor) FROM entrada WHERE UPPER(Forma_de_Pagamento)='PIX' AND DATE(Data)=DATE(?)", (d,)).fetchone()[0] or 0
        return float(vd), float(vp)

def _cartao_d1_liquido_por_data_liq(caminho_banco, data_liq_ref):
    with sqlite3.connect(caminho_banco) as conn:
        return float(conn.execute("SELECT SUM(valor_liquido) FROM entrada WHERE DATE(Data_Liq)=DATE(?) AND UPPER(Forma_de_Pagamento) NOT IN ('DINHEIRO','PIX')", (str(data_liq_ref),)).fetchone()[0] or 0)

def _saidas_total_do_dia(caminho_banco, data_ref):
    with sqlite3.connect(caminho_banco) as conn:
        return float(conn.execute("SELECT SUM(valor) FROM saida WHERE DATE(data)=DATE(?)", (str(data_ref),)).fetchone()[0] or 0)

def _correcoes_caixa_do_dia(caminho_banco, data_ref):
    with sqlite3.connect(caminho_banco) as conn:
        d = str(data_ref)
        dia = conn.execute("SELECT correcao FROM fechamento_caixa WHERE DATE(data)=DATE(?)", (d,)).fetchone()
        acum = conn.execute("SELECT SUM(correcao) FROM fechamento_caixa WHERE DATE(data)<=DATE(?)", (d,)).fetchone()
        return float(dia[0] if dia else 0), float(acum[0] if acum else 0)

def _carregar_fechamento_existente(conn, data_ref):
    try:
        df = pd.read_sql("SELECT * FROM fechamento_caixa WHERE DATE(data)=DATE(?)", conn, params=(str(data_ref),))
        return df.iloc[0].to_dict() if not df.empty else None
    except: return None

def _verificar_fechamento_dia(conn, data_ref):
    try: return bool(conn.execute("SELECT 1 FROM fechamento_caixa WHERE DATE(data)=DATE(?)", (str(data_ref),)).fetchone())
    except: return False