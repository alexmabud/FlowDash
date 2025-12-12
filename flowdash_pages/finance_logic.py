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
            # retorna o nome real (case sensitive) que está no banco
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
    Retorna lista de nomes de bancos cadastrados na tabela 'bancos'.
    Se não existir, retorna lista padrão fixa.
    """
    try:
        df_b = pd.read_sql("SELECT nome FROM bancos ORDER BY nome", conn)
        if not df_b.empty:
            return df_b["nome"].tolist()
    except Exception:
        pass
    # Fallback caso não haja tabela bancos
    return ["Inter", "Bradesco", "InfinitePay", "Caixa"]

def _sincronizar_colunas_saldos_bancos(conn: sqlite3.Connection, bancos: list[str]) -> None:
    """
    Garante que a tabela 'saldos_bancos' tenha uma coluna para cada banco ativo.
    Faz ALTER TABLE se necessário.
    """
    try:
        # Pega colunas existentes
        cursor = conn.execute("PRAGMA table_info(saldos_bancos)")
        cols_existentes = {row[1] for row in cursor.fetchall()}
        
        for banco in bancos:
            if banco not in cols_existentes:
                # Adiciona coluna REAL DEFAULT 0.0
                try:
                    conn.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{banco}" REAL DEFAULT 0.0')
                except Exception as e:
                    print(f"Erro ao adicionar coluna {banco}: {e}")
    except Exception as e:
        print(f"Erro ao sincronizar colunas saldos_bancos: {e}")


# ==============================================================================
# 3. CÁLCULO DE SALDOS ACUMULADOS (CORE LOGIC)
# ==============================================================================

def _somar_entradas_liquidas_banco(conn: sqlite3.Connection, banco_alvo: str, data_corte: date) -> float:
    """
    Soma 'valor_liquido' da tabela entrada onde:
      1. banco_destino == banco_alvo
      2. Data_Liq <= data_corte (ou Data <= data_corte para Pix/Dinheiro que cai na hora)
    
    CORREÇÃO APLICADA:
    - Agora considera explicitamente o 'banco_destino' gravado na entrada.
    - Se for PIX e tiver banco_destino, entra na conta.
    """
    try:
        data_iso = data_corte.strftime("%Y-%m-%d")
        
        # Query unificada: 
        # Busca tudo que tem esse banco como destino e já liquidou (Data_Liq <= Hoje)
        # COALESCE(Data_Liq, Data) garante que se Data_Liq for nulo (vendas antigas ou manuais), usa Data.
        query = """
            SELECT SUM(COALESCE(valor_liquido, valor, 0)) 
            FROM entrada 
            WHERE 
                banco_destino = ? 
                AND DATE(COALESCE(Data_Liq, Data)) <= DATE(?)
        """
        
        cur = conn.execute(query, (banco_alvo, data_iso))
        val = cur.fetchone()[0]
        return float(val or 0.0)
    except Exception as e:
        print(f"Erro ao somar entradas banco {banco_alvo}: {e}")
        return 0.0

def _somar_saidas_banco(conn: sqlite3.Connection, banco_alvo: str, data_corte: date) -> float:
    """
    Soma saídas da tabela 'saida' onde banco == banco_alvo e data <= data_corte.
    """
    try:
        data_iso = data_corte.strftime("%Y-%m-%d")
        # Tenta identificar coluna de banco na tabela saida
        # Padrão: 'banco' ou 'conta' ou 'origem'
        # Assumindo 'banco' conforme schema padrão
        query = f"""
            SELECT SUM(valor) 
            FROM saida 
            WHERE 
                (banco = ? OR origem_dinheiro = ?)
                AND DATE(data) <= DATE(?)
        """
        # Nota: origem_dinheiro geralmente é 'Caixa', mas vai que alguém botou o banco lá.
        # Ajuste seguro: filtrar explicitamente pela coluna de banco se existir.
        
        # Verificação rápida de colunas
        cols = [r[1] for r in conn.execute("PRAGMA table_info(saida)")]
        col_banco = "banco" if "banco" in cols else ("conta" if "conta" in cols else None)
        
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
    Soma movimentações da tabela 'movimentacoes_bancarias'.
    Entrada (+), Saída (-).
    
    CORREÇÃO APLICADA:
    - Exclui registros onde origem='saida' para evitar DUPLICIDADE com a tabela 'saida'.
    - Exclui registros onde origem='entrada' para evitar DUPLICIDADE com a tabela 'entrada'.
    - Considera apenas transferências, ajustes, depósitos, etc.
    """
    try:
        data_iso = data_corte.strftime("%Y-%m-%d")
        
        # Soma ENTRADAS (tipo='entrada') que NÃO sejam de vendas/saídas já contadas
        q_in = """
            SELECT SUM(valor) FROM movimentacoes_bancarias 
            WHERE 
                banco = ? 
                AND tipo = 'entrada'
                AND DATE(data) <= DATE(?)
                AND LOWER(COALESCE(origem,'')) NOT IN ('entrada', 'venda', 'saida') 
        """
        # Nota: 'origem'='saida' numa entrada seria estorno? Raro, mas por segurança filtramos.
        # O mais importante é filtrar origem='saida' nas SAÍDAS.

        val_in = conn.execute(q_in, (banco_alvo, data_iso)).fetchone()[0] or 0.0

        # Soma SAÍDAS (tipo='saida')
        # IMPORTANTE: Ignorar origem='saida' pois elas já estão na tabela `saida` e são somadas em _somar_saidas_banco
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
        
    except Exception as e:
        print(f"Erro mov bancarias: {e}")
        return 0.0


def _get_saldos_bancos_acumulados(conn: sqlite3.Connection, data_ref: date, bancos_ativos: list[str]) -> dict[str, float]:
    """
    Calcula o saldo acumulado de cada banco até data_ref.
    Lógica de Otimização (Checkpoint):
      1. Busca o último fechamento (snapshot) em 'saldos_bancos' <= data_ref.
      2. Se achar, usa esse valor como base e soma apenas as movimentações (entrada, saida, movs)
         que ocorreram DEPOIS do snapshot ATÉ data_ref.
      3. Se não achar snapshot anterior, soma tudo desde o início dos tempos.
    """
    saldos = {b: 0.0 for b in bancos_ativos}
    data_iso = data_ref.strftime("%Y-%m-%d")

    try:
        # 1. Busca último snapshot válido (fechamento de banco)
        #    Ordena por data DESC para pegar o mais recente anterior ou igual a hoje
        query_snap = f"""
            SELECT * FROM saldos_bancos 
            WHERE DATE(data) <= DATE(?) 
            ORDER BY data DESC LIMIT 1
        """
        df_snap = pd.read_sql(query_snap, conn, params=(data_iso,))
        
        data_inicio_calc = None
        base_saldos = {}

        if not df_snap.empty:
            # Temos um ponto de partida
            row = df_snap.iloc[0]
            snap_date_str = str(row['data'])
            snap_date = pd.to_datetime(snap_date_str).date()
            
            # Carrega saldos do snapshot
            for b in bancos_ativos:
                if b in df_snap.columns:
                    base_saldos[b] = float(row[b] or 0.0)
                else:
                    base_saldos[b] = 0.0
            
            # Se o snapshot for EXATAMENTE a data_ref, e já foi fechado, retornamos ele?
            # Depende. Se o usuário quer ver o "projetado" do dia aberto, precisamos somar o movimento do dia.
            # Se 'saldos_bancos' tem registro hoje, assume-se que é o saldo FINAL ou INICIAL? 
            # No FlowDash, saldos_bancos geralmente é gravado no FECHAMENTO.
            # Então, se DATE(data) == data_ref, já temos o valor fechado.
            
            if snap_date == data_ref:
                return base_saldos # Retorna direto o valor fechado do dia
            
            # Caso contrário, partimos do snapshot e somamos o delta
            saldos = base_saldos
            data_inicio_calc = snap_date # Exclusivo (movimentos > data_inicio_calc)
        
        else:
            # Sem snapshot, calcula do zero (data mínima)
            data_inicio_calc = date(2000, 1, 1)

        # 2. Calcula Movimentações no Intervalo (Data Snapshot < Data <= Data Ref)
        #    Precisamos somar Entradas, Saídas e Movimentações que ocorreram APÓS o snapshot
        
        # Filtro de data para as queries delta
        # OBS: Se data_inicio_calc for muito antiga, pega tudo.
        # Se veio de snapshot, queremos: mov.data > snap_date AND mov.data <= data_ref
        
        str_inicio = data_inicio_calc.strftime("%Y-%m-%d")
        
        for b in bancos_ativos:
            # A) Entradas (Vendas Liquidadas + Pix Direto)
            q_ent = """
                SELECT SUM(COALESCE(valor_liquido, valor, 0)) FROM entrada
                WHERE banco_destino = ? 
                AND DATE(COALESCE(Data_Liq, Data)) > DATE(?)
                AND DATE(COALESCE(Data_Liq, Data)) <= DATE(?)
            """
            val_ent = conn.execute(q_ent, (b, str_inicio, data_iso)).fetchone()[0] or 0.0
            
            # B) Saídas (Despesas)
            # Tenta achar coluna de banco
            cols_saida = [r[1] for r in conn.execute("PRAGMA table_info(saida)")]
            col_b_saida = "banco" if "banco" in cols_saida else "conta"
            
            if col_b_saida:
                q_sai = f"""
                    SELECT SUM(valor) FROM saida
                    WHERE {col_b_saida} = ?
                    AND DATE(data) > DATE(?)
                    AND DATE(data) <= DATE(?)
                """
                val_sai = conn.execute(q_sai, (b, str_inicio, data_iso)).fetchone()[0] or 0.0
            else:
                val_sai = 0.0
            
            # C) Movimentações Bancárias (Transferências/Ajustes) - Excluindo duplicação de Saída/Entrada
            q_mov_in = """
                SELECT SUM(valor) FROM movimentacoes_bancarias
                WHERE banco = ? AND tipo = 'entrada'
                AND DATE(data) > DATE(?) AND DATE(data) <= DATE(?)
                AND LOWER(COALESCE(origem,'')) NOT IN ('entrada', 'venda', 'saida')
            """
            v_mov_in = conn.execute(q_mov_in, (b, str_inicio, data_iso)).fetchone()[0] or 0.0
            
            q_mov_out = """
                SELECT SUM(valor) FROM movimentacoes_bancarias
                WHERE banco = ? AND tipo = 'saida'
                AND DATE(data) > DATE(?) AND DATE(data) <= DATE(?)
                AND LOWER(COALESCE(origem,'')) != 'saida'
            """
            v_mov_out = conn.execute(q_mov_out, (b, str_inicio, data_iso)).fetchone()[0] or 0.0
            
            # Saldo Final = Saldo Inicial (Snapshot) + Entradas - Saídas + Mov_Ent - Mov_Sai
            saldos[b] = saldos[b] + val_ent - val_sai + v_mov_in - v_mov_out

    except Exception as e:
        print(f"Erro calculo saldos bancos acumulados: {e}")
    
    return saldos


# ==============================================================================
# 4. CÁLCULO DE CAIXA / CAIXA 2 (SNAPSHOT E PROJEÇÃO)
# ==============================================================================

def _ultimo_caixas_ate(caminho_banco: str, data_limite: date) -> tuple[float, float, date | None]:
    """
    Retorna (caixa_total, caixa2_total, data_ref_do_snapshot).
    Busca o snapshot mais recente em saldos_caixas <= data_limite.
    """
    with sqlite3.connect(caminho_banco) as conn:
        data_iso = data_limite.strftime("%Y-%m-%d")
        row = conn.execute("""
            SELECT caixa_total, caixa2_total, data
            FROM saldos_caixas
            WHERE DATE(data) <= DATE(?)
            ORDER BY data DESC
            LIMIT 1
        """, (data_iso,)).fetchone()
        
        if row:
            d_ref = pd.to_datetime(row[2]).date() if row[2] else None
            return (float(row[0] or 0.0), float(row[1] or 0.0), d_ref)
        
    return (0.0, 0.0, None)

def _calcular_saldo_projetado(conn: sqlite3.Connection, data_ref: date) -> tuple[float, float]:
    """
    Retorna (saldo_caixa_projetado, saldo_caixa2_projetado) para o dia data_ref.
    Lógica:
      1. Pega último snapshot <= data_ref.
      2. Se snapshot == data_ref, retorna ele (já fechado/atualizado).
      3. Se snapshot < data_ref, soma movimentações (entradas/saidas em dinheiro) no intervalo.
    """
    # 1. Snapshot
    data_iso = data_ref.strftime("%Y-%m-%d")
    row = conn.execute("""
        SELECT caixa_total, caixa2_total, data
        FROM saldos_caixas
        WHERE DATE(data) <= DATE(?)
        ORDER BY data DESC LIMIT 1
    """, (data_iso,)).fetchone()
    
    saldo_cx = 0.0
    saldo_cx2 = 0.0
    data_inicio = date(2000, 1, 1)
    
    if row:
        saldo_cx = float(row[0] or 0.0)
        saldo_cx2 = float(row[1] or 0.0)
        snap_date = pd.to_datetime(row[2]).date()
        
        if snap_date == data_ref:
            # Se já existe registro no dia, assume que ele contém o acumulado até o momento
            # (pois a página de caixa atualiza o snapshot em tempo real quando salva)
            return saldo_cx, saldo_cx2
            
        data_inicio = snap_date
    
    str_inicio = data_inicio.strftime("%Y-%m-%d")
    
    # 2. Delta (Movimentações após snapshot até hoje)
    
    # A) Entradas (Dinheiro) -> Vendas em Dinheiro
    #    Geralmente Venda em Dinheiro vai para "Caixa" (loja).
    #    Caixa 2 geralmente é manual.
    
    venda_din = conn.execute("""
        SELECT SUM(valor) FROM entrada
        WHERE UPPER(Forma_de_Pagamento) = 'DINHEIRO'
        AND DATE(Data) > DATE(?) AND DATE(Data) <= DATE(?)
    """, (str_inicio, data_iso)).fetchone()[0] or 0.0
    
    # B) Saídas (Dinheiro)
    #    Precisamos ver a origem_dinheiro ('Caixa' ou 'Caixa 2')
    
    saida_cx = conn.execute("""
        SELECT SUM(valor) FROM saida
        WHERE origem_dinheiro = 'Caixa'
        AND DATE(data) > DATE(?) AND DATE(data) <= DATE(?)
    """, (str_inicio, data_iso)).fetchone()[0] or 0.0
    
    saida_cx2 = conn.execute("""
        SELECT SUM(valor) FROM saida
        WHERE origem_dinheiro = 'Caixa 2'
        AND DATE(data) > DATE(?) AND DATE(data) <= DATE(?)
    """, (str_inicio, data_iso)).fetchone()[0] or 0.0
    
    # C) Movimentações Bancárias (que afetam caixa)
    #    Ex: Sangria (Saída de Caixa), Suprimento (Entrada), Transferencias
    
    def somar_mov(banco_nome):
        # Entradas
        mi = conn.execute("""
            SELECT SUM(valor) FROM movimentacoes_bancarias
            WHERE banco = ? AND tipo='entrada'
            AND DATE(data) > DATE(?) AND DATE(data) <= DATE(?)
            AND LOWER(COALESCE(origem,'')) NOT IN ('entrada','venda','saida')
        """, (banco_nome, str_inicio, data_iso)).fetchone()[0] or 0.0
        
        # Saidas
        mo = conn.execute("""
            SELECT SUM(valor) FROM movimentacoes_bancarias
            WHERE banco = ? AND tipo='saida'
            AND DATE(data) > DATE(?) AND DATE(data) <= DATE(?)
            AND LOWER(COALESCE(origem,'')) != 'saida'
        """, (banco_nome, str_inicio, data_iso)).fetchone()[0] or 0.0
        return mi - mo

    delta_cx = somar_mov('Caixa')
    delta_cx2 = somar_mov('Caixa 2')
    
    final_cx = saldo_cx + venda_din - saida_cx + delta_cx
    final_cx2 = saldo_cx2 - saida_cx2 + delta_cx2 # Venda dinheiro costuma ir só pro Caixa 1, salvo configuração
    
    return final_cx, final_cx2


# ==============================================================================
# 5. HELPERS ESPECÍFICOS PARA PÁGINA DE FECHAMENTO (UI)
# ==============================================================================

def _dinheiro_e_pix_por_data(caminho_banco: str, data_ref: date) -> tuple[float, float]:
    """Retorna total de vendas DINHEIRO e PIX na data especifica (sem acumular)."""
    with sqlite3.connect(caminho_banco) as conn:
        data_str = str(data_ref)
        # Dinheiro
        vd = conn.execute("""
            SELECT SUM(valor) FROM entrada 
            WHERE UPPER(Forma_de_Pagamento) = 'DINHEIRO' 
            AND DATE(Data) = DATE(?)
        """, (data_str,)).fetchone()[0] or 0.0
        
        # Pix (Total do dia, independente se foi pra banco ou caixa, para exibição no card)
        vp = conn.execute("""
            SELECT SUM(valor) FROM entrada 
            WHERE UPPER(Forma_de_Pagamento) = 'PIX' 
            AND DATE(Data) = DATE(?)
        """, (data_str,)).fetchone()[0] or 0.0
        
        return float(vd), float(vp)

def _cartao_d1_liquido_por_data_liq(caminho_banco: str, data_liq_ref: date) -> float:
    """
    Soma valor liquido de cartões (exclui dinheiro/pix) cuja Data_Liq seja data_liq_ref.
    """
    with sqlite3.connect(caminho_banco) as conn:
        d_str = str(data_liq_ref)
        val = conn.execute("""
            SELECT SUM(valor_liquido) FROM entrada
            WHERE DATE(Data_Liq) = DATE(?)
            AND UPPER(Forma_de_Pagamento) NOT IN ('DINHEIRO', 'PIX')
        """, (d_str,)).fetchone()[0] or 0.0
        return float(val)

def _saidas_total_do_dia(caminho_banco: str, data_ref: date) -> float:
    with sqlite3.connect(caminho_banco) as conn:
        val = conn.execute("SELECT SUM(valor) FROM saida WHERE DATE(data)=DATE(?)", (str(data_ref),)).fetchone()[0]
        return float(val or 0.0)

def _correcoes_caixa_do_dia(caminho_banco: str, data_ref: date) -> tuple[float, float]:
    """
    Retorna (correcao_dia, correcao_acumulada_ate_hoje).
    Lê tabela 'fechamento_caixa'.
    """
    try:
        with sqlite3.connect(caminho_banco) as conn:
            # Correção do dia
            c_dia = conn.execute(
                "SELECT correcao FROM fechamento_caixa WHERE DATE(data)=DATE(?)", 
                (str(data_ref),)
            ).fetchone()
            val_dia = float(c_dia[0]) if c_dia else 0.0
            
            # Acumulado
            c_acum = conn.execute(
                "SELECT SUM(correcao) FROM fechamento_caixa WHERE DATE(data) <= DATE(?)",
                (str(data_ref),)
            ).fetchone()
            val_acum = float(c_acum[0]) if c_acum else 0.0
            
            return val_dia, val_acum
    except:
        return 0.0, 0.0

def _carregar_fechamento_existente(conn: sqlite3.Connection, data_ref: date) -> dict | None:
    try:
        df = pd.read_sql(f"SELECT * FROM fechamento_caixa WHERE DATE(data)=DATE(?)", conn, params=(str(data_ref),))
        if not df.empty:
            return df.iloc[0].to_dict()
    except:
        pass
    return None

def _verificar_fechamento_dia(conn: sqlite3.Connection, data_ref: date) -> bool:
    try:
        row = conn.execute("SELECT 1 FROM fechamento_caixa WHERE DATE(data)=DATE(?)", (str(data_ref),)).fetchone()
        return bool(row)
    except:
        return False

def _somar_bancos_totais(saldos: dict) -> float:
    return sum(saldos.values())
