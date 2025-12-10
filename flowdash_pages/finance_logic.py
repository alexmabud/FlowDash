# flowdash_pages/finance_logic.py
from __future__ import annotations

import re
import sqlite3
from datetime import date
import pandas as pd

# ==============================================================================
# HELPERS
# ==============================================================================

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

# ==============================================================================
# LOGIC
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
        print(f"Erro ao sincronizar colunas de bancos: {e}")

def _get_saldos_bancos_acumulados(conn: sqlite3.Connection, data_alvo: date, bancos_ativos: list[str]) -> dict[str, float]:
    """
    Calcula o saldo acumulado dos bancos considerando:
    1. Base: Saldos/Ajustes da tabela saldos_bancos (soma de todas as linhas <= data)
    2. Movimentações: Transferências (Entrada/Saída) em movimentacoes_bancarias
    3. Saídas: Pagamentos onde Banco_Saida = banco
    4. Entradas (Vendas): Lookup na tabela taxas_maquinas para rotear vendas p/ banco correto
    5. REGIME DE CAIXA: Usa Data_Liq para entradas.
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
    # USANDO DATA_LIQ (Regime de Caixa) conforme solicitado
    try:
        # Carrega Vendas
        df_vendas = _read_sql(conn, 
            """
            SELECT maquineta, Forma_de_Pagamento, Bandeira, Parcelas, valor_liquido 
            FROM entrada 
            WHERE DATE(Data_Liq) <= DATE(?)
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

# ========================= HELPERS DE FECHAMENTO =========================
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
