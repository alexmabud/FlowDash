# flowdash_pages/finance_logic.py
from __future__ import annotations

import re
import sqlite3
import json
from datetime import date, timedelta
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
    Novo Cálculo Baseado em Checkpoint (Fechamento)
    1. Busca último fechamento válido (<= data_alvo).
    2. Se data fechamento == data_alvo: Retorna valores salvos (Verdade Absoluta).
    3. Se data fechamento < data_alvo: 
       Saldo = Saldo Fechamento + Movimentos(Data Fechamento + 1 até data_alvo).
    """
    if not bancos_ativos:
        return {}
    
    data_alvo_str = str(data_alvo)
    
    # 1. Busca Checkpoint
    # Tenta pegar colunas legadas e JSON novo
    # Precisamos tratar o caso onde as colunas podem não existir em bancos antigos, mas o script de migração deve ter criado.
    # Por segurança, fazemos select * ou colunas específicas se tiver certeza. 
    # O _garantir_colunas_fechamento cria: bancos_detalhe, banco_1..4
    try:
        row = conn.execute("""
            SELECT data, bancos_detalhe, banco_1, banco_2, banco_3, banco_4 
            FROM fechamento_caixa 
            WHERE DATE(data) <= DATE(?) 
            ORDER BY DATE(data) DESC LIMIT 1
        """, (data_alvo_str,)).fetchone()
    except Exception:
        row = None

    saldo_inicial = {b: 0.0 for b in bancos_ativos}
    data_inicio_calc = date(2000, 1, 1)

    if row:
        r_data, r_json, r_b1, r_b2, r_b3, r_b4 = row
        # Parse data do checkpoint
        if isinstance(r_data, date):
            r_date_obj = r_data
        else:
            try:
                r_date_obj = date.fromisoformat(r_data)
            except:
                r_date_obj = date(2000, 1, 1)

        # Parse valores salvos (JSON prioridade)
        saldos_salvos = {}
        if r_json:
            try: 
                saldos_salvos = json.loads(r_json)
            except: 
                pass
        
        # Fallback colunas legadas se JSON falhar
        if not saldos_salvos:
            # Mapeamento hardcoded baseado no fechamento.py
            # Inter=b1, Bradesco=b2, Infinite*=b3, Caixa=b4 (geralmente b4 era outro, mas vamos focar nos principais)
            # A melhor aposta é o nome exato. Se não tiver JSON, paciência, o saldo pode começar zerado ou aproximado.
            map_legado = {
                'Inter': r_b1,
                'Bradesco': r_b2,
                'InfinitePay': r_b3
            }
            for nome_banco, val in map_legado.items():
                if val is not None:
                    saldos_salvos[nome_banco] = float(val)

        # CENÁRIO A: Dia já fechado hoje -> Retorna o salvo
        if r_date_obj == data_alvo:
            # Retorna apenas para bancos ativos
            return {b: float(saldos_salvos.get(b, 0.0)) for b in bancos_ativos}

        # CENÁRIO B: Checkpoint passado -> Define saldo inicial e nova data de partida
        # A lógica do usuário pede explicitamente para filtrar onde Data > Data_Checkpoint
        # Portanto, não usaremos mais "checkpoint + 1 day" e sim a data do checkpoint diretamente na query com operador >
        data_base_checkpoint = r_date_obj
        for b in bancos_ativos:
            saldo_inicial[b] = float(saldos_salvos.get(b, 0.0))


    # ================= CALCULAR MOVIMENTOS [> data_base_checkpoint ... <= data_alvo] =================
    
    dt_base_str = str(data_base_checkpoint)
    
    saldos = saldo_inicial.copy()
    bancos_map = {_norm(b): b for b in bancos_ativos}

    # 1. Movimentações Bancárias (Transferências)
    try:
        # User REQ: DATE(data) > DATE(checkpoint)
        df_mov = _read_sql(conn, """
            SELECT banco, tipo, valor 
            FROM movimentacoes_bancarias 
            WHERE DATE(data) > DATE(?) AND DATE(data) <= DATE(?)
            AND (observacao IS NULL OR NOT observacao LIKE 'Lançamento VENDA%')
            AND (observacao IS NULL OR NOT observacao LIKE 'Venda%')
        """, (dt_base_str, data_alvo_str))

        
        if not df_mov.empty:
            df_mov['banco_norm'] = df_mov['banco'].apply(_norm)
            df_mov['valor'] = pd.to_numeric(df_mov['valor'], errors='coerce').fillna(0.0)
            for _, mrow in df_mov.iterrows():
                bn = mrow['banco_norm']
                tipo = (mrow['tipo'] or '').lower().strip()
                val = float(mrow['valor'])
                if bn in bancos_map:
                    real = bancos_map[bn]
                    if tipo == 'entrada': saldos[real] += val
                    elif tipo == 'saida': saldos[real] -= val

    except Exception as e:
        print(f"Erro movs: {e}")

    # 2. Saídas (Despesas) - Pela Data de Pagamento (Data)
    try:
        df_saida = _read_sql(conn, """
            SELECT Banco_Saida, Valor 
            FROM saida 
            WHERE DATE(data) > DATE(?) AND DATE(data) <= DATE(?)
            AND Banco_Saida IS NOT NULL AND TRIM(Banco_Saida) <> ''
        """, (dt_base_str, data_alvo_str))
        
        if not df_saida.empty:
            df_saida['banco_norm'] = df_saida['Banco_Saida'].apply(_norm)
            df_saida['Valor'] = pd.to_numeric(df_saida['Valor'], errors='coerce').fillna(0.0)
            for bn, val in df_saida.groupby('banco_norm')['Valor'].sum().items():
                if bn in bancos_map:
                    saldos[bancos_map[bn]] -= float(val)


    except Exception as e:
        print(f"Erro saidas: {e}")

    # 3. Entradas (Vendas) - Regime de Caixa (Data_Liq)
    try:
        # Usa COALESCE para garantir que se Data_Liq for nula, use Data (segurança), 
        # mas o requisito pede Data_Liq explicitamente.
        df_vendas = _read_sql(conn, """
            SELECT maquineta, Forma_de_Pagamento, Bandeira, Parcelas, valor_liquido 
            FROM entrada 
            WHERE DATE(Data_Liq) > DATE(?) AND DATE(Data_Liq) <= DATE(?)
        """, (dt_base_str, data_alvo_str))
        
        df_taxas = _read_sql(conn, "SELECT maquineta, forma_pagamento, bandeira, parcelas, banco_destino FROM taxas_maquinas")
        
        if not df_vendas.empty and not df_taxas.empty:
            # Normalização (mesma lógica anterior)
            df_vendas['k_maq'] = df_vendas['maquineta'].astype(str).str.strip().str.upper()
            df_vendas['k_forma'] = df_vendas['Forma_de_Pagamento'].astype(str).str.strip().str.upper()
            df_vendas['k_band'] = df_vendas['Bandeira'].astype(str).str.strip().str.upper()
            df_vendas['k_parc'] = pd.to_numeric(df_vendas['Parcelas'], errors='coerce').fillna(1).astype(int)
            
            df_taxas['k_maq'] = df_taxas['maquineta'].astype(str).str.strip().str.upper()
            df_taxas['k_forma'] = df_taxas['forma_pagamento'].astype(str).str.strip().str.upper()
            df_taxas['k_band'] = df_taxas['bandeira'].astype(str).str.strip().str.upper()
            df_taxas['k_parc'] = pd.to_numeric(df_taxas['parcelas'], errors='coerce').fillna(1).astype(int)
            
            df_merged = pd.merge(df_vendas, df_taxas[['k_maq', 'k_forma', 'k_band', 'k_parc', 'banco_destino']], 
                                 on=['k_maq', 'k_forma', 'k_band', 'k_parc'], how='left')
            
            # Lógica de Inferência (Fallback) igual ao Fechamento
            def _inferir_banco_row(r):
                # 1. Prioridade: Banco vindo do cadastro de taxas
                b = r['banco_destino']
                if pd.notnull(b) and str(b).strip() != "":
                    return b
                
                # 2. Fallback: Nome da Maquineta
                m = str(r['maquineta']).upper()
                if 'INFINITE' in m or 'INFINITY' in m: return 'InfinitePay'
                if 'INTER' in m: return 'Inter'
                if 'BRADESCO' in m: return 'Bradesco'
                if 'PAGSEGURO' in m or 'PAGBANK' in m: return 'PagBank'
                if 'MERCADO' in m: return 'Mercado Pago'
                if 'STONE' in m or 'TON' in m: return 'Stone'
                
                return None

            df_merged['banco_final'] = df_merged.apply(_inferir_banco_row, axis=1)
            # Normaliza para comparação, mas agrupa pelo nome real para exibição
            
            for nome_banco, val in df_merged.groupby('banco_final')['valor_liquido'].sum().items():
                if not nome_banco: continue
                n = _norm(nome_banco)
                
                # Se bater com um banco cadastrado, usa a chave dele
                if n in bancos_map:
                    chave = bancos_map[n]
                    saldos[chave] += float(val)
                else:
                    # Se for novo (inferred), adiciona dinamicamente
                    # Garante Title Case para ficar bonito
                    chave = str(nome_banco).strip()
                    saldos[chave] = saldos.get(chave, 0.0) + float(val)

    except Exception as e:
        print(f"Erro entradas: {e}")

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
