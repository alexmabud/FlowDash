# flowdash_pages/fechamento/fechamento.py
from __future__ import annotations

import re
import sqlite3
from datetime import date, timedelta, datetime

import pandas as pd
import streamlit as st

# ========= formatação de moeda =========
try:
    from utils.utils import formatar_moeda as _fmt
except Exception:
    def _fmt(v):
        try:
            return f"R$ {float(v or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except Exception:
            return "R$ 0,00"

# --------- dependência opcional (dia útil) ---------
try:
    from workalendar.america import BrazilDistritoFederal
    _HAS_WORKACALENDAR = True
except Exception:
    _HAS_WORKACALENDAR = False


# ================== Acesso ao banco / utilidades ==================
def _read_sql(conn: sqlite3.Connection, query: str, params=None) -> pd.DataFrame:
    """Executa um SELECT e retorna um DataFrame."""
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
    """Normaliza string (minúsculo, sem acento/esp.) para comparação tolerante."""
    return re.sub(r"[^a-z0-9]", "", str(s).translate(_TRANSLATE).lower())

def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Encontra a primeira coluna do DF que corresponde aos candidatos (tolerante)."""
    if df is None or df.empty:
        return None
    norm_map = {_norm(c): c for c in df.columns}
    for c in candidates:
        hit = norm_map.get(_norm(c))
        if hit:
            return hit
    wn_list = [_norm(x) for x in candidates]
    for k, orig in norm_map.items():
        if any(wn in k for wn in wn_list):
            return orig
    return None


# ========= Parse de datas (sem warnings) =========
def _parse_date_col(df: pd.DataFrame, col: str) -> pd.Series:
    """
    Converte a coluna de data lidando com:
      1) ISO 8601 com 'T' e fuso (Z ou ±hh:mm)  -> parse com utc=True, sem dayfirst
      2) ISO simples YYYY-MM-DD                 -> format explícito
      3) Restante (ex. dd/mm/yyyy)              -> dayfirst=True
    Evita .fillna em datetime para não disparar FutureWarning.
    """
    s = df[col].astype(str)
    out = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")

    # 1) ISO com 'T' (e.g., 2025-09-20T12:34:56-03:00)
    mask_iso = s.str.contains("T", na=False)
    if mask_iso.any():
        parsed = pd.to_datetime(s[mask_iso], utc=True, errors="coerce")
        try:
            parsed = parsed.dt.tz_convert("America/Sao_Paulo")
        except Exception:
            pass
        out.loc[mask_iso] = parsed.dt.tz_localize(None)

    # 2) ISO simples YYYY-MM-DD
    mask_ymd = (~mask_iso) & s.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    if mask_ymd.any():
        out.loc[mask_ymd] = pd.to_datetime(s[mask_ymd], format="%Y-%m-%d", errors="coerce")

    # 3) Restante (provável dd/mm/yyyy etc.)
    rest = out.isna()
    if rest.any():
        out.loc[rest] = pd.to_datetime(s[rest], dayfirst=True, errors="coerce")

    return out


# ============= Fechamento existente (para travar o botão) =============
def _fechamento_existe(caminho_banco: str, data_str: str) -> bool:
    try:
        with sqlite3.connect(caminho_banco) as conn:
            r = conn.execute(
                "SELECT 1 FROM fechamento_caixa WHERE DATE(data)=DATE(?) LIMIT 1",
                (data_str,),
            ).fetchone()
            return bool(r)
    except Exception:
        return False


# ================== Consultas auxiliares (legado para salvar) ==================
def _get_saldos_bancos_ate(caminho_banco: str, data_ref: str) -> tuple[float, float, float, float]:
    """
    Retorna os saldos ACUMULADOS (<= data_ref) para 4 bancos legados (banco_1..banco_4).
    Funciona mesmo quando a tabela `saldos_bancos` é alimentada por deltas diários
    (somamos todas as linhas até a data, por coluna).
    """
    try:
        with sqlite3.connect(caminho_banco) as conn:
            df = _read_sql(
                conn,
                """
                SELECT * FROM saldos_bancos
                 WHERE DATE(data) <= DATE(?)
                """,
                (data_ref,),
            )
    except Exception:
        return (0.0, 0.0, 0.0, 0.0)

    if df.empty:
        return (0.0, 0.0, 0.0, 0.0)

    c_data = _find_col(df, ["data", "dt"])
    if c_data:
        df[c_data] = _parse_date_col(df, c_data)

    cols = df.columns.tolist()

    def _pick(hints: list[str]) -> str | None:
        for h in hints:
            for c in cols:
                if _norm(c) == _norm(h) or _norm(h) in _norm(c):
                    return c
        return None

    c_b1 = _pick(["banco_1", "banco1", "inter"])
    c_b2 = _pick(["banco_2", "banco2", "bradesco"])
    c_b3 = _pick(["banco_3", "banco3", "infinitepay"])
    c_b4 = _pick(["banco_4", "banco4", "outros", "outros_bancos", "outros bancos"])

    def _sum_col(col: str | None) -> float:
        if not col or col not in df.columns:
            return 0.0
        try:
            return float(pd.to_numeric(df[col], errors="coerce").fillna(0.0).sum())
        except Exception:
            return 0.0

    b1 = _sum_col(c_b1)
    b2 = _sum_col(c_b2)
    b3 = _sum_col(c_b3)
    b4 = _sum_col(c_b4)

    return (round(b1, 2), round(b2, 2), round(b3, 2), round(b4, 2))


# ================== Cálculos dos cartões do topo ==================
def _dinheiro_e_pix_por_data(caminho_banco: str, data_sel: date) -> tuple[float, float]:
    """
    Soma, no dia selecionado, o valor (prefere COALESCE(valor_liquido, Valor))
    das entradas em DINHEIRO e PIX **filtrando por `entrada.Data`**.
    """
    df = _carregar_tabela(caminho_banco, "entrada")
    if df.empty:
        return 0.0, 0.0

    c_data = _find_col(df, [
        "Data", "data", "data_venda", "dataVenda", "data_lanc", "dataLanc", "data_emissao", "dataEmissao"
    ])
    c_forma = _find_col(df, ["Forma_de_Pagamento", "forma_de_pagamento", "forma_pagamento", "forma"])
    c_val = _find_col(df, ["valor_liquido", "valorLiquido", "valor_liq", "Valor", "valor", "valor_total"])

    if not (c_data and c_forma and c_val):
        return 0.0, 0.0

    df[c_data] = _parse_date_col(df, c_data)
    df_day = df[df[c_data].dt.date == data_sel].copy()
    if df_day.empty:
        return 0.0, 0.0

    formas = df_day[c_forma].astype(str).str.upper().str.strip()
    vals = pd.to_numeric(df_day[c_val], errors="coerce").fillna(0.0)

    total_dinheiro = float(vals[formas == "DINHEIRO"].sum())
    total_pix      = float(vals[formas == "PIX"].sum())
    return round(total_dinheiro, 2), round(total_pix, 2)


def _cartao_d1_liquido_por_data_liq(caminho_banco: str, data_sel: date) -> float:
    """
    Soma, no dia selecionado, o **valor líquido** das entradas em
    DÉBITO/CRÉDITO/LINK_PAGAMENTO **filtrando por `entrada.Data_Liq`**
    (ou seja, o que *caiu hoje*).
    """
    df = _carregar_tabela(caminho_banco, "entrada")
    if df.empty:
        return 0.0

    c_data_liq = _find_col(df, ["Data_Liq", "data_liq", "data_liquidacao", "data_liquidação", "dt_liq", "data_liquid"])
    c_forma = _find_col(df, ["Forma_de_Pagamento", "forma_de_pagamento", "forma_pagamento", "forma"])
    c_val = _find_col(df, ["valor_liquido", "valorLiquido", "valor_liq", "Valor", "valor_total", "valor"])

    if not (c_data_liq and c_forma and c_val):
        return 0.0

    df[c_data_liq] = _parse_date_col(df, c_data_liq)
    df_day = df[df[c_data_liq].dt.date == data_sel].copy()
    if df_day.empty:
        return 0.0

    formas = df_day[c_forma].astype(str).str.upper().str.strip()
    vals = pd.to_numeric(df_day[c_val], errors="coerce").fillna(0.0)
    is_cartao = formas.isin(["DEBITO", "DÉBITO", "CREDITO", "CRÉDITO", "LINK_PAGAMENTO", "LINK PAGAMENTO", "LINK-DE-PAGAMENTO", "LINK DE PAGAMENTO"])
    return float(vals[is_cartao].sum())


# ============== Saldo em Caixa do DIA (sem somatórios; mantém lógica) ==============
def _caixas_totais_no_dia(caminho_banco: str, data_sel: date) -> tuple[float, float]:
    """
    Lê **apenas** o valor do dia em `saldos_caixas`:
      - caixa_total
      - caixa2_total
    para a `data_sel`. Sem somatórios, sem acumular períodos.
    """
    with sqlite3.connect(caminho_banco) as conn:
        try:
            df = _read_sql(
                conn,
                """
                SELECT caixa_total, caixa2_total
                  FROM saldos_caixas
                 WHERE DATE(data) = DATE(?)
                 LIMIT 1
                """,
                (str(data_sel),),
            )
        except Exception:
            return (0.0, 0.0)

    if df.empty:
        return (0.0, 0.0)

    try:
        cx  = float(pd.to_numeric(df.iloc[0]["caixa_total"], errors="coerce") or 0.0)
    except Exception:
        cx = 0.0
    try:
        cx2 = float(pd.to_numeric(df.iloc[0]["caixa2_total"], errors="coerce") or 0.0)
    except Exception:
        cx2 = 0.0
    return (round(cx, 2), round(cx2, 2))


# ============== NOVO: Último saldo salvo ATÉ a data (para EXIBIÇÃO) ==============
def _ultimo_caixas_ate(caminho_banco: str, data_sel: date) -> tuple[float, float, date | None]:
    """
    Retorna o **último** (mais recente) caixa_total e caixa2_total com DATE(data) <= data_sel.
    Uso EXCLUSIVO para EXIBIÇÃO dos cards, sem alterar a lógica de cálculos/salvamento.
    """
    with sqlite3.connect(caminho_banco) as conn:
        try:
            df = _read_sql(
                conn,
                """
                SELECT DATE(data) AS d, caixa_total, caixa2_total
                  FROM saldos_caixas
                 WHERE DATE(data) <= DATE(?)
                 ORDER BY DATE(data) DESC, ROWID DESC
                 LIMIT 1
                """,
                (str(data_sel),),
            )
        except Exception:
            return (0.0, 0.0, None)

    if df.empty:
        return (0.0, 0.0, None)

    try:
        cx  = float(pd.to_numeric(df.iloc[0]["caixa_total"], errors="coerce") or 0.0)
    except Exception:
        cx = 0.0
    try:
        cx2 = float(pd.to_numeric(df.iloc[0]["caixa2_total"], errors="coerce") or 0.0)
    except Exception:
        cx2 = 0.0

    try:
        dref = datetime.strptime(str(df.iloc[0]["d"]), "%Y-%m-%d").date()
    except Exception:
        dref = None

    return (round(cx, 2), round(cx2, 2), dref)


# ============== Somatórios até a data (mantidos para bancos/relatório) ==============
def _somar_bancos_totais(caminho_banco: str, data_sel: date) -> dict[str, float]:
    """Soma (<= data_sel) por coluna/banco em saldos_bancos."""
    df = _carregar_tabela(caminho_banco, "saldos_bancos")
    if df.empty:
        return {}

    c_data = _find_col(df, ["data", "dt"])
    if c_data:
        df[c_data] = _parse_date_col(df, c_data)
        df = df[df[c_data].dt.date <= data_sel].copy()

    ignore = {"id", "created_at", "updated_at"}
    if c_data:
        ignore.add(c_data)

    cols_bancos: list[str] = []
    for c in df.columns:
        lc = c.lower()
        if lc in ignore:
            continue
        if ("banco" in lc) or (lc in {"inter", "bradesco", "infinitepay", "outros bancos", "outros_bancos", "outros"}):
            cols_bancos.append(c)

    if not cols_bancos:
        for c in df.columns:
            if c.lower() in ignore:
                continue
            s = pd.to_numeric(df[c], errors="coerce")
            if s.notna().any():
                cols_bancos.append(c)

    totais = {c: float(pd.to_numeric(df[c], errors="coerce").fillna(0.0).sum()) for c in cols_bancos}
    pretty = {c.replace("_", " ").title(): v for c, v in totais.items()}
    return dict(sorted(pretty.items(), key=lambda x: x[0]))


# ============== Saídas e Correções ==============
def _saidas_total_do_dia(caminho_banco: str, data_sel: date) -> float:
    """Soma, no dia, os valores da tabela `saida` (coluna 'valor')."""
    df = _carregar_tabela(caminho_banco, "saida")
    if df.empty:
        return 0.0
    c_data = _find_col(df, ["data", "dt", "data_saida"])
    c_val = _find_col(df, ["valor", "valor_total", "valor_liquido", "valorLiquido"])
    if not (c_data and c_val):
        return 0.0
    df[c_data] = _parse_date_col(df, c_data)
    dia = df[df[c_data].dt.date == data_sel].copy()
    if dia.empty:
        return 0.0
    return float(pd.to_numeric(dia[c_val], errors="coerce").fillna(0.0).sum())


def _correcoes_caixa_do_dia(caminho_banco: str, data_sel: date) -> float:
    """Soma, no dia, os valores da tabela correcao_caixa."""
    df = _carregar_tabela(caminho_banco, "correcao_caixa")
    if df.empty:
        return 0.0
    c_data = _find_col(df, ["data", "dt", "data_correcao"])
    c_valor = _find_col(df, ["valor", "valor_correcao", "valor_liquido", "valorLiquido"])
    if not (c_data and c_valor):
        return 0.0
    df[c_data] = _parse_date_col(df, c_data)
    dia = df[df[c_data].dt.date == data_sel].copy()
    if dia.empty:
        return 0.0
    return float(pd.to_numeric(dia[c_valor], errors="coerce").sum())


def _correcoes_acumuladas_ate(caminho_banco: str, data_sel: date) -> float:
    """Soma acumulada (<= data_sel) dos valores de correcao_caixa."""
    df = _carregar_tabela(caminho_banco, "correcao_caixa")
    if df.empty:
        return 0.0
    c_data = _find_col(df, ["data", "dt", "data_correcao"])
    c_valor = _find_col(df, ["valor", "valor_correcao", "valor_liquido", "valorLiquido"])
    if not (c_data and c_valor):
        return 0.0
    df[c_data] = _parse_date_col(df, c_data)
    ate = df[df[c_data].dt.date <= data_sel].copy()
    if ate.empty:
        return 0.0
    return float(pd.to_numeric(ate[c_valor], errors="coerce").sum())


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


# ========= Formatter seguro para a tabela de “Fechamentos Anteriores” =========
def _style_moeda_seguro(df: pd.DataFrame, cols_moeda: list[str]) -> pd.io.formats.style.Styler | pd.DataFrame:
    """
    Aplica _fmt (moeda pt-BR tolerante a None) nas colunas monetárias.
    Evita erros de format string quando há None/NaN.
    """
    if df is None or df.empty:
        return df
    cols = [c for c in cols_moeda if c in df.columns]
    try:
        mapping = {c: _fmt for c in cols}
        return df.style.format(mapping, na_rep=_fmt(0))
    except Exception:
        dfx = df.copy()
        for c in cols:
            dfx[c] = dfx[c].apply(lambda v: 0 if v in (None, "", "None") else v)
        mapping = {c: _fmt for c in cols}
        return dfx.style.format(mapping, na_rep=_fmt(0))


# ========================= Página (layout) =========================
def pagina_fechamento_caixa(caminho_banco: str) -> None:
    """Renderiza a página de Fechamento de Caixa (Streamlit)."""

    data_sel = st.date_input("📅 Data do Fechamento", value=date.today())
    st.markdown(f"**🗓️ Fechamento do dia — {data_sel}**")
    data_ref = str(data_sel)

    # Flag: já fechado?
    ja_fechado = _fechamento_existe(caminho_banco, data_ref)
    if ja_fechado:
        st.toast("✅ Este dia já foi fechado. Botão desativado.", icon="🔒")
    else:
        st.toast("ℹ️ Dia aberto para fechamento.", icon="📝")

    # --- Cartões do topo ---
    valor_dinheiro, valor_pix = _dinheiro_e_pix_por_data(caminho_banco, data_sel)
    total_cartao_liquido = _cartao_d1_liquido_por_data_liq(caminho_banco, data_sel)
    entradas_total_dia = float(valor_dinheiro + valor_pix + total_cartao_liquido)

    # Caixa do DIA (mantém lógica original para cálculos/salvamento)
    caixa_total_dia, caixa2_total_dia = _caixas_totais_no_dia(caminho_banco, data_sel)

    # Total consolidado (até a data) – continua usando os valores do DIA (sem alteração de lógica)
    # saldo_total = float(disp_caixa + disp_caixa2 + total_bancos)


# ============== NOVO: Lógica de Saldo Projetado (Checkpoint + Movimentações) ==============
def _calcular_saldo_projetado(caminho_banco: str, data_alvo: date) -> tuple[float, float, date | None]:
    """
    Calcula o saldo projetado de Caixa e Caixa 2 até a data_alvo.
    Lógica:
      1. Busca último fechamento (saldos_caixas) <= data_alvo (Checkpoint).
      2. Soma Entradas (DINHEIRO) no intervalo (checkpoint, data_alvo].
      3. Subtrai Saídas (Origem=Caixa/Caixa 2) no intervalo.
      4. Aplica Movimentações (Transferências/Correções) no intervalo.
    """
    data_alvo_str = str(data_alvo)
    
    with sqlite3.connect(caminho_banco) as conn:
        # 1. Checkpoint (último saldo salvo)
        row = conn.execute(
            """
            SELECT DATE(data) as d, caixa_total, caixa2_total
            FROM saldos_caixas
            WHERE DATE(data) <= DATE(?)
            ORDER BY DATE(data) DESC, ROWID DESC
            LIMIT 1
            """,
            (data_alvo_str,)
        ).fetchone()
        
        if row:
            data_base = row[0] # String YYYY-MM-DD
            base_caixa = float(row[1] or 0.0)
            base_caixa2 = float(row[2] or 0.0)
        else:
            data_base = '2000-01-01' # Data muito antiga se não houver fechamento anterior
            base_caixa = 0.0
            base_caixa2 = 0.0
            
        # Se o checkpoint for exatamente hoje, já temos o saldo (mas vamos recalcular para garantir consistência se houver lançamentos posteriores ao fechamento no mesmo dia, embora o bloqueio deva impedir)
        # Na verdade, a regra é: soma tudo ESTRITAMENTE MAIOR que data_base até data_alvo.
        
        # 2. Entradas (Vendas em DINHEIRO) -> Vai para CAIXA (Loja)
        # Intervalo: data_base < Data <= data_alvo
        vendas_dinheiro = conn.execute(
            """
            SELECT SUM(Valor) 
            FROM entrada 
            WHERE TRIM(UPPER(Forma_de_Pagamento)) = 'DINHEIRO'
              AND DATE(Data) > DATE(?) 
              AND DATE(Data) <= DATE(?)
            """,
            (data_base, data_alvo_str)
        ).fetchone()[0] or 0.0
        
        # 3. Saídas (Caixa e Caixa 2)
        # Intervalo: data_base < Data <= data_alvo
        # Origem_Dinheiro pode ser 'Caixa', 'Caixa 2', 'Caixa 2 (Casa)', etc. Normalizar se necessário.
        # Assumindo 'Caixa' e 'Caixa 2' como chaves principais.
        saidas_caixa = conn.execute(
            """
            SELECT SUM(Valor) 
            FROM saida 
            WHERE Origem_Dinheiro = 'Caixa'
              AND DATE(Data) > DATE(?) 
              AND DATE(Data) <= DATE(?)
            """,
            (data_base, data_alvo_str)
        ).fetchone()[0] or 0.0
        
        saidas_caixa2 = conn.execute(
            """
            SELECT SUM(Valor) 
            FROM saida 
            WHERE Origem_Dinheiro IN ('Caixa 2', 'Caixa 2 (Casa)')
              AND DATE(Data) > DATE(?) 
              AND DATE(Data) <= DATE(?)
            """,
            (data_base, data_alvo_str)
        ).fetchone()[0] or 0.0

        # 4. Movimentações Bancárias (Transferências, Depósitos, Correções)
        # Tabela: movimentacoes_bancarias
        # Colunas relevantes: data, valor, tipo (entrada/saida), origem, destino, banco
        # Precisamos filtrar onde o 'banco' (ou origem/destino) afeta Caixa ou Caixa 2
        
        # Helper para somar movimentações de um "banco" específico (no caso, 'Caixa' ou 'Caixa 2')
        def _calc_movs(nome_conta):
            # Soma entradas (tipo='entrada' e banco=nome_conta)
            entradas = conn.execute(
                """
                SELECT SUM(valor) 
                FROM movimentacoes_bancarias 
                WHERE banco = ? 
                  AND tipo = 'entrada'
                  AND DATE(data) > DATE(?) 
                  AND DATE(data) <= DATE(?)
                """,
                (nome_conta, data_base, data_alvo_str)
            ).fetchone()[0] or 0.0
            
            # Soma saídas (tipo='saida' e banco=nome_conta)
            saidas = conn.execute(
                """
                SELECT SUM(valor) 
                FROM movimentacoes_bancarias 
                WHERE banco = ? 
                  AND tipo = 'saida'
                  AND DATE(data) > DATE(?) 
                  AND DATE(data) <= DATE(?)
                """,
                (nome_conta, data_base, data_alvo_str)
            ).fetchone()[0] or 0.0
            
            return entradas - saidas

        movs_caixa = _calc_movs('Caixa')
        movs_caixa2 = _calc_movs('Caixa 2')

        # Cálculo Final
        saldo_final_caixa = base_caixa + float(vendas_dinheiro) - float(saidas_caixa) + float(movs_caixa)
        saldo_final_caixa2 = base_caixa2 - float(saidas_caixa2) + float(movs_caixa2) # Vendas dinheiro geralmente não vão pra caixa 2 direto, a menos que especificado.
        
        # Data de referência para retorno (data do checkpoint)
        dref = datetime.strptime(data_base, "%Y-%m-%d").date() if data_base != '2000-01-01' else None
        
        return round(saldo_final_caixa, 2), round(saldo_final_caixa2, 2), dref


# ========================= Página (layout) =========================
def pagina_fechamento_caixa(caminho_banco: str) -> None:
    """Renderiza a página de Fechamento de Caixa (Streamlit)."""

    data_sel = st.date_input("📅 Data do Fechamento", value=date.today())
    st.markdown(f"**🗓️ Fechamento do dia — {data_sel}**")
    data_ref = str(data_sel)

    # Flag: já fechado?
    ja_fechado = _fechamento_existe(caminho_banco, data_ref)
    if ja_fechado:
        st.toast("✅ Este dia já foi fechado. Botão desativado.", icon="🔒")
    else:
        st.toast("ℹ️ Dia aberto para fechamento.", icon="📝")

    # --- Cartões do topo ---
    valor_dinheiro, valor_pix = _dinheiro_e_pix_por_data(caminho_banco, data_sel)
    total_cartao_liquido = _cartao_d1_liquido_por_data_liq(caminho_banco, data_sel)
    entradas_total_dia = float(valor_dinheiro + valor_pix + total_cartao_liquido)

    # Caixa do DIA (mantém lógica original para cálculos/salvamento)
    caixa_total_dia, caixa2_total_dia = _caixas_totais_no_dia(caminho_banco, data_sel)

    # >>> NOVO: Saldo Projetado (Acumulado Real) <<<
    # Substitui _ultimo_caixas_ate pela nova lógica
    disp_caixa, disp_caixa2, disp_ref = _calcular_saldo_projetado(caminho_banco, data_sel)

    # Bancos (acumulado <= data)
    bancos_totais = _somar_bancos_totais(caminho_banco, data_sel)
    total_bancos = float(sum(bancos_totais.values())) if bancos_totais else 0.0

    # Saídas e correções
    saidas_total_dia = _saidas_total_do_dia(caminho_banco, data_sel)
    corr_dia = _correcoes_caixa_do_dia(caminho_banco, data_sel)
    corr_acum = _correcoes_acumuladas_ate(caminho_banco, data_sel)

    # Total consolidado (agora usa o saldo projetado correto)
    saldo_total = float(disp_caixa + disp_caixa2 + total_bancos)


    # CSS para mini-tabela compacta
    st.markdown(
        """
        <style>
        .section-card .mini-table thead th{ font-size:.72rem; padding:3px 4px; }
        .section-card .mini-table tbody td{ font-size:.76rem; padding:3px 4px; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ===================== Layout =====================
    render_card_row(
        "💰 Valores que Entraram Hoje",
        [
            ("Dinheiro", valor_dinheiro, True),
            ("Pix", valor_pix, True),
            ("Cartão D-1 (Líquido)", total_cartao_liquido, True),
        ],
    )

    df_corr = pd.DataFrame(
        [
            {"Descrição": "Correção do Dia", "Valor": corr_dia},
            {"Descrição": "Correção Acumulada", "Valor": corr_acum},
        ]
    )
    render_card_row(
        "📊 Resumo das Movimentações de Hoje",
        [
            ("Entradas", entradas_total_dia, True),
            ("Saídas", saidas_total_dia, True),
            ("Correções de Caixa", df_corr, False),
        ],
    )

    # >>> EXIBIÇÃO com último saldo salvo (sem mudar lógica de cálculo) <<<
    render_card_row(
        "🧾 Saldo em Caixa",
        [
            ("Caixa (loja)", disp_caixa, True),
            ("Caixa 2 (casa)", disp_caixa2, True),
        ],
    )
    if disp_ref and disp_ref != data_sel:
        st.caption(f"Mostrando último saldo salvo em **{disp_ref}** (sem movimento em {data_sel}).")

    if bancos_totais:
        render_card_row(
            "🏦 Saldos em Bancos",
            [(label, valor, True) for label, valor in bancos_totais.items()],
        )
    else:
        st.caption("Sem bancos cadastrados na tabela saldos_bancos.")

    render_card_row("💰 Saldo Total", [("Total consolidado", saldo_total, True)])

    # ======= Salvar fechamento =======
    confirmar = st.checkbox("Confirmo que o saldo está correto.", disabled=ja_fechado)
    salvar = st.button("Salvar fechamento", disabled=ja_fechado)

    if salvar:
        if ja_fechado:
            st.toast("⚠️ Já existe um fechamento salvo para esta data.", icon="⚠️")
            return
        if not confirmar:
            st.toast("⚠️ Você precisa confirmar que o saldo está correto.", icon="✋")
            return

        b1, b2, b3, b4 = _get_saldos_bancos_ate(caminho_banco, data_ref)
        try:
            with sqlite3.connect(caminho_banco) as conn:
                existe = conn.execute(
                    "SELECT 1 FROM fechamento_caixa WHERE DATE(data)=DATE(?) LIMIT 1",
                    (str(data_sel),),
                ).fetchone()
                if existe:
                    st.toast("⚠️ Já existe um fechamento salvo para esta data.", icon="⚠️")
                    return

                conn.execute(
                    """
                    INSERT INTO fechamento_caixa (
                        data, banco_1, banco_2, banco_3, banco_4,
                        caixa, caixa_2, entradas_confirmadas, saidas,
                        correcao, saldo_esperado, valor_informado, diferenca
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(data_sel),
                        float(b1), float(b2), float(b3), float(b4),  # acumulados bancos
                        float(disp_caixa),           # SALDO de Caixa até a data
                        float(disp_caixa2),          # SALDO de Caixa 2 até a data
                        float(entradas_total_dia),                   # Dinheiro+Pix(Data) + Cartão D-1(Data_Liq)
                        float(saidas_total_dia),
                        float(corr_dia),
                        float(saldo_total),  # esperado
                        float(saldo_total),  # informado
                        0.0,
                    ),
                )
                conn.commit()
                st.toast("✅ Fechamento salvo com sucesso!", icon="🎉")
                st.balloons()
        except Exception as e:
            st.error(f"❌ Erro ao salvar fechamento: {e}")
            return

    # Fechamentos Anteriores — tabela
    st.markdown("### 📋 Fechamentos Anteriores")
    try:
        with sqlite3.connect(caminho_banco) as conn:
            df_fech = _read_sql(
                conn,
                """
                SELECT 
                    data as 'Data',
                    banco_1 as 'Inter',
                    banco_2 as 'Bradesco',
                    banco_3 as 'InfinitePay',
                    banco_4 as 'Outros Bancos',
                    caixa as 'Caixa',
                    caixa_2 as 'Caixa 2',
                    entradas_confirmadas as 'Entradas',
                    saidas as 'Saídas',
                    correcao as 'Correções',
                    saldo_esperado as 'Saldo Esperado',
                    valor_informado as 'Valor Informado',
                    diferenca as 'Diferença'
                FROM fechamento_caixa
                ORDER BY data DESC
                """,
            )
    except Exception:
        df_fech = pd.DataFrame()

    if not df_fech.empty:
        cols_moeda = [
            "Inter", "Bradesco", "InfinitePay", "Outros Bancos",
            "Caixa", "Caixa 2", "Entradas", "Saídas", "Correções",
            "Saldo Esperado", "Valor Informado", "Diferença",
        ]
        st.dataframe(
            _style_moeda_seguro(df_fech, cols_moeda),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("Nenhum fechamento realizado ainda.")
