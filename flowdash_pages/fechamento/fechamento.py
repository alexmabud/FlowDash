# flowdash_pages/fechamento/fechamento.py
from __future__ import annotations

import re
import sqlite3
from datetime import date, timedelta

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


# ================== Consultas auxiliares (legado para salvar) ==================
def _get_saldos_bancos_ate(caminho_banco: str, data_ref: str) -> tuple[float, float, float, float]:
    """Retorna o último registro (<= data) de saldos_bancos mapeado em 4 colunas (legado)."""
    try:
        with sqlite3.connect(caminho_banco) as conn:
            df = _read_sql(
                conn,
                """
                SELECT *
                FROM saldos_bancos
                WHERE data <= ?
                ORDER BY data DESC
                LIMIT 1
                """,
                (data_ref,),
            )
    except Exception:
        return (0.0, 0.0, 0.0, 0.0)

    if df.empty:
        return (0.0, 0.0, 0.0, 0.0)

    cols = df.columns.tolist()

    def _by_hint(hints: list[str]) -> str | None:
        for h in hints:
            for c in cols:
                if _norm(c) == _norm(h) or _norm(h) in _norm(c):
                    return c
        return None

    b1 = _by_hint(["banco_1", "banco1", "inter"])
    b2 = _by_hint(["banco_2", "banco2", "bradesco"])
    b3 = _by_hint(["banco_3", "banco3", "infinitepay"])
    b4 = _by_hint(["banco_4", "banco4", "outros", "outros_bancos"])
    row = df.iloc[0]

    def _get(c: str | None) -> float:
        try:
            return float(pd.to_numeric(row[c], errors="coerce") or 0.0) if c in df.columns else 0.0
        except Exception:
            return 0.0

    return (_get(b1), _get(b2), _get(b3), _get(b4))


# ================== Cálculos dos cartões do topo ==================
def _dinheiro_e_pix_por_data(caminho_banco: str, data_sel: date) -> tuple[float, float]:
    """
    Soma, no dia selecionado, o valor (prefere COALESCE(valor_liquido, Valor))
    das entradas em DINHEIRO e PIX **filtrando por `entrada.Data`**.
    """
    df = _carregar_tabela(caminho_banco, "entrada")
    if df.empty:
        return 0.0, 0.0

    # Data da VENDA (não Data_Liq, não created_at)
    c_data = _find_col(df, [
        "Data", "data", "data_venda", "dataVenda", "data_lanc", "dataLanc", "data_emissao", "dataEmissao"
    ])
    c_forma = _find_col(df, ["Forma_de_Pagamento", "forma_de_pagamento", "forma_pagamento", "forma"])
    # Preferir líquido quando existir (não afeta DINHEIRO/PIX direto; cobre PIX via maquineta)
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

    # Aqui precisamos EXPLICITAMENTE da coluna de liquidação
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


# ============== Somatórios até a data selecionada ==============
def _somar_caixas_totais(caminho_banco: str, data_sel: date) -> tuple[float, float]:
    """Soma (<= data_sel) de caixa_total e caixa2_total da tabela saldos_caixas."""
    df = _carregar_tabela(caminho_banco, "saldos_caixas")
    if df.empty:
        return 0.0, 0.0

    c_data = _find_col(df, ["data", "dt"])
    c_caixa = _find_col(df, ["caixa_total", "total_caixa", "caixa", "caixa_total_dia"])
    c_cx2 = _find_col(df, ["caixa2_total", "caixa_2_total", "total_caixa2", "caixa2", "caixa_2", "caixa2_total_dia"])

    if c_data:
        df[c_data] = _parse_date_col(df, c_data)
        df = df[df[c_data].dt.date <= data_sel].copy()

    soma_caixa = float(pd.to_numeric(df[c_caixa], errors="coerce").sum()) if c_caixa else 0.0
    soma_caixa2 = float(pd.to_numeric(df[c_cx2], errors="coerce").sum()) if c_cx2 else 0.0
    return soma_caixa, soma_caixa2


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


# ============== Saídas do dia ==============
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
    st.markdown("## 🧾 Fechamento de Caixa")

    data_sel = st.date_input("📅 Data do Fechamento", value=date.today())
    st.markdown(f"**🗓️ Fechamento do dia — {data_sel}**")
    data_ref = str(data_sel)

    # --- Cartões do topo ---
    # Dinheiro/PIX (por entrada.Data) — valores que entraram hoje (venda do dia)
    valor_dinheiro, valor_pix = _dinheiro_e_pix_por_data(caminho_banco, data_sel)
    # Cartão D-1 (por entrada.Data_Liq) — liquidações que caíram hoje
    total_cartao_liquido = _cartao_d1_liquido_por_data_liq(caminho_banco, data_sel)
    # Entradas do dia (consolidadas)
    entradas_total_dia = float(valor_dinheiro + valor_pix + total_cartao_liquido)

    # Caixa e Bancos (somatórios até a data)
    soma_caixa_total, soma_caixa2_total = _somar_caixas_totais(caminho_banco, data_sel)
    bancos_totais = _somar_bancos_totais(caminho_banco, data_sel)
    total_bancos = float(sum(bancos_totais.values())) if bancos_totais else 0.0

    # Saídas e correções
    saidas_total_dia = _saidas_total_do_dia(caminho_banco, data_sel)
    corr_dia = _correcoes_caixa_do_dia(caminho_banco, data_sel)
    corr_acum = _correcoes_acumuladas_ate(caminho_banco, data_sel)

    # Total consolidado (até a data)
    saldo_total = float(soma_caixa_total + soma_caixa2_total + total_bancos + corr_acum)

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

    render_card_row(
        "🧾 Saldo em Caixa",
        [
            ("Caixa (loja)", soma_caixa_total, True),
            ("Caixa 2 (casa)", soma_caixa2_total, True),
        ],
    )

    if bancos_totais:
        render_card_row(
            "🏦 Saldos em Bancos",
            [(label, valor, True) for label, valor in bancos_totais.items()],
        )
    else:
        st.caption("Sem bancos cadastrados na tabela saldos_bancos.")

    render_card_row("💰 Saldo Total", [("Total consolidado", saldo_total, True)])

    # ======= Salvar fechamento (schema legado) =======
    confirmar = st.checkbox("Confirmo que o saldo está correto.")
    salvar = st.button("Salvar fechamento")

    if salvar:
        if not confirmar:
            st.warning("⚠️ Você precisa confirmar que o saldo está correto antes de salvar.")
            return

        b1, b2, b3, b4 = _get_saldos_bancos_ate(caminho_banco, data_ref)
        try:
            with sqlite3.connect(caminho_banco) as conn:
                existe = conn.execute(
                    "SELECT 1 FROM fechamento_caixa WHERE data = ? LIMIT 1",
                    (str(data_sel),),
                ).fetchone()
                if existe:
                    st.warning("⚠️ Já existe um fechamento salvo para esta data.")
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
                        float(b1), float(b2), float(b3), float(b4),  # legado
                        float(soma_caixa_total),
                        float(soma_caixa2_total),
                        float(entradas_total_dia),   # Dinheiro+Pix(Data) + Cartão D-1(Data_Liq)
                        float(saidas_total_dia),
                        float(corr_dia),
                        float(saldo_total),  # esperado
                        float(saldo_total),  # informado
                        0.0,
                    ),
                )
                conn.commit()
                st.success("✅ Fechamento salvo com sucesso!")
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
