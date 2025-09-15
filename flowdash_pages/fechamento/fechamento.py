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
            return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except Exception:
            return str(v)

# --------- dependência opcional (Cartão D-1) ---------
try:
    from workalendar.america import BrazilDistritoFederal
    _HAS_WORKACALENDAR = True
except Exception:
    _HAS_WORKACALENDAR = False


# ================== Helpers de acesso ==================
def _read_sql(conn, query, params=None):
    return pd.read_sql(query, conn, params=params or ())

def _carregar_tabela(caminho_banco: str, nome: str) -> pd.DataFrame:
    with sqlite3.connect(caminho_banco) as conn:
        try:
            return _read_sql(conn, f"SELECT * FROM {nome}")
        except Exception:
            return pd.DataFrame()

def _get_saldo_caixas(caminho_banco: str, data_ref: str):
    """Retorna (caixa, caixa_2) cadastrados em saldos_caixas para a data."""
    try:
        with sqlite3.connect(caminho_banco) as conn:
            row = conn.execute(
                "SELECT caixa, caixa_2 FROM saldos_caixas WHERE data = ? LIMIT 1",
                (data_ref,)
            ).fetchone()
        if row:
            return float(row[0] or 0.0), float(row[1] or 0.0)
    except Exception:
        pass
    return 0.0, 0.0

def _mapear_colunas_bancos(cols: list[str]) -> dict[int, str]:
    """Heurística para encontrar colunas de bancos (1..4)."""
    idx_por_num = {}
    for i in (1, 2, 3, 4):
        pads = [rf"^banco[_ ]?{i}$", rf"^saldo[_ ]?banco[_ ]?{i}$", rf".*banco.*{i}.*"]
        for c in cols:
            lc = c.lower()
            if any(re.match(p, lc) for p in pads):
                idx_por_num.setdefault(i, c)
                break
    restantes = [c for c in cols if "banco" in c.lower()]
    for c in ("id", "data", "created_at", "updated_at"):
        restantes = [x for x in restantes if x.lower() != c]
    usados = set(idx_por_num.values())
    restantes = [c for c in restantes if c not in usados]
    for i in (1, 2, 3, 4):
        if i not in idx_por_num and restantes:
            idx_por_num[i] = restantes.pop(0)
    return idx_por_num

def _get_saldos_bancos_ate(caminho_banco: str, data_ref: str):
    """Tuple (b1,b2,b3,b4) com últimos saldos cadastrados (<= data) em saldos_bancos; tolera schema diferente."""
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
                (data_ref,)
            )
    except Exception:
        return (0.0, 0.0, 0.0, 0.0)
    if df.empty:
        return (0.0, 0.0, 0.0, 0.0)
    cols = df.columns.tolist()
    mapa = _mapear_colunas_bancos(cols)
    linha = df.iloc[0]
    def _get(i):
        col = mapa.get(i)
        if not col:
            return 0.0
        try:
            return float(pd.to_numeric(linha[col], errors="coerce") or 0.0)
        except Exception:
            return 0.0
    return (_get(1), _get(2), _get(3), _get(4))

def _get_movimentos_caixa(caminho_banco: str, data_ref: str):
    """Movimentações do dia (Caixa / Caixa 2) em movimentacoes_bancarias."""
    like_pat = f"{data_ref}%"
    try:
        with sqlite3.connect(caminho_banco) as conn:
            df = _read_sql(
                conn,
                """
                SELECT id, data, banco, tipo, origem, valor, observacao,
                       referencia_tabela, referencia_id
                FROM movimentacoes_bancarias
                WHERE data LIKE ?
                  AND banco IN ('Caixa','Caixa 2')
                ORDER BY id
                """,
                (like_pat,)
            )
    except Exception:
        return pd.DataFrame()
    if not df.empty:
        df["tipo"] = df["tipo"].astype(str).str.lower().str.strip()
        df["origem"] = df["origem"].astype(str).str.strip()
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0.0)
    return df


# ============== Cálculos específicos do layout ==============
def _dinheiro_e_pix_do_dia(caminho_banco: str, data_sel: date):
    """Mantém filtro por forma (usa tabela 'entrada')."""
    df_e = _carregar_tabela(caminho_banco, "entrada")
    if df_e.empty:
        return 0.0, 0.0
    df_e.columns = [c.strip() for c in df_e.columns]
    col_data = next((c for c in df_e.columns if c.lower() in ("data", "dt", "data_venda")), None)
    col_forma = next((c for c in df_e.columns if "forma" in c.lower()), None)
    col_valor = next((c for c in df_e.columns if "valor" in c.lower()), None)
    if not (col_data and col_forma and col_valor):
        return 0.0, 0.0
    df_e[col_data] = pd.to_datetime(df_e[col_data], errors="coerce")
    df_dia = df_e[df_e[col_data].dt.date == data_sel].copy()
    if df_dia.empty:
        return 0.0, 0.0
    df_dia[col_forma] = df_dia[col_forma].astype(str).str.upper().str.strip()
    valor_dinheiro = pd.to_numeric(df_dia.loc[df_dia[col_forma] == "DINHEIRO", col_valor], errors="coerce").sum()
    valor_pix = pd.to_numeric(df_dia.loc[df_dia[col_forma] == "PIX", col_valor], errors="coerce").sum()
    return float(valor_dinheiro or 0.0), float(valor_pix or 0.0)

def _saidas_dinheiro_por_origem(caminho_banco: str, data_sel: date):
    """Saídas em dinheiro (CAIXA / CAIXA 2) no dia (tabela 'saida')."""
    df_s = _carregar_tabela(caminho_banco, "saida")
    if df_s.empty:
        return 0.0, 0.0
    df_s.columns = [c.strip() for c in df_s.columns]
    col_data = next((c for c in df_s.columns if c.lower() in ("data", "dt", "data_saida")), None)
    col_forma = next((c for c in df_s.columns if "forma" in c.lower()), None)
    col_origem = next((c for c in df_s.columns if "origem" in c.lower()), None)
    col_valor = next((c for c in df_s.columns if "valor" in c.lower()), None)
    if not (col_data and col_forma and col_origem and col_valor):
        return 0.0, 0.0
    df_s[col_data] = pd.to_datetime(df_s[col_data], errors="coerce")
    df_dia = df_s[df_s[col_data].dt.date == data_sel].copy()
    if df_dia.empty:
        return 0.0, 0.0
    df_dia[col_forma] = df_dia[col_forma].astype(str).str.upper().str.strip()
    df_dia[col_origem] = df_dia[col_origem].astype(str).str.upper().str.strip()
    saidas_caixa = pd.to_numeric(
        df_dia.loc[(df_dia[col_forma] == "DINHEIRO") & (df_dia[col_origem] == "CAIXA"), col_valor],
        errors="coerce"
    ).sum()
    saidas_caixa2 = pd.to_numeric(
        df_dia.loc[(df_dia[col_forma] == "DINHEIRO") & (df_dia[col_origem].isin(["CAIXA 2", "CAIXA2"])), col_valor],
        errors="coerce"
    ).sum()
    return float(saidas_caixa or 0.0), float(saidas_caixa2 or 0.0)

def _saidas_pix_debito_por_banco(caminho_banco: str, data_sel: date):
    """Abate saídas PIX/DÉBITO por banco (1..4) no dia."""
    df_s = _carregar_tabela(caminho_banco, "saida")
    if df_s.empty:
        return (0.0, 0.0, 0.0, 0.0)
    df_s.columns = [c.strip() for c in df_s.columns]
    col_data = next((c for c in df_s.columns if c.lower() in ("data", "dt", "data_saida")), None)
    col_forma = next((c for c in df_s.columns if "forma" in c.lower()), None)
    col_banco = next((c for c in df_s.columns if "banco" in c.lower()), None)
    col_valor = next((c for c in df_s.columns if "valor" in c.lower()), None)
    if not (col_data and col_forma and col_banco and col_valor):
        return (0.0, 0.0, 0.0, 0.0)
    df_s[col_data] = pd.to_datetime(df_s[col_data], errors="coerce")
    df_dia = df_s[df_s[col_data].dt.date == data_sel].copy()
    if df_dia.empty:
        return (0.0, 0.0, 0.0, 0.0)
    df_dia[col_forma] = df_dia[col_forma].astype(str).str.upper().str.strip()
    df_dia[col_banco] = df_dia[col_banco].astype(str).str.upper().str.strip()
    def _sum(df, banco_alias):
        return float(pd.to_numeric(
            df.loc[(df[col_forma].isin(["PIX", "DÉBITO", "DEBITO"])) & (df[col_banco] == banco_alias), col_valor],
            errors="coerce"
        ).sum() or 0.0)
    return (_sum(df_dia, "BANCO 1"), _sum(df_dia, "BANCO 2"), _sum(df_dia, "BANCO 3"), _sum(df_dia, "BANCO 4"))

def _calcular_cartao_liquido_d1(caminho_banco: str, data_base: date) -> float:
    """Cartão (D-1 líquido) — depende de workalendar + taxas_maquinas. Fallback 0,00."""
    if not _HAS_WORKACALENDAR:
        return 0.0
    df_e = _carregar_tabela(caminho_banco, "entrada")
    if df_e.empty:
        return 0.0
    df_e.columns = [c.strip() for c in df_e.columns]
    col_data = next((c for c in df_e.columns if c.lower() in ("data", "dt", "data_venda")), None)
    col_forma = next((c for c in df_e.columns if "forma" in c.lower()), None)
    col_valor = next((c for c in df_e.columns if "valor" in c.lower()), None)
    col_band = next((c for c in df_e.columns if "bandeira" in c.lower()), None)
    col_parc = next((c for c in df_e.columns if "parcela" in c.lower()), None)
    if not (col_data and col_forma and col_valor):
        return 0.0

    cal = BrazilDistritoFederal()
    dias_considerados = []
    dia_anterior = data_base - timedelta(days=1)
    while not cal.is_working_day(dia_anterior):
        dias_considerados.append(dia_anterior)
        dia_anterior -= timedelta(days=1)
    dias_considerados.append(dia_anterior)

    df_e[col_data] = pd.to_datetime(df_e[col_data], errors="coerce")
    df_cartao = df_e[
        (df_e[col_forma].astype(str).str.upper().isin(["DÉBITO", "DEBITO", "CRÉDITO", "CREDITO"])) &
        (df_e[col_data].dt.date.isin([d for d in dias_considerados]))
    ].copy()
    if df_cartao.empty:
        return 0.0

    def _norm_bandeira(x: str) -> str:
        raw = (x or "").upper().replace(" ", "").replace("-", "")
        mapa = {"MASTERCARD": "MASTER", "MASTER": "MASTER", "DINERSCLUB": "DINERSCLUB",
                "DINERS": "DINERSCLUB", "AMEX": "AMEX", "ELO": "ELO", "VISA": "VISA"}
        return mapa.get(raw, raw)

    total_liquido = 0.0
    with sqlite3.connect(caminho_banco) as conn:
        for _, row in df_cartao.iterrows():
            valor = float(pd.to_numeric(row[col_valor], errors="coerce") or 0.0)
            forma = str(row[col_forma]).upper()
            bandeira = _norm_bandeira(str(row.get(col_band, "")))
            parcelas = int(pd.to_numeric(row.get(col_parc, 1), errors="coerce") or 1)
            taxa = 0.0
            try:
                trow = conn.execute(
                    """
                    SELECT taxa_percentual
                    FROM taxas_maquinas
                    WHERE UPPER(forma_pagamento) = ?
                      AND UPPER(bandeira) = ?
                      AND parcelas = ?
                    """,
                    (forma, bandeira, parcelas)
                ).fetchone()
                taxa = float(trow[0]) if trow else 0.0
            except Exception:
                taxa = 0.0
            total_liquido += valor * (1 - taxa / 100.0)
    return round(total_liquido, 2)


# ========================= UI helpers =========================
def _inject_css():
    # Números verdes e cards discretos, igual Lançamentos
    st.markdown(
        """
        <style>
        .fd-section { margin: 1.1rem 0 .6rem; font-weight: 700; font-size: 1.05rem; opacity: .92; }
        .fd-card {
          border-radius: 14px;
          padding: .85rem .95rem;
          background: rgba(255,255,255,.04);
          border: 1px solid rgba(255,255,255,.08);
        }
        .fd-card h4 { margin: 0 0 .35rem; font-weight: 600; font-size: .95rem; opacity:.85; }
        .fd-value { font-size: 1.35rem; font-weight: 800; letter-spacing: .2px; color: #22c55e; }
        .fd-note { font-size: .85rem; opacity:.7; margin-top:.2rem; }
        .fd-total { border-left: 4px solid rgba(34,197,94,.6); }
        </style>
        """,
        unsafe_allow_html=True,
    )

def _section(title: str, emoji: str = ""):
    st.markdown(f"<div class='fd-section'>{emoji} {title}</div>", unsafe_allow_html=True)

def _metric_card(col, title: str, value: str, note: str | None = None):
    with col:
        st.markdown(
            "<div class='fd-card'><h4>"
            + title
            + "</h4><div class='fd-value'>"
            + value
            + "</div>"
            + (f"<div class='fd-note'>{note}</div>" if note else "")
            + "</div>",
            unsafe_allow_html=True,
        )


# ========================= Página (layout em colunas) =========================
def pagina_fechamento_caixa(caminho_banco: str):
    _inject_css()

    st.markdown("## 🧾 Fechamento de Caixa")

    data_sel = st.date_input("📅 Data do Fechamento", value=date.today())
    st.markdown(f"**🗓️ Fechamento do dia — {data_sel}**")
    data_ref = str(data_sel)

    # --- Cálculos (mesma lógica) ---
    valor_dinheiro, valor_pix = _dinheiro_e_pix_do_dia(caminho_banco, data_sel)
    valor_caixa_cad, valor_caixa2_cad = _get_saldo_caixas(caminho_banco, data_ref)
    saidas_caixa, saidas_caixa2 = _saidas_dinheiro_por_origem(caminho_banco, data_sel)

    valor_caixa = valor_caixa_cad + valor_dinheiro - saidas_caixa
    valor_caixa2 = valor_caixa2_cad - saidas_caixa2

    saldo_b1, saldo_b2, saldo_b3, saldo_b4 = _get_saldos_bancos_ate(caminho_banco, data_ref)
    s_b1, s_b2, s_b3, s_b4 = _saidas_pix_debito_por_banco(caminho_banco, data_sel)
    saldo_b1 -= s_b1
    saldo_b2 -= s_b2
    saldo_b3 -= s_b3
    saldo_b4 -= s_b4

    total_cartao_liquido = _calcular_cartao_liquido_d1(caminho_banco, data_sel)
    if not _HAS_WORKACALENDAR:
        st.info("ℹ️ Para Cartão D-1 (líquido), instale `workalendar` — mantendo 0,00 por enquanto.")

    valor_banco_1 = saldo_b1 + valor_pix + total_cartao_liquido

    df_mov = _get_movimentos_caixa(caminho_banco, data_ref)
    if not df_mov.empty:
        entradas_total = df_mov.loc[df_mov["tipo"] == "entrada", "valor"].sum()
        saidas_total = df_mov.loc[df_mov["tipo"] == "saida", "valor"].sum()
        df_corr = df_mov[df_mov["origem"] == "correcao_caixa"]
        corr_ent = df_corr.loc[df_corr["tipo"] == "entrada", "valor"].sum() if not df_corr.empty else 0.0
        corr_sai = df_corr.loc[df_corr["tipo"] == "saida", "valor"].sum() if not df_corr.empty else 0.0
        total_correcao = (corr_ent - corr_sai)
    else:
        entradas_total = saidas_total = total_correcao = 0.0

    saldo_total = valor_caixa + valor_caixa2 + valor_banco_1 + saldo_b2 + saldo_b3 + saldo_b4 + total_correcao

    # ===================== Layout por colunas =====================
    # Valores que Entraram Hoje — 3 colunas
    _section("Valores que Entraram Hoje", emoji="💰")
    c1, c2, c3 = st.columns(3)
    _metric_card(c1, "Dinheiro", _fmt(valor_dinheiro))
    _metric_card(c2, "Pix", _fmt(valor_pix))
    _metric_card(c3, "Cartão D-1 (Líquido)", _fmt(total_cartao_liquido))

    # Resumo das Movimentações — 3 colunas
    _section("Resumo das Movimentações de Hoje", emoji="📊")
    r1, r2, r3 = st.columns(3)
    _metric_card(r1, "Entradas (Caixa/Caixa2)", _fmt(entradas_total))
    _metric_card(r2, "Saídas (Caixa/Caixa2)", _fmt(saidas_total))
    _metric_card(r3, "Correções (líquido)", _fmt(total_correcao))

    # Saldo em Caixa — 2 colunas
    _section("Saldo em Caixa", emoji="🧾")
    s1, s2 = st.columns(2)
    _metric_card(s1, "Caixa (loja)", _fmt(valor_caixa))
    _metric_card(s2, "Caixa 2 (casa)", _fmt(valor_caixa2))

    # Saldo em Bancos — 4 colunas
    _section("Saldo em Bancos", emoji="🏦")
    b1, b2, b3, b4 = st.columns(4)
    _metric_card(b1, "Inter (Banco 1)", _fmt(valor_banco_1))
    _metric_card(b2, "Bradesco (Banco 2)", _fmt(saldo_b2))
    _metric_card(b3, "InfinitePay (Banco 3)", _fmt(saldo_b3))
    _metric_card(b4, "Outros Bancos (Banco 4)", _fmt(saldo_b4))

    # Saldo Total — card único + confirmação/salvar
    _section("Saldo Total", emoji="💰")
    st.markdown("<div class='fd-card fd-total'>", unsafe_allow_html=True)
    st.markdown(f"**Total consolidado:** {_fmt(saldo_total)}")
    confirmar = st.checkbox("Confirmo que o saldo está correto.")
    salvar = st.button("Salvar fechamento")
    st.markdown("</div>", unsafe_allow_html=True)

    if salvar:
        if not confirmar:
            st.warning("⚠️ Você precisa confirmar que o saldo está correto antes de salvar.")
        else:
            try:
                with sqlite3.connect(caminho_banco) as conn:
                    existe = conn.execute(
                        "SELECT 1 FROM fechamento_caixa WHERE data = ? LIMIT 1",
                        (str(data_sel),)
                    ).fetchone()
                    if existe:
                        st.warning("⚠️ Já existe um fechamento salvo para esta data.")
                    else:
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
                                float(valor_banco_1),
                                float(saldo_b2),
                                float(saldo_b3),
                                float(saldo_b4),
                                float(valor_caixa),
                                float(valor_caixa2),
                                float(valor_dinheiro + valor_pix + total_cartao_liquido),
                                float(saidas_total),
                                float(total_correcao),
                                float(saldo_total),
                                float(saldo_total),
                                0.0,
                            )
                        )
                        conn.commit()
                        st.success("✅ Fechamento salvo com sucesso!")
                        st.balloons()
            except Exception as e:
                st.error(f"❌ Erro ao salvar fechamento: {e}")

    # Fechamentos Anteriores — tabela
    _section("Fechamentos Anteriores", emoji="📋")
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
                """
            )
    except Exception:
        df_fech = pd.DataFrame()

    if not df_fech.empty:
        st.dataframe(
            df_fech.style.format({
                "Inter": "R$ {:,.2f}", "Bradesco": "R$ {:,.2f}", "InfinitePay": "R$ {:,.2f}", "Outros Bancos": "R$ {:,.2f}",
                "Caixa": "R$ {:,.2f}", "Caixa 2": "R$ {:,.2f}", "Entradas": "R$ {:,.2f}", "Saídas": "R$ {:,.2f}",
                "Correções": "R$ {:,.2f}", "Saldo Esperado": "R$ {:,.2f}", "Valor Informado": "R$ {:,.2f}", "Diferença": "R$ {:,.2f}"
            }),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("Nenhum fechamento realizado ainda.")
