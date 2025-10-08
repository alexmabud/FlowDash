# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
from typing import Optional, List, Tuple
from datetime import date

import pandas as pd
import streamlit as st
from flowdash_pages.dataframes.filtros import selecionar_mes

# ================= Descoberta de DB (segura) =================
try:
    # Usa a camada segura (não acessa session_state no import-time)
    from shared.db import get_db_path as _shared_get_db_path, ensure_db_path_or_raise
except Exception:
    _shared_get_db_path = None

    def ensure_db_path_or_raise(_: Optional[str] = None) -> str:
        for p in (
            os.path.join("data", "flowdash_data.db"),
            os.path.join("data", "dashboard_rc.db"),
            "dashboard_rc.db",
            os.path.join("data", "flowdash_template.db"),
        ):
            if os.path.exists(p):
                return p
        raise FileNotFoundError("Nenhum banco padrão encontrado.")

def _resolve_db_path(pref: Optional[str]) -> Optional[str]:
    if isinstance(pref, str) and os.path.exists(pref):
        return pref
    if callable(_shared_get_db_path):
        p = _shared_get_db_path()
        if isinstance(p, str) and os.path.exists(p):
            return p
    # fallbacks locais
    for p in (
        os.path.join("data", "flowdash_data.db"),
        os.path.join("data", "dashboard_rc.db"),
        "dashboard_rc.db",
        os.path.join("data", "flowdash_template.db"),
    ):
        if os.path.exists(p):
            return p
    return None

def _connect(db_like: Optional[str]) -> Optional[sqlite3.Connection]:
    try:
        db = ensure_db_path_or_raise(db_like)
    except Exception as e:
        st.error("❌ Banco de dados não encontrado (Fatura Cartão).")
        st.caption(str(e))
        return None
    try:
        return sqlite3.connect(db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    except Exception as e:
        st.error("❌ Erro ao conectar no banco (Fatura Cartão).")
        st.exception(e)
        return None

# ================= Helpers =================
PT_BR_MESES = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez"
}

def _fmt_moeda(v) -> str:
    try:
        return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v)

def _zebra(df: pd.DataFrame, dark: str = "#12161d", light: str = "#1b212b") -> pd.io.formats.style.Styler:
    ncols = df.shape[1]
    def _row_style(row: pd.Series):
        bg = light if (row.name % 2) else dark
        return [f"background-color: {bg}"] * ncols
    return df.style.apply(_row_style, axis=1)

def _auto_df_height(df: pd.DataFrame, row_px: int = 30, header_px: int = 36, pad_px: int = 6, max_px: int = 1000) -> int:
    n = int(len(df))
    return min(header_px + (n * row_px) + pad_px, max_px)

def _height_exact_rows(n_rows: int) -> int:
    header_px = 38
    row_px = 34
    pad_px = 4
    return header_px + (n_rows * row_px) + pad_px

def _normalize_competencia(s: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(s):
        return s.dt.to_period("M").astype(str)
    dt = pd.to_datetime(s, errors="coerce", dayfirst=False)
    if dt.notna().any():
        return dt.dt.to_period("M").astype(str)
    s1 = s.astype(str)
    mask = s1.str.fullmatch(r"\s*\d{1,2}/\d{4}\s*")
    if mask.any():
        s2 = s1.copy()
        s2.loc[mask] = "01/" + s2.loc[mask].str.strip()
        dt2 = pd.to_datetime(s2, errors="coerce", dayfirst=True)
        return dt2.dt.to_period("M").astype(str)
    s3 = s.astype(str).str.replace(r"\D", "", regex=True)
    m2 = s3.str.match(r"^\d{6}$")
    dt3 = pd.to_datetime(s3.where(~m2, s3.str[:4] + "-" + s3.str[4:] + "-01"), errors="coerce")
    return dt3.dt.to_period("M").astype(str)

def _choose_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    lower = {c.lower(): c for c in df.columns}
    for k in candidates:
        if k.lower() in lower:
            return lower[k.lower()]
    return None

def _choose_val_total_col(df: pd.DataFrame) -> Optional[str]:
    order = [
        "valor_total_compra", "valor_compra", "valor_total",
        "valor", "total",
        "valor_liquido", "valor_lancamento", "vl_total",
        "valor_parcela",
    ]
    return _choose_col(df, order)

# ================= Leitura / preparação =================
def _load_core(db_like: Optional[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    con = _connect(db_like)
    if not con:
        return pd.DataFrame(), pd.DataFrame()
    try:
        # Cartões
        try:
            cards = pd.read_sql('SELECT id, nome FROM "cartoes_credito";', con)
        except Exception:
            cards = pd.read_sql('SELECT id, nome FROM "cartao_credito";', con)

        # Itens
        try:
            itens = pd.read_sql('SELECT * FROM "fatura_cartao_itens";', con)
        except Exception:
            itens = pd.DataFrame()
    except Exception as e:
        st.error("❌ Erro ao ler tabelas de cartão/fatura.")
        st.exception(e)
        return pd.DataFrame(), pd.DataFrame()
    finally:
        try:
            con.close()
        except Exception:
            pass
    return cards, itens

def _filter_itens_by_card(itens: pd.DataFrame, cards: pd.DataFrame, card_id: str) -> pd.DataFrame:
    if itens.empty:
        return itens
    df = itens.copy()
    cols = {c.lower(): c for c in df.columns}
    id_col = next((cols[k] for k in ("cartao_id", "id_cartao", "fk_cartao") if k in cols), None)
    if id_col:
        return df[df[id_col].astype(str) == str(card_id)]
    name_col = cols.get("cartao")
    if name_col:
        try:
            nome = cards.loc[cards["id"].astype(str) == str(card_id), "nome"].astype(str).iloc[0]
        except Exception:
            nome = ""
        if nome:
            return df[df[name_col].astype(str) == nome]
    return df.iloc[0:0].copy()

def _add_comp_mes(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    comp_col = next((cols[k] for k in ("competencia", "competência", "mes_ano", "ano_mes", "competencia_ref", "referencia") if k in cols), None)
    if comp_col:
        comp = _normalize_competencia(df[comp_col])
    else:
        date_col = next((cols[k] for k in ("data_compra", "data", "data_lanc", "data_emissao", "data_pagamento", "vencimento", "data_vencimento", "created_at") if k in cols), None)
        comp = _normalize_competencia(pd.to_datetime(df[date_col], errors="coerce")) if date_col else pd.Series([""] * len(df))
    df = df.copy()
    df["competencia_norm"] = comp
    df["_dt_comp"] = pd.to_datetime(df["competencia_norm"] + "-01", errors="coerce")
    df["mes_num"] = df["_dt_comp"].dt.month
    df["ano_num"] = df["_dt_comp"].dt.year
    return df

def _resumo_por_mes_ano(df_card_norm: pd.DataFrame, ano: int) -> pd.DataFrame:
    if df_card_norm.empty:
        return pd.DataFrame(columns=["Mês", "Total"])
    vcol = _choose_val_total_col(df_card_norm)
    vals = pd.to_numeric(df_card_norm[vcol], errors="coerce").fillna(0.0) if vcol else 0.0
    df = df_card_norm.copy()
    df = df[df["ano_num"] == int(ano)]
    df["valor_norm"] = vals
    agg = df.groupby("mes_num", dropna=True)["valor_norm"].sum().reset_index()
    meses = pd.DataFrame({"mes_num": list(range(1, 13))})
    out = meses.merge(agg, on="mes_num", how="left").fillna({"valor_norm": 0.0})
    out["Mês"] = out["mes_num"].map(PT_BR_MESES)
    out["Total"] = out["valor_norm"].astype(float)
    return out[["Mês", "Total"]]

def _itens_da_competencia(df_card_norm: pd.DataFrame, comp: str) -> pd.DataFrame:
    df = df_card_norm[df_card_norm["competencia_norm"] == str(comp)].copy()
    if df.empty:
        return df
    date_col = _choose_col(df, ["data_compra", "data", "data_lanc", "data_emissao", "data_pagamento", "vencimento", "data_vencimento", "created_at"])
    desc_col = _choose_col(df, ["descricao_compra", "descrição_compra", "descricao", "descrição", "estabelecimento", "historico", "histórico"])
    cat_col  = _choose_col(df, ["categoria", "categoria_compra", "nome_categoria", "grupo_categoria"])
    vcol     = _choose_val_total_col(df)
    out = pd.DataFrame()
    out["Data"] = pd.to_datetime(df[date_col], errors="coerce").dt.date.astype("string") if date_col else ""
    out["Valor"] = pd.to_numeric(df[vcol], errors="coerce").fillna(0.0) if vcol else 0.0
    out["Categoria"] = df[cat_col].astype(str) if cat_col else ""
    out["Descrição"] = df[desc_col].astype(str) if desc_col else ""
    try:
        out = out.sort_values("Data")
    except Exception:
        pass
    return out

# ================= Página =================
def render(_df_unused: pd.DataFrame, caminho_banco: Optional[str] = None) -> None:
    dbp = _resolve_db_path(caminho_banco)
    if not dbp:
        st.error("Não foi possível localizar o banco de dados.")
        return

    cards, itens = _load_core(dbp)
    if cards.empty:
        st.warning("Tabela 'cartoes_credito' (ou 'cartao_credito') vazia ou não encontrada.")
        return
    if itens.empty:
        st.info("Tabela 'fatura_cartao_itens' vazia (sem lançamentos).")
        return

    # Seleção do cartão
    cards = cards.dropna(subset=["id", "nome"]).copy()
    cards["id"] = cards["id"].astype(str)
    nomes_cartoes = cards["nome"].astype(str).tolist()
    cartao_nome = st.selectbox("Escolha o cartão", nomes_cartoes, index=0, key="sel_cartao_fatura")
    try:
        cartao_id = cards.loc[cards["nome"].astype(str) == cartao_nome, "id"].astype(str).iloc[0]
    except Exception:
        st.error("Cartão selecionado inválido.")
        return

    # Itens desse cartão, com competências normalizadas
    df_card = _filter_itens_by_card(itens, cards, cartao_id)
    df_card = _add_comp_mes(df_card)
    if df_card.empty:
        st.info("Não há lançamentos para este cartão.")
        return

    # ---------- Seletor de ANO ----------
    anos_disponiveis = sorted(df_card["ano_num"].dropna().astype(int).unique())
    hoje = date.today()
    idx_padrao_ano = anos_disponiveis.index(hoje.year) if (hoje.year in anos_disponiveis) else len(anos_disponiveis) - 1
    ano_sel = st.selectbox("Ano (competência)", anos_disponiveis, index=idx_padrao_ano, key=f"ano_{cartao_id}")

    # ---------- Tabela 1 (1/4): Jan..Dez do ANO escolhido ----------
    resumo = _resumo_por_mes_ano(df_card, ano_sel)

    # ====== NOVO: Banner “Ano selecionado • Total no ano” ======
    total_ano = float(pd.to_numeric(resumo["Total"], errors="coerce").fillna(0.0).sum()) if not resumo.empty else 0.0
    st.markdown(
        f"""
        <div style="font-size:1.25rem;font-weight:700;margin:6px 0 10px;">
            Ano selecionado: {ano_sel} • Total no ano:
            <span style="color:#00C853;">{_fmt_moeda(total_ano)}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ---------- Botões do mês ----------
    df_btn_source = df_card[df_card["ano_num"] == ano_sel].copy()
    df_btn_source["Data"] = pd.to_datetime(df_btn_source["competencia_norm"] + "-01", errors="coerce")
    df_btn_source = df_btn_source[["Data"]].dropna()

    st.markdown("**Escolha um mês**")
    mes_sel, _ = selecionar_mes(df_btn_source, key=f"mes_{cartao_id}_{ano_sel}", label="")

    # Se nada selecionado e o ano é o atual, abrir no mês corrente (se houver dados)
    meses_disponiveis = set(df_btn_source["Data"].dt.month.tolist())
    if mes_sel is None and ano_sel == hoje.year and hoje.month in meses_disponiveis:
        mes_sel = hoje.month

    # Competência alvo = <ano>-<mês>
    comp_target = f"{ano_sel}-{int(mes_sel):02d}" if mes_sel is not None else None

    # ---------- Layout 1/4 x 3/4 ----------
    col_resumo, col_itens = st.columns([1, 3])

    with col_resumo:
        st.markdown(
            f"**Faturamento por mês no ano** "
            f"<span style='color:#60a5fa;'>{ano_sel}</span>",
            unsafe_allow_html=True,
        )
        df_show = resumo.copy()
        df_show["Total"] = df_show["Total"].map(_fmt_moeda)
        height = _height_exact_rows(len(df_show))  # 12 linhas, sem scroll
        st.dataframe(
            _zebra(df_show[["Mês", "Total"]]),
            use_container_width=True,
            hide_index=True,
            height=height
        )

    with col_itens:
        # Preparar dados do mês selecionado e TOTAL para o título
        if comp_target:
            df_mes_raw = _itens_da_competencia(df_card, comp_target)
            total_mes_num = float(pd.to_numeric(df_mes_raw.get("Valor", 0), errors="coerce").sum()) if not df_mes_raw.empty else 0.0
        else:
            df_mes_raw = pd.DataFrame(columns=["Data", "Valor", "Categoria", "Descrição"])
            total_mes_num = 0.0

        mes_nome = PT_BR_MESES.get(int(mes_sel), "—") if mes_sel is not None else "—"

        st.markdown(
            f"**Detalhe diário do mês** "
            f"<span style='color:#60a5fa;'>{mes_nome}</span> "
            f"— Total: <span style='color:#00C853;'>{_fmt_moeda(total_mes_num)}</span>",
            unsafe_allow_html=True,
        )

        # Render tabela do mês
        if comp_target is None:
            st.caption("Selecione um mês para listar as compras.")
            st.dataframe(df_mes_raw, use_container_width=True, hide_index=True, height=180)
        else:
            if df_mes_raw.empty:
                st.caption(f"Sem compras para {mes_nome}/{ano_sel}.")
                st.dataframe(df_mes_raw, use_container_width=True, hide_index=True, height=180)
            else:
                df_mes = df_mes_raw.copy()
                df_mes["Valor"] = df_mes["Valor"].map(_fmt_moeda)
                st.dataframe(
                    _zebra(df_mes[["Data", "Valor", "Categoria", "Descrição"]]),
                    use_container_width=True,
                    hide_index=True,
                    height=_auto_df_height(df_mes, max_px=900)
                )
