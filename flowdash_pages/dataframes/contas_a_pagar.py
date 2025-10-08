# -*- coding: utf-8 -*-
# flowdash_pages/dataframes/contas_a_pagar.py
from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from typing import Optional, Any, Dict, List
import html
from textwrap import dedent
from datetime import datetime, date

import pandas as pd
import streamlit as st

# ===================== Descoberta de DB (segura) =====================
def _ensure_db_path_or_raise(pref: Optional[str] = None) -> str:
    if pref and os.path.exists(pref):
        return pref
    for k in ("caminho_banco", "db_path"):
        v = st.session_state.get(k)
        if isinstance(v, str) and os.path.exists(v):
            return v
    try:
        from shared.db import get_db_path as _shared_get_db_path  # type: ignore
        p = _shared_get_db_path()
        if isinstance(p, str) and os.path.exists(p):
            return p
    except Exception:
        pass
    for p in (
        os.path.join("data", "flowdash_data.db"),
        os.path.join("data", "dashboard_rc.db"),
        "dashboard_rc.db",
        os.path.join("data", "flowdash_template.db"),
    ):
        if os.path.exists(p):
            return p
    raise FileNotFoundError("Banco de dados não encontrado. Configure st.session_state['caminho_banco'].")


@dataclass
class DB:
    path: str
    def conn(self) -> sqlite3.Connection:
        cx = sqlite3.connect(self.path)
        cx.row_factory = sqlite3.Row
        return cx
    def q(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        try:
            with self.conn() as cx:
                return pd.read_sql_query(sql, cx, params=params)
        except Exception as e:
            st.warning(f"Não foi possível executar a consulta.\n\nSQL: {sql}\n\nErro: {e}")
            return pd.DataFrame()


# ===================== Helpers UI =====================
def _fmt_brl(v: Any) -> str:
    try:
        return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v)

def _first_existing(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
        matches = [col for col in df.columns if col.lower() == c.lower()]
        if matches:
            return matches[0]
    return None

def _month_year_label(y: int, m: int) -> str:
    meses = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]
    return f"{meses[m-1]}/{y}"


# ===================== Loaders básicos =====================
def _table_exists(db: DB, name: str) -> bool:
    sql = "SELECT name FROM sqlite_master WHERE type='table' AND lower(name)=lower(?)"
    try:
        with db.conn() as cx:
            return cx.execute(sql, (name,)).fetchone() is not None
    except Exception:
        return False

def _load_loans_raw(db: DB) -> pd.DataFrame:
    if not _table_exists(db, "emprestimos_financiamentos"):
        return pd.DataFrame()
    return db.q("SELECT * FROM emprestimos_financiamentos")

def _load_cards_catalog(db: DB) -> pd.DataFrame:
    for tb in ("cartoes_creditos", "cartoes_credito", "cartao_credito", "cartoes", "cartoes_cartao"):
        if _table_exists(db, tb):
            df = db.q(f'SELECT * FROM "{tb}"')
            if df.empty:
                continue
            id_col = _first_existing(df, ["id", "Id", "ID", "id_cartao", "cartao_id"])
            name_col = _first_existing(df, ["nome", "descricao", "descrição", "apelido", "titulo"])
            out = pd.DataFrame()
            out["card_id"] = df[id_col].astype(str) if id_col else df.index.astype(str)
            out["card_nome"] = df[name_col].astype(str) if name_col else out["card_id"]
            out["_key_nome_norm"] = out["card_nome"].astype(str).str.strip().str.lower()
            return out
    return pd.DataFrame(columns=["card_id", "card_nome", "_key_nome_norm"])

def _load_contas_apagar_mov(db: DB) -> pd.DataFrame:
    if not _table_exists(db, "contas_a_pagar_mov"):
        return pd.DataFrame()
    return db.q('SELECT * FROM "contas_a_pagar_mov"')

def _load_fatura_itens(db: DB) -> pd.DataFrame:
    if not _table_exists(db, "fatura_cartao_itens"):
        return pd.DataFrame()
    return db.q('SELECT * FROM "fatura_cartao_itens"')

# ---- FIXAS (categoria 4) ----
def _load_subcats_fixas(db: DB) -> pd.DataFrame:
    for tb in ("subcategorias_saida", "subcategoria_saida", "saidas_subcategorias"):
        if not _table_exists(db, tb):
            continue
        df = db.q(f'SELECT * FROM "{tb}"')
        if df.empty:
            continue
        id_col  = _first_existing(df, ["id","Id","ID","id_subcategoria","subcategoria_id"])
        name_col = _first_existing(df, ["nome","descricao","descrição","titulo"])
        cat_col = _first_existing(df, ["categoria_id","id_categoria","categoria"])
        if not name_col:
            continue
        if cat_col:
            df["_cat_id"] = pd.to_numeric(df[cat_col], errors="coerce").fillna(-1).astype(int)
            df = df[df["_cat_id"] == 4]
        out = pd.DataFrame()
        out["subcat_id"] = df[id_col].astype(str) if id_col else df.index.astype(str)
        out["subcat_nome"] = df[name_col].astype(str)
        out["_key_nome_norm"] = out["subcat_nome"].str.strip().str.lower()
        return out.drop_duplicates(subset=["subcat_id","subcat_nome"]).reset_index(drop=True)
    return pd.DataFrame(columns=["subcat_id","subcat_nome","_key_nome_norm"])

def _load_saidas_all(db: DB) -> pd.DataFrame:
    for tb in ("saidas","saida","pagamentos_saida","pagamentos"):
        if _table_exists(db, tb):
            return db.q(f'SELECT * FROM "{tb}"')
    return pd.DataFrame()


# ===================== Empréstimos =====================
def _build_loans_view(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    id_col   = _first_existing(df, ["id", "Id", "ID"])
    desc_col = _first_existing(df, ["descricao", "Descrição", "titulo", "nome", "credor"])

    vparc_col = _first_existing(df, ["valor_parcela", "parcela_valor", "Valor_Parcela", "parcela"])
    sdev_col  = _first_existing(df, ["saldo_devedor", "Saldo_Devedor"])

    vtot_col   = _first_existing(df, ["valor_total", "principal", "valor", "Valor_Total"])
    pagas_col  = _first_existing(df, ["parcelas_pagas", "parcelas_pag", "qtd_parcelas_pagas", "Parcelas_Pagas"])

    out = pd.DataFrame()
    out["id"] = df[id_col].astype(str) if id_col else df.index.astype(str)
    out["descricao"] = df[desc_col].astype(str) if desc_col else "(sem descrição)"
    out["Valor da Parcela Mensal"] = pd.to_numeric(df.get(vparc_col, 0), errors="coerce").fillna(0.0)

    if sdev_col:
        out["Saldo Devedor do Empréstimo"] = pd.to_numeric(df[sdev_col], errors="coerce").fillna(0.0)
    else:
        try:
            tot   = pd.to_numeric(df.get(vtot_col, 0), errors="coerce").fillna(0.0)
            pagas = pd.to_numeric(df.get(pagas_col, 0), errors="coerce").fillna(0.0)
            parc  = out["Valor da Parcela Mensal"]
            out["Saldo Devedor do Empréstimo"] = (tot - (pagas * parc)).clip(lower=0)
        except Exception:
            out["Saldo Devedor do Empréstimo"] = 0.0

    out = out[["id", "descricao", "Saldo Devedor do Empréstimo", "Valor da Parcela Mensal"]].copy()
    out = out.sort_values(by=["descricao", "id"], kind="stable").reset_index(drop=True)
    return out

def _loans_totals(df_view: pd.DataFrame) -> Dict[str, float]:
    if df_view.empty:
        return {"saldo_total": 0.0, "parcelas_total": 0.0}
    return {
        "saldo_total": float(pd.to_numeric(df_view["Saldo Devedor do Empréstimo"], errors="coerce").fillna(0).sum()),
        "parcelas_total": float(pd.to_numeric(df_view["Valor da Parcela Mensal"], errors="coerce").fillna(0).sum()),
    }


# ===================== Cartões =====================
def _normalize_paid_mask(df: pd.DataFrame) -> pd.Series:
    cols = {c.lower(): c for c in df.columns}
    yes = {"1","true","t","sim","s","y","yes","pago","quitado","baixado","baixa","ok","liquidado"}
    if "pago" in cols:
        s = df[cols["pago"]].astype(str).str.lower()
        return s.isin(yes)
    for k in ("quitado","baixado"):
        if k in cols:
            s = df[cols[k]].astype(str).str.lower()
            return s.isin(yes)
    for k in ("status","situacao","situação"):
        if k in cols:
            s = df[cols[k]].astype(str).str.lower().str.strip()
            return s.isin({"pago","quitado","baixado","liquidado"})
    return pd.Series(False, index=df.index)

def _filter_card_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra linhas relacionadas a cartão de crédito.
    (Agora inclui 'credor' como nome do cartão e não faz downcasting silencioso)
    """
    if df.empty:
        return df

    cols = {c.lower(): c for c in df.columns}
    id_cols = [c for c in ("cartao_id","id_cartao","cartao_credito_id","id_cartao_credito") if c in cols]
    name_cols = [c for c in ("cartao","cartão","cartao_nome","nome_cartao","credor") if c in cols]
    tipo_cols = [c for c in ("origem","tipo","categoria","fonte","meio_pagamento","forma_pagamento","tipo_origem") if c in cols]

    if id_cols:
        m = pd.Series(False, index=df.index)
        for c in id_cols:
            ser = df[cols[c]]
            s = ser.notna() & (ser.astype(str).str.strip() != "")
            m = m | s.fillna(False)
        return df[m.fillna(False).astype(bool)]

    if name_cols:
        m = pd.Series(False, index=df.index)
        for c in name_cols:
            ser = df[cols[c]]
            s = ser.notna() & (ser.astype(str).str.strip() != "")
            m = m | s.fillna(False)
        return df[m.fillna(False).astype(bool)]

    for c in tipo_cols:
        s = df[cols[c]].astype(str).str.lower()
        return df[s.str.contains("cartao") | s.str.contains("cartão")]

    return df


def _pick_amount_col(df: pd.DataFrame) -> Optional[str]:
    # >>> Inclui 'valor_evento' (do teu contas_a_pagar_mov)
    return _first_existing(
        df,
        ["valor_evento","valor_a_pagar","valor_parcela","valor","valor_total","valor_pago","valor_saida","parcela_valor","valor_fatura"]
    )

def _pick_due_col(df: pd.DataFrame) -> Optional[str]:
    # Inclui 'competencia' e 'data_evento' como alternativas
    return _first_existing(df, ["data_vencimento","vencimento","data_fatura","competencia","data","data_evento"])

def _parse_competencia(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().str.replace("/", "-", regex=False)
    return pd.to_datetime(s, errors="coerce")


def _cards_view(db: DB, ref_year: int, ref_month: int) -> pd.DataFrame:
    """
    Une:
      - em_aberto_total: de contas_a_pagar_mov (usando status/pago + vencimento)
      - fatura_mes_total: de fatura_cartao_itens (competencia)
    Quase sempre tu vai ter as duas fontes; quando uma faltar, a outra cobre.
    """
    cards_cat = _load_cards_catalog(db)
    mov = _load_contas_apagar_mov(db)
    fat = _load_fatura_itens(db)

    # === Base (catálogo)
    if cards_cat.empty:
        base = pd.DataFrame(columns=["card_id","card_nome","_key_nome_norm"])
    else:
        base = cards_cat[["card_id","card_nome","_key_nome_norm"]].copy()

    # === Em aberto + (fallback para fatura do mês) vindos de MOV
    em_aberto_by_id = pd.DataFrame(columns=["card_id","em_aberto_total","fatura_mes_total_mov"])
    if not mov.empty:
        mv = _filter_card_rows(mov.copy())
        if not mv.empty:
            amount_col = _pick_amount_col(mv)
            due_col = _pick_due_col(mv)
            paid = _normalize_paid_mask(mv)
            mv["_valor"] = pd.to_numeric(mv[amount_col], errors="coerce").fillna(0.0) if amount_col else 0.0
            if due_col:
                if due_col.lower() == "competencia":
                    mv["_venc"] = _parse_competencia(mv[due_col])
                else:
                    mv["_venc"] = pd.to_datetime(mv[due_col], errors="coerce")
            else:
                mv["_venc"] = pd.NaT

            cols = {c.lower(): c for c in mv.columns}
            id_colm = next((cols[c] for c in ("cartao_id","id_cartao","cartao_credito_id","id_cartao_credito") if c in cols), None)
            if id_colm is None and not base.empty:
                name_colm = next((cols[c] for c in ("cartao","cartão","cartao_nome","nome_cartao","credor") if c in cols), None)
                if name_colm:
                    mv["_key_nome_norm"] = mv[name_colm].astype(str).str.strip().str.lower()
                    mv = mv.merge(base[["_key_nome_norm","card_id"]], on="_key_nome_norm", how="left")
                    id_colm = "card_id"

            if id_colm is not None:
                id_ser = pd.to_numeric(mv[id_colm], errors="coerce").astype("Int64").astype(str)
                is_mes = mv["_venc"].dt.month.eq(ref_month) & mv["_venc"].dt.year.eq(ref_year)
                grp = mv.groupby(id_ser, dropna=False, sort=True)
                em_aberto_by_id = pd.DataFrame({
                    "card_id": grp.size().index.astype(str),
                    "em_aberto_total": grp.apply(lambda g: float((g.loc[(~paid).loc[g.index], "_valor"]).sum())),
                    "fatura_mes_total_mov": grp.apply(lambda g: float((g.loc[(~paid).loc[g.index] & is_mes.loc[g.index], "_valor"]).sum())),
                }).reset_index(drop=True)

    # === Fatura do mês vinda de FATURA_CARTAO_ITENS
    fatura_by_name = pd.DataFrame(columns=["_key_nome_norm","fatura_mes_total_fat"])
    if not fat.empty:
        f = fat.copy()
        cart_col = _first_existing(f, ["cartao","cartão","cartao_nome","nome_cartao"])
        val_col  = _first_existing(f, ["valor_parcela","valor_fatura","valor","valor_total"])
        comp_col = _first_existing(f, ["competencia","data_fatura","mes"])
        f["_valor"] = pd.to_numeric(f[val_col], errors="coerce").fillna(0.0) if val_col else 0.0
        if comp_col:
            if comp_col.lower() == "competencia":
                f["_comp"] = _parse_competencia(f[comp_col])
            else:
                f["_comp"] = pd.to_datetime(f[comp_col], errors="coerce")
        else:
            f["_comp"] = pd.NaT
        is_mes = f["_comp"].dt.month.eq(ref_month) & f["_comp"].dt.year.eq(ref_year)
        f = f[is_mes]
        if cart_col:
            f["_key_nome_norm"] = f[cart_col].astype(str).str.strip().str.lower()
        else:
            f["_key_nome_norm"] = "cartao"
        fatura_by_name = f.groupby("_key_nome_norm", dropna=False, sort=True)["_valor"].sum().reset_index(name="fatura_mes_total_fat")

    # === Monta saída final
    if base.empty:
        if not em_aberto_by_id.empty:
            df = em_aberto_by_id.copy()
            df["card_nome"] = df["card_id"]
            df["fatura_mes_total"] = df.get("fatura_mes_total_mov", 0.0)
            return df[["card_id","card_nome","em_aberto_total","fatura_mes_total"]].sort_values("card_nome").reset_index(drop=True)
        if not fatura_by_name.empty:
            df = fatura_by_name.copy()
            df["card_id"] = df["_key_nome_norm"]
            df["card_nome"] = df["_key_nome_norm"]
            df["em_aberto_total"] = 0.0
            df["fatura_mes_total"] = df["fatura_mes_total_fat"]
            return df[["card_id","card_nome","em_aberto_total","fatura_mes_total"]].sort_values("card_nome").reset_index(drop=True)
        return pd.DataFrame(columns=["card_id","card_nome","em_aberto_total","fatura_mes_total"])

    out = base.copy()
    if not em_aberto_by_id.empty:
        out = out.merge(em_aberto_by_id, on="card_id", how="left")
    if not fatura_by_name.empty:
        out = out.merge(fatura_by_name, on="_key_nome_norm", how="left")

    out["em_aberto_total"] = pd.to_numeric(out.get("em_aberto_total"), errors="coerce").fillna(0.0) if "em_aberto_total" in out.columns else 0.0
    out["fatura_mes_total"] = 0.0
    if "fatura_mes_total_fat" in out.columns:
        out["fatura_mes_total"] = pd.to_numeric(out["fatura_mes_total_fat"], errors="coerce").fillna(0.0)
    if "fatura_mes_total_mov" in out.columns:
        out["fatura_mes_total"] = out["fatura_mes_total"].where(out["fatura_mes_total"] > 0,
                                                                pd.to_numeric(out["fatura_mes_total_mov"], errors="coerce").fillna(0.0))
    return out[["card_id","card_nome","em_aberto_total","fatura_mes_total"]].sort_values("card_nome").reset_index(drop=True)

def _cards_totals(df_cards_view: pd.DataFrame) -> Dict[str, float]:
    if df_cards_view.empty:
        return {"aberto_total": 0.0, "faturas_mes_total": 0.0}
    return {
        "aberto_total": float(pd.to_numeric(df_cards_view["em_aberto_total"], errors="coerce").fillna(0).sum()),
        "faturas_mes_total": float(pd.to_numeric(df_cards_view["fatura_mes_total"], errors="coerce").fillna(0).sum()),
    }


# ===================== FIXAS: painel (Pago / Sem) com soma por subcategoria =====================
def _build_fixed_panel_status(subcats: pd.DataFrame, saidas: pd.DataFrame, ref_year: int, ref_month: int) -> pd.DataFrame:
    out_cols = ["subcat_id", "subcat_nome", "status", "valor_mes"]

    if subcats.empty:
        return pd.DataFrame(columns=out_cols)

    if saidas.empty:
        df = subcats.copy()
        df["status"] = "sem"
        df["valor_mes"] = 0.0
        return df[out_cols]

    cat_col  = _first_existing(saidas, ["Categoria", "categoria", "categoria_nome", "nome_categoria"])
    sub_col  = _first_existing(saidas, ["Sub_Categoria", "subcategoria", "Subcategoria", "nome_subcategoria", "subcategoria_nome"])
    valor_col = _first_existing(saidas, ["Valor", "valor", "valor_saida", "valor_total"])
    data_col  = _first_existing(saidas, ["Data", "data", "data_vencimento", "vencimento", "competencia"])

    if not (cat_col and sub_col and valor_col and data_col):
        df = subcats.copy()
        df["status"] = "sem"
        df["valor_mes"] = 0.0
        return df[out_cols]

    def _norm_txt(x: Any) -> str:
        return str(x).strip().lower()

    saidas = saidas.copy()
    saidas["_dt"] = pd.to_datetime(saidas[data_col], errors="coerce")
    m_mes = saidas["_dt"].dt.month.eq(ref_month) & saidas["_dt"].dt.year.eq(ref_year)

    cat_norm = saidas[cat_col].map(_norm_txt)
    CATS_OK = {"custos fixos", "contas fixas", "fixas", "despesas fixas"}
    m_cat = cat_norm.isin(CATS_OK)

    saidas["_valor"] = pd.to_numeric(saidas[valor_col], errors="coerce").fillna(0.0).clip(lower=0)
    saidas["_sub"] = saidas[sub_col].map(_norm_txt)

    base_mes = saidas[m_mes & m_cat]

    rows: List[Dict[str, Any]] = []
    for _, r in subcats.iterrows():
        sid = str(r.get("subcat_id", ""))
        nome = str(r.get("subcat_nome", ""))
        key = _norm_txt(nome)

        soma = float(base_mes.loc[base_mes["_sub"] == key, "_valor"].sum())
        status = "pago" if soma > 0.0 else "sem"

        rows.append({"subcat_id": sid, "subcat_nome": nome, "status": status, "valor_mes": soma})

    df_out = pd.DataFrame(rows, columns=out_cols)
    return df_out.sort_values("subcat_nome").reset_index(drop=True)


# ===================== Render =====================
def render(db_path_pref: Optional[str] = None):

    # CSS (acrescentadas classes do painel de fixas)
    st.markdown("""
    <style>
      .cap-card { border: 1px solid rgba(255,255,255,0.10); border-radius: 16px; padding: 14px 16px;
                  background: rgba(255,255,255,0.03); box-shadow: 0 1px 4px rgba(0,0,0,0.10); }
      .cap-card-lg { padding: 18px 20px; border-width: 1.5px; }
      .cap-title-xl { font-size: 1.25rem; font-weight: 700; margin-bottom: 10px; }
      .cap-metrics-row { display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 14px; }
      .cap-metrics-row.cap-3col { grid-template-columns: repeat(3, minmax(0,1fr)); }
      .cap-metrics-row.cap-1col { grid-template-columns: 1fr; }
      .cap-metric { background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.08);
                    border-radius: 12px; padding: 10px 12px; }
      .cap-metric-accent { background: rgba(34,197,94,0.12); border-color: rgba(34,197,94,0.35); }
      .cap-label { font-size: 0.85rem; opacity: 0.85; margin-bottom: 4px; }
      .cap-value { font-size: 1.35rem; font-weight: 700; }
      .cap-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 12px; }
      .cap-card h4 { margin: 0 0 8px 0; font-size: 1rem; }
      .cap-sub { font-size: .80rem; opacity: .75; margin: -4px 0 8px 0; }

      .cap-h3 { font-size: 1.5rem; font-weight: 800; margin: 14px 0 8px; }
      .cap-h4 { font-size: 1.05rem; font-weight: 700; margin: 2px 0 10px; opacity: .95; }
      .cap-red    { color: #ef4444 !important; }
      .cap-purple { color: #a78bfa !important; }
      .cap-cyan   { color: #22d3ee !important; }
      .cap-amber  { color: #f59e0b !important; }
      .cap-green  { color: #22c55e !important; }

      /* NOVO: estilos do painel de Contas Fixas (iguais ao do segundo código) */
      .cap-chips-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:10px; }
      .cap-chip { display:flex; align-items:center; justify-content:space-between; gap:8px; padding:8px 10px;
                  border:1px solid rgba(255,255,255,0.10); border-radius:12px;
                  background: rgba(255,255,255,0.04); }
      .cap-chip-left { display:flex; align-items:center; gap:8px; min-width: 0; }
      .cap-dot { width:12px; height:12px; border-radius:50%; border:1px solid rgba(255,255,255,0.35); flex:0 0 auto; }
      .cap-dot.pago { background:#10b981; }  /* verde */
      .cap-dot.sem  { background:#6b7280; }  /* cinza */
      .cap-badge { font-size:.80rem; padding:2px 8px; border-radius:9999px;
                   background: rgba(255,255,255,0.08); border:1px solid rgba(255,255,255,0.12); }
      .cap-badge.muted { opacity:.6; }
      .cap-legend { display:flex; gap:14px; font-size:.85rem; opacity:.85; margin-bottom:8px; }
      .cap-legend span { display:flex; align-items:center; gap:6px; }

      /* Centralizar conteúdo de métricas destacadas, se usado */
      .cap-center { text-align: center; display: flex; flex-direction: column; align-items: center; justify-content: center; }

      @media (max-width: 900px) { .cap-metrics-row, .cap-metrics-row.cap-3col, .cap-metrics-row.cap-1col { grid-template-columns: 1fr; } }
    </style>
    """, unsafe_allow_html=True)

    # ===== Seletor de MÊS/ANO =====
    hoje = date.today()
    meses_labels = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]
    col_mes, col_ano = st.columns([2,1])
    with col_mes:
        ref_month = st.selectbox("📅 Mês", options=list(range(1,13)), index=hoje.month - 1, format_func=lambda m: meses_labels[m-1])
    with col_ano:
        anos_opts = list(range(hoje.year - 5, hoje.year + 2))
        ref_year = st.selectbox("Ano", options=anos_opts, index=anos_opts.index(hoje.year))
    st.caption(f"Exibindo dados de {_month_year_label(ref_year, ref_month)}.")

    # ===== DB =====
    try:
        db = DB(_ensure_db_path_or_raise(db_path_pref))
    except Exception as e:
        st.error(str(e)); return

    # ===== CÁLCULOS =====
    df_loans_raw = _load_loans_raw(db)
    df_loans = _build_loans_view(df_loans_raw) if not df_loans_raw.empty else pd.DataFrame()
    loans_sums = _loans_totals(df_loans)

    # >>>>>> NOVO: usa visão combinada dos cartões
    df_cards_view = _cards_view(db, ref_year, ref_month)
    cards_sums = _cards_totals(df_cards_view)

    subcats = _load_subcats_fixas(db)
    saidas_all = _load_saidas_all(db)
    painel = _build_fixed_panel_status(subcats, saidas_all, ref_year, ref_month)
    total_fixas_mes = float(pd.to_numeric(painel["valor_mes"], errors="coerce").fillna(0).sum()) if not painel.empty else 0.0

    # ===== NOVO CARD GERAL =====
    total_saldo = loans_sums["saldo_total"] + cards_sums["aberto_total"]
    total_parcelas_mes = loans_sums["parcelas_total"] + cards_sums["faturas_mes_total"]
    total_mes_geral = total_parcelas_mes + total_fixas_mes

    novo_top_geral = dedent(f"""
    <div class="cap-card cap-card-lg">
      <div class="cap-title-xl cap-red">Total Geral — {_month_year_label(ref_year, ref_month)}</div>
      <div class="cap-metrics-row cap-3col">
        <div class="cap-metric"><div class="cap-label">Saldo devedor (cartões + empréstimos)</div><div class="cap-value">{_fmt_brl(total_saldo)}</div></div>
        <div class="cap-metric"><div class="cap-label">Parcelas do mês (cartões + empréstimos)</div><div class="cap-value">{_fmt_brl(total_parcelas_mes)}</div></div>
        <div class="cap-metric"><div class="cap-label">Gastos fixos (mês)</div><div class="cap-value">{_fmt_brl(total_fixas_mes)}</div></div>
      </div>
      <div class="cap-metrics-row cap-1col" style="margin-top:10px;">
        <div class="cap-metric cap-metric-accent cap-center">
          <div class="cap-label cap-green">Total do mês (Parcelas Empréstimos e Fatura + Contas Fixas)</div>
          <div class="cap-value cap-green">{_fmt_brl(total_mes_geral)}</div>
        </div>
      </div>
    </div>
    """).strip()
    st.markdown(novo_top_geral, unsafe_allow_html=True)

    st.divider()

    # ===== Contas Fixas (painel) — REFEITO IGUAL AO SEGUNDO CÓDIGO =====
    st.markdown('<h3 class="cap-h3 cap-amber">Contas Fixas (painel)</h3>', unsafe_allow_html=True)
    if painel.empty:
        st.info("Nenhuma subcategoria de contas fixas (categoria 4) encontrada.")
    else:
        top_metric = f"""
        <div class="cap-card cap-card-lg">
          <div class="cap-title-xl">Contas fixas — {_month_year_label(ref_year, ref_month)}</div>
          <div class="cap-metrics-row cap-1col">
            <div class="cap-metric">
              <div class="cap-label">Total gasto fixo (mês)</div>
              <div class="cap-value">{_fmt_brl(total_fixas_mes)}</div>
            </div>
          </div>
          <div class="cap-legend" style="margin-top:8px;">
            <span><span class="cap-dot pago"></span>Pago</span>
            <span><span class="cap-dot sem"></span>Sem lançamento</span>
          </div>
          <div class="cap-chips-grid">
            {''.join(
              f'<div class="cap-chip">'
              f'  <div class="cap-chip-left"><span class="cap-dot {"pago" if r.status=="pago" else "sem"}"></span><span>{html.escape(str(r.subcat_nome))}</span></div>'
              f'  <span class="cap-badge{" muted" if float(r.valor_mes) <= 0 else ""}">{_fmt_brl(r.valor_mes)}</span>'
              f'</div>'
              for r in painel.sort_values("subcat_nome").itertuples(index=False)
            )}
          </div>
        </div>
        """
        st.markdown(top_metric, unsafe_allow_html=True)

    st.divider()

    # ===== Empréstimos =====
    st.markdown('<h3 class="cap-h3 cap-purple">Empréstimos</h3>', unsafe_allow_html=True)
    if df_loans_raw.empty:
        st.info("Nenhum empréstimo encontrado (tabela esperada: `emprestimos_financiamentos`).")
    else:
        top_html = dedent(f"""
        <div class="cap-card cap-card-lg">
          <div class="cap-title-xl">Total Empréstimos</div>
          <div class="cap-metrics-row">
            <div class="cap-metric"><div class="cap-label">Saldo devedor de todos empréstimos</div><div class="cap-value">{_fmt_brl(loans_sums['saldo_total'])}</div></div>
            <div class="cap-metric"><div class="cap-label">Parcela somada (mês) — todos os empréstimos</div><div class="cap-value">{_fmt_brl(loans_sums['parcelas_total'])}</div></div>
          </div>
        </div>
        """).strip()
        st.markdown(top_html, unsafe_allow_html=True)

        if not df_loans.empty:
            cards: List[str] = ['<div class="cap-grid">']
            for _, r in df_loans.iterrows():
                emp_id = html.escape(str(r["id"]))
                desc_raw = str(r.get("descricao", "") or "")
                desc = html.escape(desc_raw)
                titulo = desc if desc_raw and desc_raw != "(sem descrição)" else f"Empréstimo {emp_id}"
                cards.append(dedent(f"""
                <div class="cap-card">
                  <h4>{titulo}</h4>
                  <div class="cap-metrics-row">
                    <div class="cap-metric"><div class="cap-label">Saldo devedor</div><div class="cap-value">{_fmt_brl(r["Saldo Devedor do Empréstimo"])}</div></div>
                    <div class="cap-metric"><div class="cap-label">Parcela (mês)</div><div class="cap-value">{_fmt_brl(r["Valor da Parcela Mensal"])}</div></div>
                  </div>
                </div>
                """).strip())
            cards.append("</div>")
            st.markdown("\n".join(cards), unsafe_allow_html=True)

    st.divider()

    # ===== Cartões =====
    st.markdown('<h3 class="cap-h3 cap-cyan">Fatura Cartão de Crédito</h3>', unsafe_allow_html=True)
    if df_cards_view.empty:
        st.info("Sem cartões/faturas localizados (tabelas esperadas: `cartoes_credito`, `fatura_cartao_itens` e/ou `contas_a_pagar_mov`).")
    else:
        top_cards = dedent(f"""
        <div class="cap-card cap-card-lg">
          <div class="cap-title-xl">Total Cartões</div>
          <div class="cap-metrics-row">
            <div class="cap-metric"><div class="cap-label">Valor em aberto (todos os cartões)</div><div class="cap-value">{_fmt_brl(cards_sums['aberto_total'])}</div></div>
            <div class="cap-metric"><div class="cap-label">Faturas do mês (somadas)</div><div class="cap-value">{_fmt_brl(cards_sums['faturas_mes_total'])}</div></div>
          </div>
        </div>
        """).strip()
        st.markdown(top_cards, unsafe_allow_html=True)

        cards_html = ['<div class="cap-grid">']
        for _, r in df_cards_view.iterrows():
            nome = html.escape(str(r["card_nome"]))
            cards_html.append(dedent(f"""
            <div class="cap-card">
              <h4>{nome}</h4>
              <div class="cap-metrics-row">
                <div class="cap-metric"><div class="cap-label">Em aberto</div><div class="cap-value">{_fmt_brl(r["em_aberto_total"])}</div></div>
                <div class="cap-metric"><div class="cap-label">Fatura (mês)</div><div class="cap-value">{_fmt_brl(r["fatura_mes_total"])}</div></div>
              </div>
            </div>
            """).strip())
        cards_html.append("</div>")
        st.markdown("\n".join(cards_html), unsafe_allow_html=True)


if __name__ == "__main__":
    st.set_page_config(page_title="Contas a Pagar", layout="wide")
    render()
