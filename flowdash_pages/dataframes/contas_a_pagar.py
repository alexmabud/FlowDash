# -*- coding: utf-8 -*-
# flowdash_pages/dataframes/contas_a_pagar.py
from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from typing import Optional, Any, Dict, List
import html
from textwrap import dedent
from datetime import date

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

# ===================== Utilitários comuns CAP =====================
def _pick_amount_col(df: pd.DataFrame) -> Optional[str]:
    return _first_existing(df, [
        "valor_evento","valor_a_pagar","valor_parcela","valor","valor_total",
        "valor_saida","parcela_valor","valor_fatura"
    ])

def _pick_paid_col(df: pd.DataFrame) -> Optional[str]:
    # Prioriza campos explícitos de pagamento no CAP
    return _first_existing(df, [
        "valor_pago", "valor_pago_mes", "valor_baixa", "valor_liquidado",
        "valor_pago_acumulado"  # fallback (pode superestimar se for acumulado)
    ])

def _pick_due_col(df: pd.DataFrame) -> Optional[str]:
    return _first_existing(df, ["competencia","data_vencimento","vencimento","data_fatura","data","data_evento"])

def _parse_competencia(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().str.replace("/", "-", regex=False)
    return pd.to_datetime(s, errors="coerce")

def _best_due_series(df: pd.DataFrame) -> pd.Series:
    order = ["competencia","vencimento","data_vencimento","data_evento","data_fatura","data"]
    best = None
    best_count = -1
    for c in order:
        if c in df.columns:
            dt = _parse_competencia(df[c]) if c.lower() == "competencia" else pd.to_datetime(df[c], errors="coerce")
            n = int(dt.notna().sum())
            if n > best_count:
                best, best_count = dt, n
    return best if best is not None else pd.Series(pd.NaT, index=df.index)

def _calc_status_from_paid(mensal: float, pago: float, eps: float = 0.005) -> str:
    m = float(mensal or 0.0)
    p = float(pago or 0.0)
    if p <= eps:
        return "nada"            # sem pagamento
    if p + eps < m - eps:
        return "parcial"         # pago parcialmente
    return "ok"                  # quitado (≈ ou >)

# ===================== Contas Fixas (painel) =====================
def _build_fixed_panel_status(
    subcats: pd.DataFrame,
    saidas_all: pd.DataFrame,
    ref_year: int,
    ref_month: int,
) -> pd.DataFrame:
    out_cols = ["subcat_id", "subcat_nome", "valor_mes", "status"]
    if subcats is None or subcats.empty or saidas_all is None or saidas_all.empty:
        return pd.DataFrame(columns=out_cols)

    df = saidas_all.copy()
    cols = {c.lower(): c for c in df.columns}

    cat_col  = next((cols[c] for c in ("categoria", "categoria_saida", "grupo") if c in cols), None)
    subc_col = next((cols[c] for c in ("sub_categoria", "sub-categoria", "subcategoria", "subcategoria_saida") if c in cols), None)
    val_col  = _pick_amount_col(df)
    due_col  = _pick_due_col(df)

    if not val_col or not due_col or not subc_col:
        return pd.DataFrame(columns=out_cols)

    df["_valor"] = pd.to_numeric(df[val_col], errors="coerce").fillna(0.0)
    if due_col.lower() == "competencia":
        df["_dt"] = _parse_competencia(df[due_col])
    else:
        df["_dt"] = pd.to_datetime(df[due_col], errors="coerce")

    m_mes = df["_dt"].dt.month.eq(ref_month) & df["_dt"].dt.year.eq(ref_year)

    if cat_col:
        s = df[cat_col].astype(str).str.lower().str.strip()
        m_fixas = (s.isin({"custos fixos", "custo fixo", "fixo", "fixas"}) | s.str.contains("fixo"))
    else:
        m_fixas = pd.Series(True, index=df.index)

    df["_sub_nome_norm"] = df[subc_col].astype(str).str.strip().str.lower().replace({"": None})

    grp_mes = (
        df.loc[m_mes & m_fixas & df["_sub_nome_norm"].notna(), ["_sub_nome_norm", "_valor"]]
          .groupby("_sub_nome_norm", dropna=False, sort=True)["_valor"]
          .sum()
          .rename("valor_mes")
          .reset_index()
    )

    base = subcats[["subcat_id", "subcat_nome", "_key_nome_norm"]].copy().rename(columns={"_key_nome_norm": "_sub_nome_norm"})
    painel = base.merge(grp_mes, on="_sub_nome_norm", how="left")
    painel["valor_mes"] = pd.to_numeric(painel["valor_mes"], errors="coerce").fillna(0.0)
    painel["status"] = painel["valor_mes"].apply(lambda v: "pago" if v > 0 else "pendente")
    return painel[["subcat_id", "subcat_nome", "valor_mes", "status"]].sort_values("subcat_nome").reset_index(drop=True)


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

# === Parcelas de EMPRÉSTIMOS (CAP) ===
def _loans_month_total_from_cap(db: DB, ref_year: int, ref_month: int) -> float:
    cap = _load_contas_apagar_mov(db)
    if cap.empty:
        return 0.0
    df = cap.copy()
    df["_dt"] = _best_due_series(df)
    is_mes = df["_dt"].dt.month.eq(ref_month) & df["_dt"].dt.year.eq(ref_year)
    val_col = _pick_amount_col(df)
    df["_valor"] = pd.to_numeric(df[val_col], errors="coerce").fillna(0.0) if val_col else 0.0

    cols = {c.lower(): c for c in df.columns}
    loan_id_col = next((cols[c] for c in ("emprestimo_id","id_emprestimo","loan_id") if c in cols), None)

    if loan_id_col:
        m_loan = df[loan_id_col].notna() & (df[loan_id_col].astype(str).str.strip() != "")
    else:
        hint_cols = [c for c in ("categoria","origem","tipo","fonte","classe","grupo") if c in cols]
        if hint_cols:
            m_list = []
            for c in hint_cols:
                s = df[cols[c]].astype(str).str.lower()
                m_list.append(s.str.contains("emprest"))
            m_loan = pd.concat(m_list, axis=1).any(axis=1)
        else:
            m_loan = pd.Series(False, index=df.index)

    total = float(df.loc[is_mes & m_loan, "_valor"].sum())
    return total


# ===================== Cartões =====================
def _normalize_paid_mask(df: pd.DataFrame) -> pd.Series:
    # Mantido por compatibilidade, mas preferimos somar valor_pago quando disponível
    cols = {c.lower(): c for c in df.columns}
    yes = {"1","true","t","sim","s","y","yes","pago","quitado","baixado","ok","liquidado"}
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


def _cards_view(db: DB, ref_year: int, ref_month: int) -> pd.DataFrame:
    cards_cat = _load_cards_catalog(db)
    mov = _load_contas_apagar_mov(db)
    fat = _load_fatura_itens(db)

    if cards_cat.empty:
        base = pd.DataFrame(columns=["card_id","card_nome","_key_nome_norm"])
    else:
        base = cards_cat[["card_id","card_nome","_key_nome_norm"]].copy()

    em_aberto_by_id = pd.DataFrame(columns=["card_id","em_aberto_total","fatura_mes_total_mov"])
    if not mov.empty:
        mv = _filter_card_rows(mov.copy())
        if not mv.empty:
            amount_col = _pick_amount_col(mv)
            due_col = _pick_due_col(mv)
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
                # em_aberto_total somente itens não quitados ainda (sem usar valor_pago)
                paid_mask = _normalize_paid_mask(mv)
                em_aberto_by_id = pd.DataFrame({
                    "card_id": grp.size().index.astype(str),
                    "em_aberto_total": grp.apply(lambda g: float((g.loc[(~paid_mask).loc[g.index], "_valor"]).sum())),
                    "fatura_mes_total_mov": grp.apply(lambda g: float((g.loc[(~paid_mask).loc[g.index] & is_mes.loc[g.index], "_valor"]).sum())),
                }).reset_index(drop=True)

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

# ===================== Boletos (CAP) =====================
def _boletos_flag_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(False, index=df.index)
    cols = {c.lower(): c for c in df.columns}
    m_direct = pd.Series(False, index=df.index)
    if "tipo_obrigacao" in cols:
        s = df[cols["tipo_obrigacao"]].astype(str).str.upper().str.strip()
        m_direct = s.eq("BOLETO") | s.str.contains("BOLET")
    text_fields = ["tipo_obrigacao","tipo_origem","forma_pagamento","categoria_evento","categoria","origem","tipo","fonte","classe","grupo","descricao","descrição","titulo","título","credor","fornecedor"]
    m_text = pd.Series(False, index=df.index)
    for key in text_fields:
        c = cols.get(key)
        if not c:
            continue
        s = df[c].astype(str).str.lower()
        m_text = m_text | s.str.contains("boleto")
    return (m_direct | m_text).fillna(False)

def _boletos_month_total_from_cap(db: DB, ref_year: int, ref_month: int) -> float:
    cap = _load_contas_apagar_mov(db)
    if cap.empty:
        return 0.0
    df = cap.copy()
    df["_dt"] = _best_due_series(df)
    is_mes = df["_dt"].dt.month.eq(ref_month) & df["_dt"].dt.year.eq(ref_year)
    val_col = _pick_amount_col(df)
    df["_valor"] = pd.to_numeric(df[val_col], errors="coerce").fillna(0.0) if val_col else 0.0
    m_bol = _boletos_flag_mask(df)
    return float(df.loc[is_mes & m_bol, "_valor"].sum())

def _build_boletos_view(db: DB, ref_year: int, ref_month: int) -> pd.DataFrame:
    cap = _load_contas_apagar_mov(db)
    if cap.empty:
        return pd.DataFrame(columns=["id","descricao","Saldo Devedor do Boleto","Valor da Parcela Mensal"])
    df = cap.copy()
    df["_dt"] = _best_due_series(df)
    val_col = _pick_amount_col(df)
    df["_valor"] = pd.to_numeric(df[val_col], errors="coerce").fillna(0.0) if val_col else 0.0
    paid = _normalize_paid_mask(df)
    m_bol = _boletos_flag_mask(df)

    cols = {c.lower(): c for c in df.columns}
    fonte_col = next((cols[c] for c in ("credor","fornecedor","descricao","descrição","titulo","título") if c in cols), None)
    fonte = (df[fonte_col].astype(str).str.strip() if fonte_col else pd.Series(["Boleto"]*len(df), index=df.index)).replace({"": "Boleto"})

    is_mes = df["_dt"].dt.month.eq(ref_month) & df["_dt"].dt.year.eq(ref_year)
    base_mes = pd.DataFrame({"fonte": fonte, "valor": df["_valor"]})[is_mes & m_bol]
    parcela_mes = base_mes.groupby("fonte", dropna=False, sort=True)["valor"].sum().rename("Valor da Parcela Mensal")

    base_aberto = pd.DataFrame({"fonte": fonte, "valor": df["_valor"]})[m_bol & (~paid.fillna(False))]
    sdev = base_aberto.groupby("fonte", dropna=False, sort=True)["valor"].sum().rename("Saldo Devedor do Boleto")

    out = pd.concat([sdev, parcela_mes], axis=1).fillna(0.0).reset_index()
    out = out.rename(columns={"fonte": "descricao"})
    out.insert(0, "id", out["descricao"].astype(str))
    out = out[["id","descricao","Saldo Devedor do Boleto","Valor da Parcela Mensal"]].sort_values(["descricao","id"], kind="stable").reset_index(drop=True)
    return out

def _boletos_totals_view(df_view: pd.DataFrame) -> Dict[str, float]:
    if df_view.empty:
        return {"saldo_total": 0.0, "parcelas_total": 0.0}
    return {
        "saldo_total": float(pd.to_numeric(df_view["Saldo Devedor do Boleto"], errors="coerce").fillna(0).sum()),
        "parcelas_total": float(pd.to_numeric(df_view["Valor da Parcela Mensal"], errors="coerce").fillna(0).sum()),
    }

# ===================== PAGOS NO MÊS (usando valor_pago) =====================
def _cap_month_paid_by_loan(db: DB, ref_year: int, ref_month: int) -> pd.DataFrame:
    cap = _load_contas_apagar_mov(db)
    if cap.empty:
        return pd.DataFrame(columns=["loan_key","pago_mes"])
    df = cap.copy()
    df["_dt"] = _best_due_series(df)

    val_col = _pick_amount_col(df)
    df["_valor"] = pd.to_numeric(df[val_col], errors="coerce").fillna(0.0) if val_col else 0.0

    paid_col = _pick_paid_col(df)
    if paid_col:
        df["_pago"] = pd.to_numeric(df[paid_col], errors="coerce").fillna(0.0)
    else:
        # Fallback: se não houver valor_pago, usa máscara booleana para contar valor integral
        mask = _normalize_paid_mask(df)
        df["_pago"] = df["_valor"].where(mask, 0.0)

    is_mes = df["_dt"].dt.month.eq(ref_month) & df["_dt"].dt.year.eq(ref_year)
    cols = {c.lower(): c for c in df.columns}
    loan_id_col = next((cols[c] for c in ("emprestimo_id","id_emprestimo","loan_id") if c in cols), None)
    if loan_id_col is None:
        return pd.DataFrame(columns=["loan_key","pago_mes"])
    grp = df[is_mes].groupby(df[loan_id_col].astype(str), dropna=False, sort=True)["_pago"].sum()
    return grp.reset_index().rename(columns={loan_id_col: "loan_key", "_pago": "pago_mes"})

def _cap_month_paid_by_card(db: DB, ref_year: int, ref_month: int, base_cards: pd.DataFrame) -> pd.DataFrame:
    cap = _load_contas_apagar_mov(db)
    if cap.empty:
        return pd.DataFrame(columns=["card_id","pago_mes"])
    df = cap.copy()
    df["_dt"] = _best_due_series(df)

    val_col = _pick_amount_col(df)
    df["_valor"] = pd.to_numeric(df[val_col], errors="coerce").fillna(0.0) if val_col else 0.0

    paid_col = _pick_paid_col(df)
    if paid_col:
        df["_pago"] = pd.to_numeric(df[paid_col], errors="coerce").fillna(0.0)
    else:
        mask = _normalize_paid_mask(df)
        df["_pago"] = df["_valor"].where(mask, 0.0)

    is_mes = df["_dt"].dt.month.eq(ref_month) & df["_dt"].dt.year.eq(ref_year)
    cols = {c.lower(): c for c in df.columns}
    id_colm = next((cols[c] for c in ("cartao_id","id_cartao","cartao_credito_id","id_cartao_credito") if c in cols), None)
    if id_colm is None:
        name_colm = next((cols[c] for c in ("cartao","cartão","cartao_nome","nome_cartao","credor") if c in cols), None)
        if name_colm is None or base_cards.empty:
            return pd.DataFrame(columns=["card_id","pago_mes"])
        df["_key_nome_norm"] = df[name_colm].astype(str).str.strip().str.lower()
        df = df.merge(base_cards[["_key_nome_norm","card_id"]], on="_key_nome_norm", how="left")
        id_colm = "card_id"
    grp = df[is_mes].groupby(df[id_colm].astype(str), dropna=False, sort=True)["_pago"].sum()
    return grp.reset_index().rename(columns={id_colm: "card_id", "_pago": "pago_mes"})

def _cap_month_paid_by_boleto(db: DB, ref_year: int, ref_month: int) -> pd.DataFrame:
    cap = _load_contas_apagar_mov(db)
    if cap.empty:
        return pd.DataFrame(columns=["fonte","pago_mes"])
    df = cap.copy()
    df["_dt"] = _best_due_series(df)

    val_col = _pick_amount_col(df)
    df["_valor"] = pd.to_numeric(df[val_col], errors="coerce").fillna(0.0) if val_col else 0.0

    paid_col = _pick_paid_col(df)
    if paid_col:
        df["_pago"] = pd.to_numeric(df[paid_col], errors="coerce").fillna(0.0)
    else:
        mask = _normalize_paid_mask(df)
        df["_pago"] = df["_valor"].where(mask, 0.0)

    is_mes = df["_dt"].dt.month.eq(ref_month) & df["_dt"].dt.year.eq(ref_year)
    m_bol = _boletos_flag_mask(df)
    cols = {c.lower(): c for c in df.columns}
    fonte_col = next((cols[c] for c in ("credor","fornecedor","descricao","descrição","titulo","título") if c in cols), None)
    fonte = (df[fonte_col].astype(str).str.strip() if fonte_col else pd.Series(["Boleto"]*len(df), index=df.index)).replace({"": "Boleto"})
    grp = pd.DataFrame({"fonte": fonte, "_pago": df["_pago"]})[is_mes & m_bol].groupby("fonte", dropna=False, sort=True)["_pago"].sum()
    return grp.reset_index().rename(columns={"_pago": "pago_mes"})

# ===================== Render =====================
def render(db_path_pref: Optional[str] = None):

    # CSS
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

      .cap-inner { background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.12);
                   border-radius: 14px; padding: 12px; }
      .cap-inner + .cap-inner { margin-top: 12px; }

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
      .cap-blue   { color: #60a5fa !important; }
      .cap-pink   { color: #f472b6 !important; }
      .cap-lime  { color: #a3e635 !important; }
      .cap-teal  { color: #2dd4bf !important; }
      .cap-indigo{ color: #818cf8 !important; }

      .cap-chips-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:10px; }
      .cap-chip { display:flex; align-items:center; justify-content:space-between; gap:8px; padding:8px 10px;
                  border:1px solid rgba(255,255,255,0.10); border-radius:12px;
                  background: rgba(255,255,255,0.04); }
      .cap-chip-left { display:flex; align-items:center; gap:8px; min-width: 0; }
      .cap-dot { width:12px; height:12px; border-radius:50%; border:1px solid rgba(255,255,255,0.35); flex:0 0 auto; }
      .cap-dot.ok { background:#10b981; }
      .cap-dot.parcial { background:#f59e0b; }
      .cap-dot.nada { background:#6b7280; }

      .cap-badge { font-size:.80rem; padding:2px 8px; border-radius:9999px;
                   background: rgba(255,255,255,0.08); border:1px solid rgba(255,255,255,0.12); }
      .cap-badges { display:flex; gap:6px; flex-wrap:wrap; }

      .cap-legend { display:flex; gap:14px; font-size:.85rem; opacity:.85; margin-bottom:8px; }
      .cap-legend span { display:flex; align-items:center; gap:6px; }

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

    df_cards_view = _cards_view(db, ref_year, ref_month)
    cards_sums = _cards_totals(df_cards_view)

    subcats = _load_subcats_fixas(db)
    saidas_all = _load_saidas_all(db)
    painel = _build_fixed_panel_status(subcats, saidas_all, ref_year, ref_month)
    total_fixas_mes = float(pd.to_numeric(painel["valor_mes"], errors="coerce").fillna(0).sum()) if not painel.empty else 0.0

    parcelas_mes_emprestimos_cap = _loans_month_total_from_cap(db, ref_year, ref_month)

    # ===== CARD GERAL =====
    total_saldo = loans_sums["saldo_total"] + cards_sums["aberto_total"]
    total_parcelas_mes = parcelas_mes_emprestimos_cap + cards_sums["faturas_mes_total"]
    total_mes_geral = total_parcelas_mes + total_fixas_mes

    novo_top_geral = dedent(f"""
    <div class="cap-card cap-card-lg">
      <div class="cap-title-xl cap-red">Total Geral — {_month_year_label(ref_year, ref_month)}</div>
      <div class="cap-metrics-row cap-3col">
        <div class="cap-metric"><div class="cap-label">Saldo devedor: Cartões + Empréstimos + Boletos</div><div class="cap-value">{_fmt_brl(total_saldo)}</div></div>
        <div class="cap-metric"><div class="cap-label">Parcelas do mês: Cartões + Empréstimos + Boletos</div><div class="cap-value">{_fmt_brl(total_parcelas_mes)}</div></div>
        <div class="cap-metric"><div class="cap-label">Gastos fixos (mês)</div><div class="cap-value">{_fmt_brl(total_fixas_mes)}</div></div>
      </div>
      <div class="cap-metrics-row cap-1col" style="margin-top:10px;">
        <div class="cap-metric cap-metric-accent cap-center">
          <div class="cap-label cap-green">Total do mês: Cartões + Empréstimos + Boletos + Gastos Fixos</div>
          <div class="cap-value cap-green">{_fmt_brl(total_mes_geral)}</div>
        </div>
      </div>
    </div>
    """).strip()
    st.markdown(novo_top_geral, unsafe_allow_html=True)

    st.divider()

    # ====== Dados para os 3 cards/chips (USANDO valor_pago) =====
    paid_loans = _cap_month_paid_by_loan(db, ref_year, ref_month)
    loans_map = df_loans.rename(columns={"id":"loan_key","descricao":"titulo","Valor da Parcela Mensal":"mensal"})[["loan_key","titulo","mensal"]]
    loans_card_df = loans_map.merge(paid_loans, on="loan_key", how="left").fillna({"pago_mes":0.0})
    if not loans_card_df.empty:
        loans_card_df["falta"] = (loans_card_df["mensal"] - loans_card_df["pago_mes"]).clip(lower=0.0)
        loans_card_df["status"] = [
            _calc_status_from_paid(m, p) for m, p in zip(loans_card_df["mensal"], loans_card_df["pago_mes"])
        ]

    paid_cards = _cap_month_paid_by_card(db, ref_year, ref_month, _load_cards_catalog(db))
    cards_map = df_cards_view.rename(columns={"card_id":"card_id","card_nome":"titulo","fatura_mes_total":"mensal"})[["card_id","titulo","mensal"]]
    cards_card_df = cards_map.merge(paid_cards, on="card_id", how="left").fillna({"pago_mes":0.0})
    if not cards_card_df.empty:
        cards_card_df["falta"] = (pd.to_numeric(cards_card_df["mensal"], errors="coerce").fillna(0.0) - cards_card_df["pago_mes"]).clip(lower=0.0)
        cards_card_df["status"] = [
            _calc_status_from_paid(m, p) for m, p in zip(cards_card_df["mensal"], cards_card_df["pago_mes"])
        ]

    df_boletos_view = _build_boletos_view(db, ref_year, ref_month)
    paid_bol = _cap_month_paid_by_boleto(db, ref_year, ref_month)
    bols_map = df_boletos_view.rename(columns={"descricao":"titulo","Valor da Parcela Mensal":"mensal"})[["titulo","mensal"]]
    bols_card_df = bols_map.merge(paid_bol.rename(columns={"fonte":"titulo"}), on="titulo", how="left").fillna({"pago_mes":0.0})
    if not bols_card_df.empty:
        bols_card_df["falta"] = (pd.to_numeric(bols_card_df["mensal"], errors="coerce").fillna(0.0) - bols_card_df["pago_mes"]).clip(lower=0.0)
        bols_card_df["status"] = [
            _calc_status_from_paid(m, p) for m, p in zip(bols_card_df["mensal"], bols_card_df["pago_mes"])
        ]

    # ===== NOVO CONTAINER PRINCIPAL + SUB-CONTAINERS =====
    def _rows_html(df: pd.DataFrame) -> str:
        if df.empty:
            return '<div class="cap-sub">Sem itens para o mês.</div>'
        rows = []
        for _, r in df.sort_values("titulo").iterrows():
            titulo = html.escape(str(r["titulo"])) if pd.notna(r["titulo"]) else "(sem título)"
            mensal = _fmt_brl(r.get("mensal", 0.0))
            pago   = _fmt_brl(r.get("pago_mes", 0.0))
            falta  = _fmt_brl(r.get("falta", 0.0))
            status = str(r.get("status","nada"))
            rows.append(
                f'<div class="cap-chip">'
                f'  <div class="cap-chip-left"><span class="cap-dot {status}"></span><span>{titulo}</span></div>'
                f'  <div class="cap-badges"><span class="cap-badge">Mensal {mensal}</span>'
                f'  <span class="cap-badge">Pago {pago}</span>'
                f'  <span class="cap-badge">Falta {falta}</span></div>'
                f'</div>'
            )
        return "".join(rows)

    sub1_html = dedent(f"""
    <div class="cap-inner">
      <div class="cap-h4 cap-teal">Status Empréstimos, Fatura do Cartão e Boletos</div>
      <div class="cap-metrics-row cap-3col" style="margin-top:6px;">
        <div class="cap-metric">
          <div class="cap-label cap-purple">Empréstimos</div>
          <div>{_rows_html(loans_card_df)}</div>
        </div>
        <div class="cap-metric">
          <div class="cap-label cap-blue">Fatura do Cartão</div>
          <div>{_rows_html(cards_card_df)}</div>
        </div>
        <div class="cap-metric">
          <div class="cap-label cap-pink">Boletos</div>
          <div>{_rows_html(bols_card_df)}</div>
        </div>
      </div>
    </div>
    """).strip()

    if painel.empty:
        sub2_inner = dedent("""
        <div class="cap-inner">
          <div class="cap-sub">Nenhuma subcategoria de contas fixas (categoria 4) encontrada.</div>
        </div>
        """).strip()
    else:
        chips_html = ''.join(
            f'<div class="cap-chip">'
            f'  <div class="cap-chip-left"><span class="cap-dot {"ok" if float(r.valor_mes) > 0 else "nada"}"></span><span>{html.escape(str(r.subcat_nome))}</span></div>'
            f'  <span class="cap-badge{" muted" if float(r.valor_mes) <= 0 else ""}">{_fmt_brl(r.valor_mes)}</span>'
            f'</div>'
            for r in painel.sort_values("subcat_nome").itertuples(index=False)
        )
        sub2_inner = dedent(f"""
        <div class="cap-inner">
          <div class="cap-h4 cap-indigo">Status Contas Fixas</div>
          <div class="cap-metrics-row cap-1col">
            <div class="cap-metric">
              <div class="cap-label">Total gasto fixo (mês)</div>
              <div class="cap-value">{_fmt_brl(total_fixas_mes)}</div>
            </div>
          </div>
          <div class="cap-chips-grid">{chips_html}</div>
        </div>
        """).strip()

    # Legenda GLOBAL
    legenda_global = dedent("""
    <div class="cap-legend" style="margin:6px 0 8px;">
      <span><span class="cap-dot ok"></span>Quitado</span>
      <span><span class="cap-dot parcial"></span>Parcial</span>
      <span><span class="cap-dot nada"></span>Sem pagamento</span>
    </div>
    """).strip()

    painel_principal = dedent(f"""
    <div class="cap-card cap-card-lg">
      <div class="cap-title-xl cap-lime">Painel Contas a Pagar</div>
      {legenda_global}
      {sub1_html}
      {sub2_inner}
    </div>
    """).strip()
    st.markdown(painel_principal, unsafe_allow_html=True)

    st.divider()

    # ===== Empréstimos — mantido =====
    if df_loans_raw.empty:
        st.info("Nenhum empréstimo encontrado (tabela esperada: `emprestimos_financiamentos`).")
    else:
        parts = ["""
        <div class="cap-card cap-card-lg">
          <div class="cap-title-xl cap-purple">Empréstimos</div>
        """]

        parts.append(dedent(f"""
          <div class="cap-inner">
            <div class="cap-metrics-row">
              <div class="cap-metric"><div class="cap-label">Saldo devedor de todos empréstimos</div><div class="cap-value">{_fmt_brl(loans_sums['saldo_total'])}</div></div>
              <div class="cap-metric"><div class="cap-label">Parcela somada (mês) — CAP</div><div class="cap-value">{_fmt_brl(parcelas_mes_emprestimos_cap)}</div></div>
            </div>
          </div>
        """).strip())

        if not df_loans.empty:
            items_html = ['<div class="cap-grid">']
            for _, r in df_loans.iterrows():
                emp_id = html.escape(str(r["id"]))
                desc_raw = str(r.get("descricao", "") or "")
                desc = html.escape(desc_raw)
                titulo = desc if desc_raw and desc_raw != "(sem descrição)" else f"Empréstimo {emp_id}"
                items_html.append(dedent(f"""
                <div class="cap-card">
                  <h4>{titulo}</h4>
                  <div class="cap-metrics-row">
                    <div class="cap-metric"><div class="cap-label">Saldo devedor</div><div class="cap-value">{_fmt_brl(r["Saldo Devedor do Empréstimo"])}</div></div>
                    <div class="cap-metric"><div class="cap-label">Parcela (catálogo)</div><div class="cap-value">{_fmt_brl(r["Valor da Parcela Mensal"])}</div></div>
                  </div>
                </div>
                """).strip())
            items_html.append("</div>")
            parts.append(f'<div class="cap-inner">{"".join(items_html)}</div>')

        parts.append("</div>")
        st.markdown("\n".join(parts), unsafe_allow_html=True)

    st.divider()

    # ===== Fatura — mantido =====
    if df_cards_view.empty:
        st.info("Sem cartões/faturas localizados (tabelas esperadas: `cartoes_credito`, `fatura_cartao_itens` e/ou `contas_a_pagar_mov`).")
    else:
        parts = ["""
        <div class="cap-card cap-card-lg">
          <div class="cap-title-xl cap-blue">Fatura Cartão de Crédito</div>
        """]

        parts.append(dedent(f"""
          <div class="cap-inner">
            <div class="cap-metrics-row">
              <div class="cap-metric"><div class="cap-label">Valor em aberto (todos os cartões)</div><div class="cap-value">{_fmt_brl(cards_sums['aberto_total'])}</div></div>
              <div class="cap-metric"><div class="cap-label">Faturas do mês (somadas)</div><div class="cap-value">{_fmt_brl(cards_sums['faturas_mes_total'])}</div></div>
            </div>
          </div>
        """).strip())

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
        parts.append(f'<div class="cap-inner">{"".join(cards_html)}</div>')

        parts.append("</div>")
        st.markdown("\n".join(parts), unsafe_allow_html=True)

    st.divider()

    # ===== Boletos — mantido =====
    if df_boletos_view.empty:
        st.info("Nenhum boleto localizado (fonte: CAP).")
    else:
        bols_sums = _boletos_totals_view(df_boletos_view)
        bols_mes_total = _boletos_month_total_from_cap(db, ref_year, ref_month)

        parts = ["""
        <div class="cap-card cap-card-lg">
          <div class="cap-title-xl cap-pink">Boletos</div>
        """]

        parts.append(dedent(f"""
          <div class="cap-inner">
            <div class="cap-metrics-row">
              <div class="cap-metric">
                <div class="cap-label">Saldo devedor de todos boletos</div>
                <div class="cap-value">{_fmt_brl(bols_sums['saldo_total'])}</div>
              </div>
              <div class="cap-metric">
                <div class="cap-label">Parcela somada (mês) — CAP</div>
                <div class="cap-value">{_fmt_brl(bols_mes_total)}</div>
              </div>
            </div>
          </div>
        """).strip())

        cards_bol = ['<div class="cap-grid">']
        for _, r in df_boletos_view.iterrows():
            bol_id = html.escape(str(r["id"]))
            desc_raw = str(r.get("Descricao", r.get("descricao", "")) or "")
            desc = html.escape(desc_raw)
            titulo = desc if desc_raw and desc_raw != "(sem descrição)" else f"Boleto {bol_id}"
            saldo_txt = _fmt_brl(r.get("Saldo Devedor do Boleto", 0.0))
            parc_txt  = _fmt_brl(r.get("Valor da Parcela Mensal", 0.0))
            cards_bol.append(dedent(f"""
            <div class="cap-card">
              <h4>{titulo}</h4>
              <div class="cap-metrics-row">
                <div class="cap-metric"><div class="cap-label">Saldo devedor</div><div class="cap-value">{saldo_txt}</div></div>
                <div class="cap-metric"><div class="cap-label">Parcela (catálogo)</div><div class="cap-value">{parc_txt}</div></div>
              </div>
            </div>
            """).strip())
        cards_bol.append("</div>")
        parts.append(f'<div class="cap-inner">{"".join(cards_bol)}</div>')

        parts.append("</div>")
        st.markdown("\n".join(parts), unsafe_allow_html=True)


if __name__ == "__main__":
    st.set_page_config(page_title="Contas a Pagar", layout="wide")
    render()
