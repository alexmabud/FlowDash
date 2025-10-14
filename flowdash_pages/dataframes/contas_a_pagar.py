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

def _pick_paid_acumulado_col(df: pd.DataFrame) -> Optional[str]:
    # inclui variações mais comuns e fallback para valor_pago_mes/valor_pago
    return _first_existing(df, ["valor_pago_acumulado", "valor_pago_acum", "pago_acumulado", "valor_pago_mes", "valor_pago"])

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

# --------- Normalização de STATUS (texto do CAP) ----------
def _norm_status_text(s: str) -> str:
    t = str(s).strip().lower()
    if "abert" in t or "pend" in t:
        return "nada"      # sem pagamento
    if "parc" in t:
        return "parcial"   # pagamento parcial
    if "quit" in t:
        return "ok"        # quitado
    return "nada"

def _agg_status(series: pd.Series) -> str:
    """Pior status vence: aberto/pendente > parcial > quitado."""
    vals = list(series.dropna().astype(str))
    if any(v == "nada" for v in vals):
        return "nada"
    if any(v == "parcial" for v in vals):
        return "parcial"
    if any(v == "ok" for v in vals):
        return "ok"
    return "nada"

# --------- Normalização de TIPO_OBRIGACAO ----------
def _norm_tipo_obrigacao(s: str) -> str:
    if not isinstance(s, str):
        return ""
    t = s.strip().lower()
    # remoção simples de acentos-chave (robustez)
    t = (t.replace("ã","a").replace("â","a").replace("á","a")
           .replace("é","e").replace("ê","e")
           .replace("í","i")
           .replace("ó","o").replace("ô","o")
           .replace("ú","u").replace("ç","c"))
    if ("fatura" in t) and ("cart" in t):
        return "fatura_cartao"
    if "boleto" in t:
        return "boleto"
    if "emprest" in t:
        return "emprestimo"
    return t

# ===================== FIXAS: painel (definido ANTES do uso) =====================
def _build_fixed_panel_status(
    subcats: pd.DataFrame,
    saidas_all: pd.DataFrame,
    ref_year: int,
    ref_month: int,
) -> pd.DataFrame:
    """
    Monta o painel de Contas Fixas (categoria 4), devolvendo:
    ['subcat_id', 'subcat_nome', 'valor_mes', 'status'].

    - Soma por subcategoria no mês/ano selecionado.
    - status: 'pago' se houve gasto (>0) no mês; senão 'pendente'.
    """
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

# ===================== Somas/Status por tipo_obrigacao a partir do CAP =====================
def _cap_month_summary_by_tipo(db: DB, ref_year: int, ref_month: int, tipo_key: str) -> pd.DataFrame:
    """
    Lê CAP e retorna por 'credor' (titulo) PARA UM TIPO (tipo_obrigacao/categoria_evento):
      - mensal = soma(valor_evento) do mês
      - pago_mes = soma(valor_pago_acumulado) do mês
      - falta = mensal - pago_mes (>=0)
      - status = mapeado da coluna 'status' do CAP (prioridade pior-vence)
    Filtra por tipo (normalizado).
    """
    cap = _load_contas_apagar_mov(db)
    if cap.empty:
        return pd.DataFrame(columns=["titulo","mensal","pago_mes","falta","status"])

    df = cap.copy()
    df["_dt"] = _best_due_series(df)

    # Colunas
    val_col = "valor_evento" if "valor_evento" in df.columns else _pick_amount_col(df)
    pago_col = _pick_paid_acumulado_col(df)
    status_col = _first_existing(df, ["status","situacao","situação"])
    credor_col = _first_existing(df, ["credor","fornecedor","descricao","descrição","titulo","título"])

    # >>> PRIORIDADE CERTA: primeiro tipo_obrigacao, depois categoria_evento, etc. <<<
    tipo_col = _first_existing(df, ["tipo_obrigacao", "categoria_evento", "tipo", "origem", "classe", "grupo"])

    if val_col is None or credor_col is None or tipo_col is None:
        return pd.DataFrame(columns=["titulo","mensal","pago_mes","falta","status"])

    df["_tipo_norm"] = df[tipo_col].astype(str).map(_norm_tipo_obrigacao)
    alvo = _norm_tipo_obrigacao(tipo_key)

    # Filtro por MÊS/ANO + tipo
    is_mes = df["_dt"].dt.month.eq(ref_month) & df["_dt"].dt.year.eq(ref_year)
    df = df[is_mes & (df["_tipo_norm"] == alvo)].copy()
    if df.empty:
        return pd.DataFrame(columns=["titulo","mensal","pago_mes","falta","status"])

    # Números
    df["_mensal"] = pd.to_numeric(df[val_col],  errors="coerce").fillna(0.0) if val_col else 0.0
    df["_pago"]   = pd.to_numeric(df[pago_col], errors="coerce").fillna(0.0) if pago_col else 0.0

    # Título
    df["_titulo"] = df[credor_col].astype(str).str.strip().replace({"": "(sem nome)"})

    # Status por linha
    if status_col:
        df["_status_norm"] = df[status_col].astype(str).map(_norm_status_text)
    else:
        df["_status_norm"] = "nada"

    grp = df.groupby("_titulo", dropna=False, sort=True)
    out = pd.DataFrame({
        "titulo": grp.size().index.astype(str),
        "mensal": grp["_mensal"].sum().astype(float),
        "pago_mes": grp["_pago"].sum().astype(float),
        "status": grp["_status_norm"].apply(_agg_status).values
    })
    out["falta"] = (out["mensal"] - out["pago_mes"]).clip(lower=0.0)
    return out.reset_index(drop=True)

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
    val_col = "valor_evento" if "valor_evento" in df.columns else _pick_amount_col(df)
    df["_valor"] = pd.to_numeric(df[val_col], errors="coerce").fillna(0.0) if val_col else 0.0

    cols = {c.lower(): c for c in df.columns}
    loan_id_col = next((cols[c] for c in ("emprestimo_id","id_emprestimo","loan_id") if c in cols), None)

    if loan_id_col:
        m_loan = df[loan_id_col].notna() & (df[loan_id_col].astype(str).str.strip() != "")
    else:
        hint_cols = [c for c in ("tipo_obrigacao","categoria_evento","categoria","origem","tipo","fonte","classe","grupo") if c in cols]
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


# ===================== Cartões (cards grandes) =====================
def _normalize_paid_mask(df: pd.DataFrame) -> pd.Series:
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
    """Prioriza tipo_obrigacao == BOLETO; mantém heurística textual como fallback."""
    if df.empty:
        return pd.Series(False, index=df.index)
    cols = {c.lower(): c for c in df.columns}
    m = pd.Series(False, index=df.index)

    tipo_col = cols.get("tipo_obrigacao")
    if tipo_col:
        m = df[tipo_col].astype(str).map(_norm_tipo_obrigacao).eq("boleto")

    if m.any():
        return m.fillna(False)
    text_fields = ["tipo_obrigacao","tipo_origem","forma_pagamento","categoria_evento","categoria","origem","tipo","fonte","classe","grupo","descricao","descrição","titulo","título","credor","fornecedor"]
    m_text = pd.Series(False, index=df.index)
    for key in text_fields:
        c = cols.get(key)
        if not c: continue
        s = df[c].astype(str).str.lower()
        m_text = m_text | s.str.contains("boleto")
    return m_text.fillna(False)

def _boletos_month_total_from_cap(db: DB, ref_year: int, ref_month: int) -> float:
    cap = _load_contas_apagar_mov(db)
    if cap.empty:
        return 0.0
    df = cap.copy()
    df["_dt"] = _best_due_series(df)
    is_mes = df["_dt"].dt.month.eq(ref_month) & df["_dt"].dt.year.eq(ref_year)
    val_col = "valor_evento" if "valor_evento" in df.columns else _pick_amount_col(df)
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

# ===================== Chips (por tipo_obrigacao + credor) =====================
def _chips_df_emprestimos(db: DB, ref_year: int, ref_month: int) -> pd.DataFrame:
    return _cap_month_summary_by_tipo(db, ref_year, ref_month, "EMPRESTIMO")

def _chips_df_cartoes(db: DB, ref_year: int, ref_month: int) -> pd.DataFrame:
    return _cap_month_summary_by_tipo(db, ref_year, ref_month, "FATURA_CARTAO")

def _chips_df_boletos(db: DB, ref_year: int, ref_month: int) -> pd.DataFrame:
    return _cap_month_summary_by_tipo(db, ref_year, ref_month, "BOLETO")


# ===================== Render =====================
def render(db_path_pref: Optional[str] = None):

    # CSS
    st.markdown("""
    <style>
    /* --- cartões e estilos base --- */
    .cap-card{border:1px solid rgba(255,255,255,0.10);border-radius:16px;padding:14px 16px;background:rgba(255,255,255,0.03);box-shadow:0 1px 4px rgba(0,0,0,0.10);}
    .cap-card-lg{padding:18px 20px;border-width:1.5px;}
    .cap-title-xl{font-size:1.25rem;font-weight:700;margin-bottom:10px;}
    .cap-metric{background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:10px 12px;}
    .cap-metric-accent{background:rgba(34,197,94,0.12);border-color:rgba(34,197,94,0.35);} /* legado (verde) */
    /* NOVAS variações de cor para métricas */
    .cap-metric-green{background:rgba(34,197,94,0.12);border-color:rgba(34,197,94,0.35);}
    .cap-metric-red{background:rgba(239,68,68,0.12);border-color:rgba(239,68,68,0.35);}
    .cap-metric-blue{background:rgba(59,130,246,0.12);border-color:rgba(59,130,246,0.35);}

    .cap-label{font-size:.85rem;opacity:.85;margin-bottom:4px;}
    .cap-value{font-size:1.35rem;font-weight:700;}
    .cap-inner{background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.12);border-radius:14px;padding:12px;}
    .cap-inner + .cap-inner{margin-top:12px;}
    .cap-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px;}
    .cap-h4{font-size:1.05rem;font-weight:700;margin:2px 0 10px;opacity:.95;}
    .cap-red{color:#ef4444!important}.cap-purple{color:#a78bfa!important}.cap-blue{color:#60a5fa!important}.cap-pink{color:#f472b6!important}.cap-teal{color:#2dd4bf!important}.cap-indigo{color:#818cf8!important}.cap-lime{color:#a3e635!important}
    .cap-cyan{color:#22d3ee!important}

    /* ===== Linhas de KPIs ===== */
    .cap-metrics-row{display:grid;gap:14px;}
    .cap-metrics-row.cap-3col{grid-template-columns:repeat(3,minmax(260px,1fr));align-items:stretch;}
    .cap-metrics-row.cap-2col{grid-template-columns:repeat(2,minmax(260px,1fr));align-items:stretch;}
    .cap-metrics-row.cap-1col{grid-template-columns:1fr;}
    @media (max-width:1100px){
      .cap-metrics-row.cap-3col{grid-template-columns:repeat(2,minmax(260px,1fr));}
      .cap-metrics-row.cap-2col{grid-template-columns:1fr;}
    }
    @media (max-width:900px){
      .cap-metrics-row.cap-3col{grid-template-columns:1fr;}
    }

    /* ===== Painéis “Empréstimos / Fatura / Boletos” ===== */
    .cap-section-grid{
      display:grid;
      grid-template-columns:repeat(3,minmax(260px,1fr));
      gap:12px;
      align-items:start;
    }
    @media (max-width:1100px){ .cap-section-grid{grid-template-columns:repeat(2,minmax(260px,1fr));} }
    @media (max-width:900px){  .cap-section-grid{grid-template-columns:1fr;} }

    /* container interno sem forçar altura */
    
    .cap-eq{
    display:flex;
    flex-direction:column;        /* garante topo alinhado e conteúdo empilhado */
    }
    .cap-eq .cap-h4{
    margin:0 0 8px !important;    /* zera margem superior para todos */
    line-height:1.2;
    }
    /* força todos os itens da grade a começarem no mesmo topo */
    .cap-section-grid > *{
    align-self:stretch;
    margin-top:0 !important;
    }


    /* chips grid responsivo */
    .cap-chips-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px;}
    @media (max-width:560px){ .cap-chips-grid{grid-template-columns:1fr;} }

    .cap-panel{display:flex;flex-direction:column;gap:8px;min-width:0;}
    .cap-panel-title{
      display:block;font-size:.95rem;font-weight:600;opacity:.9;
      margin-bottom:6px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
    }

    /* ===== Chip ===== */
    .cap-chip{
      display:grid;grid-template-columns:minmax(0,1fr) auto;
      align-items:center;gap:8px;padding:8px 10px;
      border:1px solid rgba(255,255,255,0.10);border-radius:12px;
      background:rgba(255,255,255,0.04);overflow:hidden;
    }
    @media (max-width:1200px){ .cap-chip{grid-template-columns:1fr;align-items:flex-start;} }

    .cap-chip-left{min-width:0;display:flex;align-items:center;gap:8px;overflow:hidden;}
    .cap-chip-title{
      min-width:0;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;
      overflow:hidden;word-break:break-word;overflow-wrap:anywhere;line-height:1.2;max-height:2.4em;
    }

    .cap-badges{display:flex;gap:6px;flex-wrap:wrap;white-space:normal;justify-content:flex-end;}
    .cap-badge{font-size:.80rem;padding:2px 8px;border-radius:9999px;background:rgba(255,255,255,0.08);border:1px solid rgba(255,255,255,0.12);text-align:center;}

    /* ===== Modo EMPILHADO ===== */
    .cap-chip.cap-chip-stack{
      grid-template-columns:1fr !important;
      grid-template-rows:auto auto;
      row-gap:6px; align-items:flex-start;
    }
    .cap-chip-stack .cap-chip-head{display:flex; align-items:center; gap:8px; min-width:0;}
    .cap-chip-stack .cap-chip-title{white-space:nowrap; overflow:hidden; text-overflow:ellipsis;}
    .cap-chip-stack .cap-badges{justify-content:flex-start; flex-wrap:wrap; white-space:normal;}

    /* Pontos de status */
    .cap-dot{width:12px;height:12px;border-radius:50%;border:1px solid rgba(255,255,255,0.35);flex:0 0 auto;}
    .cap-dot.ok{background:#10b981;}
    .cap-dot.parcial{background:#f59e0b;}
    .cap-dot.nada{background:#6b7280;}

    /* Legenda */
    .cap-legend{display:flex;gap:14px;font-size:.85rem;opacity:.85;margin-bottom:8px;}
    .cap-legend span{display:flex;align-items:center;gap:6px;}

    /* Centro (para o KPI grande) */
    .cap-center{ text-align:center; display:flex; flex-direction:column; align-items:center; justify-content:center; }

    /* utilitárias agrupadas */
    .cap-spacer{height:14px}
    .cap-panel-title-plain{font-size:.95rem;font-weight:600;opacity:.9;margin:6px 0 8px;}
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
        st.error(str(e)); 
        return

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

    # ===== TOTAIS de chips CAP (para KPIs do topo) =====
    chips_emp_top  = _chips_df_emprestimos(db, ref_year, ref_month)
    chips_cart_top = _chips_df_cartoes(db, ref_year, ref_month)
    chips_bol_top  = _chips_df_boletos(db, ref_year, ref_month)

    def _sum_col(dfs, col):
        tot = 0.0
        for d in dfs:
            if d is not None and not d.empty and col in d.columns:
                tot += pd.to_numeric(d[col], errors="coerce").fillna(0.0).sum()
        return float(tot)

    pago_mes_total  = _sum_col((chips_emp_top, chips_cart_top, chips_bol_top), "pago_mes")
    falta_mes_total = _sum_col((chips_emp_top, chips_cart_top, chips_bol_top), "falta")

    # ===== CARD GERAL =====
    total_saldo = loans_sums["saldo_total"] + cards_sums["aberto_total"]
    total_parcelas_mes = parcelas_mes_emprestimos_cap + cards_sums["faturas_mes_total"]
    total_mes_geral = total_parcelas_mes + total_fixas_mes

    novo_top_geral = dedent(f"""
    <div class="cap-card cap-card-lg">
      <div class="cap-title-xl cap-red">Resumo — {_month_year_label(ref_year, ref_month)}</div>

      <!-- Linha 1: 3 KPIs -->
      <div class="cap-metrics-row cap-3col">
        <div class="cap-metric">
          <div class="cap-label">Saldo devedor: Cartões + Empréstimos + Boletos</div>
          <div class="cap-value">{_fmt_brl(total_saldo)}</div>
        </div>
        <div class="cap-metric">
          <div class="cap-label">Parcelas do mês: Cartões + Empréstimos + Boletos</div>
          <div class="cap-value">{_fmt_brl(total_parcelas_mes)}</div>
        </div>
        <div class="cap-metric">
          <div class="cap-label">Gastos fixos (mês)</div>
          <div class="cap-value">{_fmt_brl(total_fixas_mes)}</div>
        </div>
      </div>

      <!-- Linha 2: 2 KPIs ocupando a largura inteira -->
      <div class="cap-metrics-row cap-2col" style="margin-top:10px;">
        <div class="cap-metric cap-metric-green">
          <div class="cap-label cap-green">Pagamentos efetuados no mês: Empréstimos/Faturas/Boletos</div>
          <div class="cap-value cap-green">{_fmt_brl(pago_mes_total)}</div>
        </div>
        <div class="cap-metric cap-metric-red">
          <div class="cap-label cap-red">Saldo a pagar: Empréstimos/Faturas/Boletos</div>
          <div class="cap-value cap-red">{_fmt_brl(falta_mes_total)}</div>
        </div>
      </div>

      <!-- Linha 3: KPI único azul -->
      <div class="cap-metrics-row cap-1col" style="margin-top:10px;">
        <div class="cap-metric cap-metric-blue cap-center">
          <div class="cap-label cap-blue">Total do mês: Cartões + Empréstimos + Boletos + Gastos Fixos</div>
          <div class="cap-value cap-blue">{_fmt_brl(total_mes_geral)}</div>
        </div>
      </div>
    </div>
    """).strip()
    st.markdown(novo_top_geral, unsafe_allow_html=True)

    st.divider()

    # ===== CHIPS (lendo CAP por tipo + credor) =====
    loans_card_df  = _chips_df_emprestimos(db, ref_year, ref_month)
    cards_card_df  = _chips_df_cartoes(db, ref_year, ref_month)
    bols_card_df   = _chips_df_boletos(db, ref_year, ref_month)

    # chips (sem placeholder — reduz altura quando vazio)
    def _chips_rows(df: pd.DataFrame) -> str:
        if df is None or df.empty:
            return '<div class="cap-sub">Sem itens para o mês.</div>'
        rows = []
        df2 = df.copy()
        for c in ("titulo","mensal","pago_mes","falta","status"):
            if c not in df2.columns:
                df2[c] = 0 if c not in ("titulo","status") else ("(sem nome)" if c=="titulo" else "nada")
        for _, r in df2.sort_values("titulo").iterrows():
            titulo = html.escape(str(r["titulo"])) if pd.notna(r["titulo"]) else "(sem nome)"
            mensal = _fmt_brl(r.get("mensal", 0.0))
            pago   = _fmt_brl(r.get("pago_mes", 0.0))
            falta  = _fmt_brl(r.get("falta", 0.0))
            status = str(r.get("status","nada"))
            rows.append(
                f'<div class="cap-chip cap-chip-stack">'
                f'  <div class="cap-chip-head">'
                f'    <span class="cap-dot {status}"></span>'
                f'    <span class="cap-chip-title">{titulo}</span>'
                f'  </div>'
                f'  <div class="cap-badges">'
                f'    <span class="cap-badge">Mensal {mensal}</span>'
                f'    <span class="cap-badge">Pago {pago}</span>'
                f'    <span class="cap-badge">Falta {falta}</span>'
                f'  </div>'
                f'</div>'
            )
        return "".join(rows)

    # seção estilo Contas Fixas, sem "Total do mês (mensal)"
    def _secao_like_fixas(titulo: str, color_cls: str, df: pd.DataFrame) -> str:
        chips_html = _chips_rows(df)
        return f"""
        <div class="cap-inner cap-eq">
          <div class="cap-h4 {color_cls}">{html.escape(titulo)}</div>
          <div class="cap-chips-grid">{chips_html}</div>
        </div>
        """.strip()

    # três seções (altura natural)
    secao_emprestimos = _secao_like_fixas("Empréstimos", "cap-purple", loans_card_df)
    secao_fatura      = _secao_like_fixas("Fatura do Cartão", "cap-blue", cards_card_df)
    secao_boletos     = _secao_like_fixas("Boletos", "cap-pink", bols_card_df)

    # ===== Status Contas Fixas =====
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
          <div class="cap-h4 cap-cyan">Status Contas Fixas</div>
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

    # === Container agrupado: Status Empréstimos, Fatura do Cartão e Boletos ===
    secao_status_ags = dedent(f"""
    <div class="cap-inner">
      <div class="cap-h4 cap-cyan">Status Empréstimos, Fatura do Cartão e Boletos</div>
      <div class="cap-section-grid">
        {secao_emprestimos}
        {secao_fatura}
        {secao_boletos}
      </div>
    </div>
    """).strip()

    # === Painel principal com container + espaçador antes do Status Contas Fixas ===
    painel_principal = dedent(f"""
    <div class="cap-card cap-card-lg">
      <div class="cap-title-xl cap-lime">Painel Contas a Pagar</div>
      {legenda_global}
      {secao_status_ags}
      <div class="cap-spacer"></div>
      {sub2_inner}
    </div>
    """).strip()
    st.markdown(painel_principal, unsafe_allow_html=True)

    st.divider()

    # ===== Seções grandes =====
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

    # ===== Boletos (cards grandes) =====
    df_boletos_view = _build_boletos_view(db, ref_year, ref_month)
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
