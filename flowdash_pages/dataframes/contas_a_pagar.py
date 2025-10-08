# -*- coding: utf-8 -*-
# flowdash_pages/contas_a_pagar/contas_a_pagar.py
from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from typing import Optional, Any, Dict, List

import pandas as pd
import streamlit as st

# ===================== Descoberta de DB (segura) =====================
def _ensure_db_path_or_raise(pref: Optional[str] = None) -> str:
    """
    Resolve o caminho do banco de forma resiliente, alinhado ao padrão do projeto.
    Tenta: session_state -> shared.db -> caminhos conhecidos.
    """
    # 1) Session
    if pref and os.path.exists(pref):
        return pref
    for k in ("caminho_banco", "db_path"):
        v = st.session_state.get(k)
        if isinstance(v, str) and os.path.exists(v):
            return v

    # 2) shared.db (opcional)
    try:
        from shared.db import get_db_path as _shared_get_db_path  # type: ignore
        p = _shared_get_db_path()
        if isinstance(p, str) and os.path.exists(p):
            return p
    except Exception:
        pass

    # 3) Candidatos conhecidos do repositório
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
def _subtle_help(msg: str):
    st.caption(f"ℹ️ {msg}")


def _fmt_brl(v: Any) -> str:
    try:
        return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v)


def _section_header(t: str):
    st.markdown(f"### {t}")


# ===================== Queries (placeholders seguros) =====================
def _load_loans_raw(db: DB) -> pd.DataFrame:
    """
    Carrega a tabela de empréstimos, sem suposições fortes de schema.
    Nome canônico esperado (já usado no projeto): `emprestimos_financiamentos`.
    """
    if not _table_exists(db, "emprestimos_financiamentos"):
        return pd.DataFrame()
    return db.q("SELECT * FROM emprestimos_financiamentos")


def _load_cards_open_raw(db: DB) -> pd.DataFrame:
    """
    Carrega visão simplificada de faturas/cartões. Ajustaremos no Passo 2.
    Aqui só tenta encontrar tabelas usuais, sem travar a página.
    Tentativas comuns:
      - fatura_cartao / fatura_cartao_itens
      - cartoes_credito
    """
    # Preferimos itens (mais granular). Se não existir, retornamos vazio.
    if _table_exists(db, "fatura_cartao_itens"):
        return db.q("SELECT * FROM fatura_cartao_itens")
    elif _table_exists(db, "fatura_cartao"):
        return db.q("SELECT * FROM fatura_cartao")
    elif _table_exists(db, "cartoes_credito"):
        return db.q("SELECT * FROM cartoes_credito")
    return pd.DataFrame()


def _table_exists(db: DB, name: str) -> bool:
    sql = "SELECT name FROM sqlite_master WHERE type='table' AND lower(name)=lower(?)"
    try:
        with db.conn() as cx:
            r = cx.execute(sql, (name,)).fetchone()
            return r is not None
    except Exception:
        return False


# ===================== Cálculos (versões mínimas, não definitivas) =====================
def _calc_loans_view(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria uma visão por empréstimo: Nome/Identificador | Valor Total | Saldo Devedor | Parcela Mensal.
    Nesta versão (Passo 1), tentamos campos comuns e fazemos fallback defensivo.
    Campos tentados (se existirem):
      - identificador: nome, titulo, descricao, credor, id
      - valor_total: valor_total, principal, valor
      - parcela_mensal: parcela_valor, valor_parcela, parcela, parcela_atual
      - parcelas_pag: parcelas_pagas, parcelas_pag, qtd_parcelas_pagas
      - parcelas: parcelas, qtd_parcelas
      - saldo_devedor: saldo_devedor
    """
    if df.empty:
        return df

    # Escolher coluna de identificação
    ident_cols = ["nome", "titulo", "descricao", "credor", "Identificador", "id"]
    ident = _first_existing(df, ident_cols) or df.columns[0]

    # Valor total
    total_cols = ["valor_total", "principal", "valor", "Valor_Total"]
    vtot = _first_existing(df, total_cols)

    # Valor da parcela
    parcela_cols = ["parcela_valor", "valor_parcela", "parcela", "Parcela_Valor"]
    vparc = _first_existing(df, parcela_cols)

    # Parcelas
    parc_qtd_cols = ["parcelas", "qtd_parcelas", "Parcelas"]
    parc_pag_cols = ["parcelas_pagas", "parcelas_pag", "qtd_parcelas_pagas", "Parcelas_Pagas"]

    # Saldo devedor (se existir, usamos; senão tentamos aproximar)
    sdev_cols = ["saldo_devedor", "Saldo_Devedor"]
    sdev = _first_existing(df, sdev_cols)

    out = pd.DataFrame()
    out["Empréstimo"] = df[ident].astype(str)

    if vtot:
        out["Valor Total do Empréstimo"] = df[vtot]
    else:
        out["Valor Total do Empréstimo"] = None

    if vparc:
        out["Valor da Parcela Mensal"] = df[vparc]
    else:
        out["Valor da Parcela Mensal"] = None

    # Saldo devedor: preferir coluna nativa; fallback = total - (pagas * parcela)
    if sdev:
        out["Saldo Devedor do Empréstimo"] = df[sdev]
    else:
        try:
            pagas = df[_first_existing(df, parc_pag_cols)] if _first_existing(df, parc_pag_cols) else 0
            tot   = df[vtot] if vtot else 0
            parc  = df[vparc] if vparc else 0
            out["Saldo Devedor do Empréstimo"] = (tot - (pagas * parc)).clip(lower=0)
        except Exception:
            out["Saldo Devedor do Empréstimo"] = None

    # Formatação amigável (deixamos numéricas cruas para agregações no Passo 2; aqui apenas exibimos)
    return out


def _calc_loans_summary(df_view: pd.DataFrame) -> Dict[str, float]:
    if df_view.empty:
        return {"total_emprestimos": 0.0, "saldo_devedor": 0.0, "parcelas_mes": 0.0}
    return {
        "total_emprestimos": float(pd.to_numeric(df_view.get("Valor Total do Empréstimo"), errors="coerce").fillna(0).sum()),
        "saldo_devedor": float(pd.to_numeric(df_view.get("Saldo Devedor do Empréstimo"), errors="coerce").fillna(0).sum()),
        "parcelas_mes": float(pd.to_numeric(df_view.get("Valor da Parcela Mensal"), errors="coerce").fillna(0).sum()),
    }


def _calc_cards_view(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Passo 1: apenas prepara colunas destino e deixa placeholders.
    No Passo 2 conectamos com o schema real (fatura em aberto, valor do mês etc).
    """
    if df_raw.empty:
        return df_raw

    # Tentativa de identificar um "cartão"
    ident = _first_existing(df_raw, ["Cartao", "cartao", "Cartao_Nome", "bandeira", "descricao", "nome", "id"]) or df_raw.columns[0]
    out = pd.DataFrame()
    out["Cartão"] = df_raw[ident].astype(str)

    # Placeholders (serão substituídos no Passo 2 por cálculos reais)
    out["Valor total em aberto"] = None
    out["Valor mensal da fatura"] = None
    return out


def _calc_cards_summary(df_view: pd.DataFrame) -> Dict[str, float]:
    if df_view.empty:
        return {"aberto_total": 0.0, "faturas_mes": 0.0}
    return {
        "aberto_total": float(pd.to_numeric(df_view.get("Valor total em aberto"), errors="coerce").fillna(0).sum()),
        "faturas_mes": float(pd.to_numeric(df_view.get("Valor mensal da fatura"), errors="coerce").fillna(0).sum()),
    }


def _first_existing(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
        # procurar case-insensitive
        matches = [col for col in df.columns if col.lower() == c.lower()]
        if matches:
            return matches[0]
    return None


# ===================== Render =====================
def render(db_path_pref: Optional[str] = None):
    st.header("Contas a Pagar")

    # DB
    try:
        db = DB(_ensure_db_path_or_raise(db_path_pref))
    except Exception as e:
        st.error(str(e))
        return

    # ---------------- Empréstimos (lista) ----------------
    _section_header("Empréstimos")
    df_loans_raw = _load_loans_raw(db)
    if df_loans_raw.empty:
        _subtle_help("Não encontrei a tabela de empréstimos ainda (esperado: `emprestimos_financiamentos`).")
    df_loans_view = _calc_loans_view(df_loans_raw)
    if not df_loans_view.empty:
        st.dataframe(df_loans_view, use_container_width=True)
    else:
        st.info("Sem dados de empréstimos para exibir por enquanto.")

    # ---------------- Resumo dos Empréstimos ----------------
    _section_header("Resumo dos Empréstimos")
    sums_loans = _calc_loans_summary(df_loans_view)
    c1, c2, c3 = st.columns(3)
    c1.metric("Valor total de todos os empréstimos", _fmt_brl(sums_loans["total_emprestimos"]))
    c2.metric("Saldo devedor de todos os empréstimos", _fmt_brl(sums_loans["saldo_devedor"]))
    c3.metric("Parcela somada (mês)", _fmt_brl(sums_loans["parcelas_mes"]))
    _subtle_help("No Passo 2 conectaremos os cálculos exatamente ao seu schema (total, saldo devedor e parcela).")

    # ---------------- Fatura Cartão de Crédito (lista) ----------------
    _section_header("Fatura Cartão de Crédito")
    df_cards_raw = _load_cards_open_raw(db)
    if df_cards_raw.empty:
        _subtle_help("Não encontrei dados de fatura/cartões (tabelas usuais: `fatura_cartao_itens`, `fatura_cartao`, `cartoes_credito`).")
    df_cards_view = _calc_cards_view(df_cards_raw)
    if not df_cards_view.empty:
        st.dataframe(df_cards_view, use_container_width=True)
    else:
        st.info("Sem dados de cartão para exibir por enquanto.")

    # ---------------- Resumo Cartão ----------------
    _section_header("Resumo Cartão")
    sums_cards = _calc_cards_summary(df_cards_view)
    c1, c2 = st.columns(2)
    c1.metric("Valor em aberto (todos cartões)", _fmt_brl(sums_cards["aberto_total"]))
    c2.metric("Faturas do mês (somadas)", _fmt_brl(sums_cards["faturas_mes"]))
    _subtle_help("No Passo 2 vamos buscar exatamente o 'em aberto' e o 'valor do mês' do seu banco.")

    # ---------------- Contas em aberto (por mês) ----------------
    _section_header("Contas em aberto por mês")
    st.caption("Soma de parcelas de empréstimos + faturas de cartão, agrupadas por mês.")
    st.info("Placeholders criados. No Passo 2 ligaremos esta grade ao seu cronograma de parcelas e às faturas por competência.")

    # Placeholder de tabela mensal (vazia por enquanto)
    meses = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]
    df_mes = pd.DataFrame({
        "Mês": meses,
        "Empréstimos": [None]*12,
        "Cartões": [None]*12,
        "Total": [None]*12,
    })
    st.dataframe(df_mes, use_container_width=True)
    _subtle_help("Ao conectar os cálculos no Passo 2, estes valores serão preenchidos corretamente.")


# Execução direta (opcional para testes locais)
if __name__ == "__main__":
    st.set_page_config(page_title="Contas a Pagar", layout="wide")
    render()
