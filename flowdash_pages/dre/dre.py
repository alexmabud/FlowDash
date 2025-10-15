# -*- coding: utf-8 -*-
# flowdash_pages/dre/dre.py
from __future__ import annotations

import sqlite3
from calendar import monthrange
from dataclasses import dataclass
from typing import Dict, Tuple, List, Iterable, Optional

import pandas as pd
import streamlit as st


# ============================== Config de início do DRE ==============================
# Meses ANTES de START_YEAR/START_MONTH mostram apenas Faturamento
START_YEAR = 2025
START_MONTH = 10  # Outubro


# ============================== Helpers ==============================

def _conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn

def _periodo_ym(ano: int, mes: int) -> Tuple[str, str, str]:
    """Retorna (inicio_iso, fim_iso, competencia 'YYYY-MM')."""
    last = monthrange(ano, mes)[1]
    ini = f"{ano:04d}-{mes:02d}-01"
    fim = f"{ano:04d}-{mes:02d}-{last:02d}"
    comp = f"{ano:04d}-{mes:02d}"
    return ini, fim, comp

def _fmt_brl(v: float) -> str:
    s = f"{v:,.2f}"
    return "R$ " + s.replace(",", "X").replace(".", ",").replace("X", ".")

def _safe(v) -> float:
    try:
        return float(v or 0)
    except Exception:
        return 0.0

@dataclass
class VarsDRE:
    simples: float = 0.0    # %
    markup: float = 0.0     # coef
    sacolas: float = 0.0    # %
    fundo: float = 0.0      # %


# ============================== Schema helpers ==============================

@st.cache_data(show_spinner=False)
def _table_cols(db_path: str, table: str) -> List[str]:
    """Lista as colunas (minúsculas) de uma tabela; retorna [] se não existir."""
    try:
        with _conn(db_path) as c:
            rows = c.execute(f"PRAGMA table_info('{table}')").fetchall()
            return [str(r[1]).lower() for r in rows]
    except Exception:
        return []

def _find_col(cols_lower: Iterable[str], candidates: Iterable[str]) -> Optional[str]:
    """Retorna o primeiro nome de coluna realmente existente (respeita caixa original se estiver em candidates)."""
    lowset = {c.lower() for c in cols_lower}
    for cand in candidates:
        if cand.lower() in lowset:
            return cand
    return None


# ============================== Queries (cache) ==============================

@st.cache_data(show_spinner=False)
def _load_vars(db_path: str) -> VarsDRE:
    q = """
    SELECT chave, COALESCE(valor_num, 0) AS v
    FROM dre_variaveis
    WHERE chave IN (
        'aliquota_simples_nacional',
        'markup_medio',
        'sacolas_percent',
        'fundo_promocao_percent'
    )
    """
    try:
        with _conn(db_path) as c:
            df = pd.read_sql(q, c)
    except Exception:
        return VarsDRE(0, 0, 0, 0)

    d: Dict[str, float] = {r["chave"]: float(r["v"] or 0) for _, r in df.iterrows()}
    return VarsDRE(
        simples=_safe(d.get("aliquota_simples_nacional")),
        markup=_safe(d.get("markup_medio")),
        sacolas=_safe(d.get("sacolas_percent")),
        fundo=_safe(d.get("fundo_promocao_percent")),
    )

@st.cache_data(show_spinner=False)
def _query_entradas(db_path: str, ini: str, fim: str) -> Tuple[float, float]:
    """
    Retorna (faturamento_bruto, taxa_maquineta_rs)
    - Faturamento = SUM(Valor)
    - Taxa Maquineta (R$) = SUM(Valor - COALESCE(valor_liquido,0))
    """
    sql = """
    SELECT
      SUM(COALESCE(Valor,0)) AS fat,
      SUM(COALESCE(Valor,0) - COALESCE(valor_liquido,0)) AS tx
    FROM entrada
    WHERE date(Data) BETWEEN ? AND ?;
    """
    try:
        with _conn(db_path) as c:
            row = c.execute(sql, (ini, fim)).fetchone()
            return _safe(row[0]), _safe(row[1])
    except Exception:
        return 0.0, 0.0

@st.cache_data(show_spinner=False)
def _query_fretes(db_path: str, ini: str, fim: str) -> float:
    sql = """
    SELECT SUM(COALESCE(Frete,0))
    FROM mercadorias
    WHERE date(Data) BETWEEN ? AND ?;
    """
    try:
        with _conn(db_path) as c:
            row = c.execute(sql, (ini, fim)).fetchone()
            return _safe(row[0])
    except Exception:
        return 0.0

@st.cache_data(show_spinner=False)
def _query_saidas_total(db_path: str, ini: str, fim: str,
                        categoria: str, subcat: str | None = None) -> float:
    """
    Soma Valor de `saida` por filtros.
    - categoria: compara case-insensitive
    - subcat: se informado, compara case-insensitive
    Implementa fallback p/ diferenças de schema:
      - Sub_Categoria (preferido) ou Sub_Categorias_saida (legado)
      - se não achar nada com Categoria + Sub, tenta só Sub_Categoria
    """
    def _sum_with_sub(subcol: str) -> float:
        if subcat:
            sql = f"""
            SELECT SUM(COALESCE(Valor,0))
            FROM saida
            WHERE UPPER(Categoria)=UPPER(?)
              AND UPPER(COALESCE({subcol},''))=UPPER(?)
              AND date(Data) BETWEEN ? AND ?;
            """
            args = (categoria, subcat, ini, fim)
        else:
            sql = """
            SELECT SUM(COALESCE(Valor,0))
            FROM saida
            WHERE UPPER(Categoria)=UPPER(?)
              AND date(Data) BETWEEN ? AND ?;
            """
            args = (categoria, ini, fim)
        with _conn(db_path) as c:
            row = c.execute(sql, args).fetchone()
            return _safe(row[0])

    def _sum_only_sub(subcol: str) -> float:
        if not subcat:
            return 0.0
        sql = f"""
        SELECT SUM(COALESCE(Valor,0))
        FROM saida
        WHERE UPPER(COALESCE({subcol},''))=UPPER(?)
          AND date(Data) BETWEEN ? AND ?;
        """
        with _conn(db_path) as c:
            row = c.execute(sql, (subcat, ini, fim)).fetchone()
            return _safe(row[0])

    # 1) Tenta com Sub_Categoria
    try:
        total = _sum_with_sub("Sub_Categoria")
        if total == 0.0 and subcat:
            only = _sum_only_sub("Sub_Categoria")
            if only > 0.0:
                return only
        return total
    except Exception:
        pass

    # 2) Fallback: schema legado Sub_Categorias_saida
    try:
        total = _sum_with_sub("Sub_Categorias_saida")
        if total == 0.0 and subcat:
            only = _sum_only_sub("Sub_Categorias_saida")
            if only > 0.0:
                return only
        return total
    except Exception:
        return 0.0

@st.cache_data(show_spinner=False)
def _query_cap_emprestimos(db_path: str, competencia: str) -> float:
    """
    Soma valor_pago_acumulado em contas_a_pagar_mov:
    - tipo_obrigacao='EMPRESTIMO'
    - competencia='YYYY-MM'
    """
    sql = """
    SELECT SUM(COALESCE(valor_pago_acumulado,0))
    FROM contas_a_pagar_mov
    WHERE tipo_obrigacao = 'EMPRESTIMO'
      AND competencia = ?;
    """
    try:
        with _conn(db_path) as c:
            row = c.execute(sql, (competencia,)).fetchone()
            return _safe(row[0])
    except Exception:
        return 0.0

@st.cache_data(show_spinner=False)
def _query_mkt_cartao(db_path: str, ini: str, fim: str) -> float:
    """
    Soma Marketing via fatura_cartao_itens, usando APENAS colunas reais do banco:
      - data_compra (filtro de período)
      - categoria  (igual a 'Despesas / Marketing')
      - valor_parcela (soma)
    """
    sql = """
    SELECT SUM(COALESCE(valor_parcela, 0))
    FROM fatura_cartao_itens
    WHERE date(data_compra) BETWEEN ? AND ?
      AND categoria = 'Despesas / Marketing';
    """
    try:
        with _conn(db_path) as c:
            row = c.execute(sql, (ini, fim)).fetchone()
            return _safe(row[0])
    except Exception:
        return 0.0


# ============================== Anos disponíveis ==============================

@st.cache_data(show_spinner=False)
def _listar_anos(db_path: str) -> List[int]:
    """
    Lista anos presentes em entrada.Data, mercadorias.Data, saida.Data,
    contas_a_pagar_mov.competencia e (se existir) fatura_cartao_itens.data_compra.
    Tenta consulta completa; se falhar por falta de tabela/coluna, usa fallback sem o trecho problemático.
    """
    sql_all = """
    SELECT ano FROM (
        SELECT CAST(substr(Data,1,4) AS INT) AS ano FROM entrada       WHERE length(COALESCE(Data,'')) >= 4
        UNION
        SELECT CAST(substr(Data,1,4) AS INT) AS ano FROM mercadorias   WHERE length(COALESCE(Data,'')) >= 4
        UNION
        SELECT CAST(substr(Data,1,4) AS INT) AS ano FROM saida         WHERE length(COALESCE(Data,'')) >= 4
        UNION
        SELECT CAST(substr(competencia,1,4) AS INT) AS ano FROM contas_a_pagar_mov WHERE length(COALESCE(competencia,'')) >= 4
        UNION
        SELECT CAST(substr(data_compra,1,4) AS INT) AS ano FROM fatura_cartao_itens WHERE length(COALESCE(data_compra,'')) >= 4
    )
    WHERE ano IS NOT NULL
    ORDER BY ano;
    """
    sql_fallback = """
    SELECT ano FROM (
        SELECT CAST(substr(Data,1,4) AS INT) AS ano FROM entrada       WHERE length(COALESCE(Data,'')) >= 4
        UNION
        SELECT CAST(substr(Data,1,4) AS INT) AS ano FROM mercadorias   WHERE length(COALESCE(Data,'')) >= 4
        UNION
        SELECT CAST(substr(Data,1,4) AS INT) AS ano FROM saida         WHERE length(COALESCE(Data,'')) >= 4
        UNION
        SELECT CAST(substr(competencia,1,4) AS INT) AS ano FROM contas_a_pagar_mov WHERE length(COALESCE(competencia,'')) >= 4
    )
    WHERE ano IS NOT NULL
    ORDER BY ano;
    """
    anos: List[int] = []
    try:
        with _conn(db_path) as c:
            rows = c.execute(sql_all).fetchall()
            for r in rows:
                try:
                    anos.append(int(r[0]))
                except Exception:
                    pass
    except Exception:
        try:
            with _conn(db_path) as c:
                rows = c.execute(sql_fallback).fetchall()
                for r in rows:
                    try:
                        anos.append(int(r[0]))
                    except Exception:
                        pass
        except Exception:
            pass

    if not anos:
        anos = [pd.Timestamp.today().year]
    return sorted(set(anos))


# ============================== Cálculo por mês ==============================

@st.cache_data(show_spinner=False)
def _calc_mes(db_path: str, ano: int, mes: int, vars_dre: VarsDRE) -> Dict[str, float]:
    ini, fim, comp = _periodo_ym(ano, mes)

    fat, taxa_maq_rs = _query_entradas(db_path, ini, fim)
    fretes_rs = _query_fretes(db_path, ini, fim)
    fixas_rs = _query_saidas_total(db_path, ini, fim, "Custos Fixos")

    mkt_saida_rs  = _query_saidas_total(db_path, ini, fim, "Despesas", "Marketing")
    mkt_cartao_rs = _query_mkt_cartao(db_path, ini, fim)
    mkt_rs = mkt_saida_rs + mkt_cartao_rs

    limp_rs = _query_saidas_total(db_path, ini, fim, "Despesas", "Manutenção/Limpeza")
    emp_rs  = _query_cap_emprestimos(db_path, comp)

    simples_rs = fat * (vars_dre.simples / 100.0)
    fundo_rs   = fat * (vars_dre.fundo   / 100.0)
    sacolas_rs = fat * (vars_dre.sacolas / 100.0)
    cmv_rs     = (fat / vars_dre.markup) if vars_dre.markup > 0 else 0.0

    saida_imp_maq   = simples_rs + taxa_maq_rs
    receita_liq     = fat - saida_imp_maq

    total_grupo_cmv = cmv_rs + fretes_rs + sacolas_rs + fundo_rs
    margem_contrib  = receita_liq - total_grupo_cmv

    total_cf_emprestimos = fixas_rs + emp_rs
    total_saida_oper     = fixas_rs + emp_rs + mkt_rs + limp_rs
    ebitda               = margem_contrib - total_saida_oper

    return {
        "fat": fat,
        "simples": simples_rs,
        "taxa_maq": taxa_maq_rs,
        "saida_imp_maq": saida_imp_maq,
        "receita_liq": receita_liq,
        "cmv": cmv_rs,
        "fretes": fretes_rs,
        "sacolas": sacolas_rs,
        "fundo": fundo_rs,
        "margem_contrib": margem_contrib,
        "fixas": fixas_rs,
        "emp": emp_rs,
        "mkt": mkt_rs,
        "limp": limp_rs,
        "total_cf_emp": total_cf_emprestimos,
        "total_saida_oper": total_saida_oper,
        "ebitda": ebitda,
    }


# ============================== UI / Página ==============================

def render_dre(caminho_banco: str):
    """Página DRE — visão anual em uma tabela (pré-out/2025: apenas Faturamento)."""
    st.subheader("📉 DRE — Visão Anual (12 meses)")

    anos = _listar_anos(caminho_banco)
    ano = st.selectbox("Ano", options=anos, index=len(anos) - 1)

    if int(ano) == START_YEAR:
        st.caption("🔖 **YTD desde out/2025** — meses anteriores exibem apenas *Faturamento*.")

    vars_dre = _load_vars(caminho_banco)
    if vars_dre.markup <= 0:
        st.warning("⚠️ Markup médio não configurado (ou 0). CMV estimado será 0.")
    if all(v == 0 for v in (vars_dre.simples, vars_dre.markup, vars_dre.fundo, vars_dre.sacolas)):
        st.info("ℹ️ Configure em: Cadastros › Variáveis do DRE.")

    _render_anual(caminho_banco, int(ano), vars_dre)


def _render_anual(db_path: str, ano: int, vars_dre: VarsDRE):
    st.caption(f"Cada mês mostra **Valores R$** e **Análise Vertical (%)** • Ano: **{ano}**")

    meses = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]
    rows = [
        "Faturamento",
        "Simples Nacional",
        "Taxa Maquineta",
        "Saída Imposto e Maquininha",
        "Receita Líquida",
        "CMV (Mercadorias)",
        "Fretes",
        "Sacolas",
        "Fundo de Promoção",
        "Margem de Contribuição",
        "Custo Fixo Mensal",
        "Empréstimos/Financiamentos",
        "Marketing",
        "Manutenção/Limpeza",
        "Total CF + Empréstimos",
        "Total de Saída",
        "EBITDA Lucro/Prejuízo",
    ]

    columns = pd.MultiIndex.from_product([meses, ["Valores R$", "Análise Vertical"]])
    df = pd.DataFrame(index=rows, columns=columns, dtype=object)

    for i, mes in enumerate(range(1, 13), start=0):
        pre_start = (ano < START_YEAR) or (ano == START_YEAR and mes < START_MONTH)

        m = _calc_mes(db_path, ano, mes, vars_dre)
        fat = m["fat"]

        if pre_start:
            vals = {r: None for r in rows}
            vals["Faturamento"] = fat
            for r in rows:
                df.loc[r, (meses[i], "Valores R$")] = vals.get(r, None)
            df.loc["Faturamento", (meses[i], "Análise Vertical")] = (100.0 if fat and fat > 0 else 0.0)
            for r in rows:
                if r != "Faturamento":
                    df.loc[r, (meses[i], "Análise Vertical")] = None
        else:
            vals = {
                "Faturamento": m["fat"], "Simples Nacional": m["simples"], "Taxa Maquineta": m["taxa_maq"],
                "Saída Imposto e Maquininha": m["saida_imp_maq"],
                "Receita Líquida": m["receita_liq"] if m["receita_liq"] is not None else None,
                "CMV (Mercadorias)": m["cmv"], "Fretes": m["fretes"], "Sacolas": m["sacolas"],
                "Fundo de Promoção": m["fundo"],
                "Margem de Contribuição": m["margem_contrib"] if m["margem_contrib"] is not None else None,
                "Custo Fixo Mensal": m["fixas"], "Empréstimos/Financiamentos": m["emp"],
                "Marketing": m["mkt"], "Manutenção/Limpeza": m["limp"],
                "Total CF + Empréstimos": m["total_cf_emp"], "Total de Saída": m["total_saida_oper"],
                "EBITDA Lucro/Prejuízo": m["ebitda"],
            }
            for r in rows:
                df.loc[r, (meses[i], "Valores R$")] = vals.get(r, 0.0)

            if fat > 0:
                for r in rows:
                    v = vals.get(r, 0.0)
                    df.loc[r, (meses[i], "Análise Vertical")] = (v / fat * 100.0) if isinstance(v, (int, float)) else None
            else:
                df.loc[:, (meses[i], "Análise Vertical")] = 0.0

    def _fmt_val(v):
        if v is None:
            return "—"
        try:
            return _fmt_brl(float(v))
        except Exception:
            return "—"

    def _fmt_pct(v):
        if v is None:
            return "—"
        try:
            return f"{float(v):.0f}%"
        except Exception:
            return "—"

    df_show = df.copy()
    for mes in meses:
        df_show[(mes, "Valores R$")] = df_show[(mes, "Valores R$")].map(_fmt_val)
        df_show[(mes, "Análise Vertical")] = df_show[(mes, "Análise Vertical")].map(_fmt_pct)

    _KEY_ROWS = [
        "Faturamento",
        "Receita Líquida",
        "Saída Imposto e Maquininha",
        "Margem de Contribuição",
        "Total CF + Empréstimos",
        "Total de Saída",
        "EBITDA Lucro/Prejuízo",
    ]

    styler = df_show.style.set_properties(
        **{"font-weight": "bold", "font-size": "1.16em"},
        subset=pd.IndexSlice[_KEY_ROWS, :]
    )

    # Altura: mostrar +1 linha extra para eliminar qualquer scroll vertical
    rows_to_show = len(rows)      # 17
    row_px = 32                   # altura estimada por linha
    header_px = 96                # cabeçalho (MultiIndex)
    height_px = header_px + (rows_to_show + 1) * row_px  # <<< +1 linha

    st.dataframe(styler, use_container_width=True, height=height_px)


# Alias para retrocompatibilidade
pagina_dre = render_dre
