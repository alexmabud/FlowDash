# -*- coding: utf-8 -*-
# flowdash_pages/dre/dre.py
from __future__ import annotations

import sqlite3
from calendar import monthrange
from dataclasses import dataclass
from typing import Dict, Tuple, List, Iterable, Optional, Any
import os

import pandas as pd
import streamlit as st
from datetime import date

DEBUG_LOGS: List[Dict[str, Any]] = []


def _debug_log(label: str, payload: Any) -> None:
    """Collect debug information safely for later display."""
    def _sanitize(value: Any) -> Any:
        basic_types = (str, int, float, bool, type(None))
        if isinstance(value, basic_types):
            return value
        if isinstance(value, dict):
            return {str(k): _sanitize(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [_sanitize(v) for v in value]
        try:
            return repr(value)
        except Exception:
            return "<unrepresentable>"

    try:
        DEBUG_LOGS.append({"label": label, "data": _sanitize(payload)})
    except Exception as exc:
        print(f"[DRE DEBUG] failed to log {label}: {exc}")

# ============================== Config de início do DRE ==============================
START_YEAR = 2025
START_MONTH = 10  # Outubro
KPI_TITLE = "KPIs"  # título exibido acima dos cards

# ============================== Helpers ==============================
def _conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn

def _ensure_db_path_or_raise(pref: Optional[str] = None) -> str:
    """Resolve caminho do banco de forma resiliente.
    - Usa o parâmetro se existir.
    - Tenta `st.session_state['db_path']`/`['caminho_banco']`.
    - Procura nos caminhos padrão em `data/`.
    """
    searched_candidates: List[str] = []
    if pref and isinstance(pref, str):
        exists_pref = os.path.exists(pref)
        searched_candidates.append(pref)
        _debug_log(
            "_ensure_db_path_or_raise.pref",
            {"input": pref, "abs": os.path.abspath(pref), "exists": exists_pref},
        )
        if exists_pref:
            return pref
    try:
        for k in ("caminho_banco", "db_path"):
            v = st.session_state.get(k)
            if isinstance(v, str):
                exists_v = os.path.exists(v)
                searched_candidates.append(v)
                _debug_log(
                    "_ensure_db_path_or_raise.session_state",
                    {"key": k, "value": v, "abs": os.path.abspath(v), "exists": exists_v},
                )
                if exists_v:
                    return v
    except Exception as exc:
        _debug_log("_ensure_db_path_or_raise.session_state_error", {"error": repr(exc)})
    for p in (
        os.path.join("data", "flowdash_data.db"),
        os.path.join("data", "dashboard_rc.db"),
        "dashboard_rc.db",
        os.path.join("data", "flowdash_template.db"),
        "./flowdash_data.db",
    ):
        exists_p = os.path.exists(p)
        searched_candidates.append(p)
        _debug_log(
            "_ensure_db_path_or_raise.search_path",
            {"candidate": p, "abs": os.path.abspath(p), "exists": exists_p},
        )
        if exists_p:
            return p
    _debug_log("_ensure_db_path_or_raise.not_found", {"searched": searched_candidates, "cwd": os.getcwd()})
    raise FileNotFoundError("Nenhum banco encontrado. Defina st.session_state['db_path'].")

def _periodo_ym(ano: int, mes: int) -> Tuple[str, str, str]:
    last = monthrange(ano, mes)[1]
    ini = f"{ano:04d}-{mes:02d}-01"
    fim = f"{ano:04d}-{mes:02d}-{last:02d}"
    comp = f"{ano:04d}-{mes:02d}"
    return ini, fim, comp

def _fmt_brl(v: float) -> str:
    try:
        s = f"{float(v):,.2f}"
        return "R$ " + s.replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"

def _fmt_pct(v: float, casas: int = 1) -> str:
    try:
        return f"{float(v):.{casas}f}%"
    except Exception:
        return "—"

def _safe(v) -> float:
    try:
        return float(v or 0)
    except Exception:
        return 0.0

def _nz_div(n: float, d: float) -> float:
    return (n / d) if d not in (0, None) else 0.0

def _mes_anterior(ano: int, mes: int) -> Tuple[int, int]:
    return (ano, mes - 1) if mes > 1 else (ano - 1, 12)

# === Crescimento MTD (1º→D do mês vs 1º→D do mês anterior)
def _crescimento_mtd(db_path: str, ano: int, mes: int, today: Optional[date] = None) -> float:
    today = today or date.today()
    is_corrente = (ano == today.year and mes == today.month)
    D = today.day if is_corrente else monthrange(ano, mes)[1]

    ini_atual = f"{ano:04d}-{mes:02d}-01"
    fim_atual = f"{ano:04d}-{mes:02d}-{min(D, monthrange(ano, mes)[1]):02d}"
    fat_mtd, _, _ = _query_entradas(db_path, ini_atual, fim_atual)

    prev_ano, prev_mes = _mes_anterior(ano, mes)
    D_prev = min(D, monthrange(prev_ano, prev_mes)[1])
    ini_prev = f"{prev_ano:04d}-{prev_mes:02d}-01"
    fim_prev = f"{prev_ano:04d}-{prev_mes:02d}-{D_prev:02d}"
    fat_mtd_prev, _, _ = _query_entradas(db_path, ini_prev, fim_prev)

    return _nz_div(fat_mtd - fat_mtd_prev, fat_mtd_prev) * 100.0 if fat_mtd_prev > 0 else 0.0

@dataclass
class VarsDRE:
    # parâmetros configuráveis
    simples: float = 0.0   # %
    markup: float = 0.0    # coef
    sacolas: float = 0.0   # %
    fundo: float = 0.0     # %

    # avançados
    dep_padrao: float = 0.0     # R$/mês (depreciação padrão)
    pl_base: float = 0.0        # patrimônio líquido
    inv_base: float = 0.0       # investimento total
    atv_base: float = 0.0       # ativos totais

# ============================== Schema helpers ==============================
@st.cache_data(show_spinner=False)
def _table_cols(db_path: str, table: str) -> List[str]:
    try:
        with _conn(db_path) as c:
            rows = c.execute(f"PRAGMA table_info('{table}')").fetchall()
            return [str(r[1]).lower() for r in rows]
    except Exception as exc:
        _debug_log("_table_cols.error", {"error": repr(exc), "table": table, "db_path": db_path})
        return []

def _find_col(cols_lower: Iterable[str], candidates: Iterable[str]) -> Optional[str]:
    lowset = {c.lower() for c in cols_lower}
    for cand in candidates:
        if cand.lower() in lowset:
            return cand
    return None

# ============================== Queries (cache) ==============================
@st.cache_data(show_spinner=False, ttl=5)
def _load_vars(db_path: str) -> VarsDRE:
    _debug_log("_load_vars.start", {"db_path": db_path, "cwd": os.getcwd()})
    q = """
    SELECT chave, COALESCE(valor_num, 0) AS v
      FROM dre_variaveis
     WHERE chave IN (
        'aliquota_simples_nacional',
        'markup_medio',
        'sacolas_percent',
        'fundo_promocao_percent',
        'depreciacao_mensal_padrao',
        'patrimonio_liquido_base',
        'investimento_total_base',
        'ativos_totais_base'
     );
    """
    d: Dict[str, float] = {}
    try:
        with _conn(db_path) as c:
            df = pd.read_sql(q, c)
            _debug_log(
                "_load_vars.df",
                {"shape": list(df.shape), "head": df.head(5).to_dict(orient="records")},
            )
            d = {r["chave"]: float(r["v"] or 0) for _, r in df.iterrows()}
    except Exception as exc:
        _debug_log("_load_vars.error", {"error": repr(exc)})

    vars_dre_obj = VarsDRE(
        simples=_safe(d.get("aliquota_simples_nacional")),
        markup=_safe(d.get("markup_medio")),
        sacolas=_safe(d.get("sacolas_percent")),
        fundo=_safe(d.get("fundo_promocao_percent")),
        dep_padrao=_safe(d.get("depreciacao_mensal_padrao")),
        pl_base=_safe(d.get("patrimonio_liquido_base")),
        inv_base=_safe(d.get("investimento_total_base")),
        atv_base=_safe(d.get("ativos_totais_base")),
    )
    _debug_log("_load_vars.result", vars(vars_dre_obj))
    return vars_dre_obj

def _vars_dynamic_overrides(db_path: str, vars_dre: "VarsDRE") -> "VarsDRE":
    """Recalcula variáveis derivadas com base nos dados atuais, sem depender da tela de cadastro.

    - Ativos Totais (calc.) = Bancos+Caixa (consolidado) + Estoque atual (estimado) + Imobilizado (JSON)
    - PL (calc.) = max(0, Ativos Totais − Passivos Totais CAP)
    - Depreciação mensal padrão = Imobilizado × (taxa_dep% / 100)
    Mantém as variáveis de entrada (simples, markup, sacolas, fundo, investimento) como estão no DB.
    """
    _debug_log("_vars_dynamic_overrides.start", {"db_path": db_path, "base_vars": vars(vars_dre)})
    try:
        from flowdash_pages.cadastros.variaveis_dre import (
            get_estoque_atual_estimado as _estoque_est,
            _get_total_consolidado_bancos_caixa as _bancos_total,
            _get_passivos_totais_cap as _cap_totais,
            _load_ui_prefs as _load_prefs,
        )
    except Exception as exc:
        _debug_log("_vars_dynamic_overrides.import_error", {"error": repr(exc)})
        return vars_dre

    try:
        # dados auxiliares
        estoque_atual = float(_estoque_est(db_path) or 0.0)
        with _conn(db_path) as c_local:
            bancos_total, _ = _bancos_total(c_local, db_path)
        passivos_totais, _ = _cap_totais(db_path)

        # preferências JSON para imobilizado e taxa depreciação
        prefs = _load_prefs(db_path)
        imobilizado = _safe(prefs.get("pl_imobilizado_valor_total"))
        taxa_dep = _safe(prefs.get("dep_taxa_mensal_percent_live"))

        ativos_totais = float(bancos_total or 0.0) + float(estoque_atual or 0.0) + float(imobilizado or 0.0)
        pl_calc = ativos_totais - float(passivos_totais or 0.0)
        pl_calc_nn = pl_calc if pl_calc > 0 else 0.0
        dep_padrao = float(imobilizado) * (float(taxa_dep) / 100.0)

        result = VarsDRE(
            simples=vars_dre.simples,
            markup=vars_dre.markup,
            sacolas=vars_dre.sacolas,
            fundo=vars_dre.fundo,
            dep_padrao=dep_padrao,
            pl_base=pl_calc_nn,
            inv_base=vars_dre.inv_base,
            atv_base=ativos_totais,
        )
        _debug_log("_vars_dynamic_overrides.result", vars(result))
        return result
    except Exception as exc:
        _debug_log("_vars_dynamic_overrides.error", {"error": repr(exc)})
        return vars_dre

def _persist_overrides_to_db(db_path: str, vars_dre: "VarsDRE") -> None:
    """Grava em dre_variaveis os derivados recalculados (ativos_totais_base, patrimonio_liquido_base, depreciacao_mensal_padrao).
    Aplica threshold para evitar escrita desnecessária.
    """
    try:
        sql_create = (
            "CREATE TABLE IF NOT EXISTS dre_variaveis (\n"
            "    id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
            "    chave TEXT NOT NULL UNIQUE,\n"
            "    tipo  TEXT NOT NULL CHECK (tipo IN ('num','text','bool')),\n"
            "    valor_num  REAL,\n"
            "    valor_text TEXT,\n"
            "    descricao  TEXT,\n"
            "    updated_at TEXT NOT NULL DEFAULT (datetime('now'))\n"
            ");"
        )

        def _get_current(c: sqlite3.Connection, chave: str) -> float:
            try:
                row = c.execute("SELECT valor_num FROM dre_variaveis WHERE chave=? LIMIT 1", (chave,)).fetchone()
                return float(row[0]) if row and row[0] is not None else 0.0
            except Exception:
                return 0.0

        def _upsert_num(c: sqlite3.Connection, chave: str, val: float, desc: str) -> None:
            c.execute(
                """
                INSERT INTO dre_variaveis (chave, tipo, valor_num, descricao)
                VALUES (?,?,?,?)
                ON CONFLICT(chave) DO UPDATE SET
                    tipo=excluded.tipo,
                    valor_num=excluded.valor_num,
                    descricao=excluded.descricao,
                    updated_at=datetime('now')
                """,
                (chave, "num", float(val or 0.0), desc.strip()),
            )

        with _conn(db_path) as c:
            c.execute(sql_create)
            # Threshold de mudança para evitar escrita constante
            eps = 0.005
            targets = [
                ("ativos_totais_base", vars_dre.atv_base, "Ativos Totais (calc.) — usado no DRE"),
                ("patrimonio_liquido_base", vars_dre.pl_base, "Patrimônio Líquido (calc.) — usado no DRE"),
                ("depreciacao_mensal_padrao", vars_dre.dep_padrao, "Depreciação mensal p/ EBITDA (R$)"),
            ]
            for chave, novo, desc in targets:
                atual = _get_current(c, chave)
                if abs(float(novo or 0.0) - float(atual or 0.0)) > eps:
                    _upsert_num(c, chave, float(novo or 0.0), desc)
            c.commit()
    except Exception as exc:
        _debug_log("_persist_overrides_to_db.error", {"error": repr(exc), "db_path": db_path})

@st.cache_data(show_spinner=False, ttl=60)
def _query_entradas(db_path: str, ini: str, fim: str) -> Tuple[float, float, int]:
    sql = """
    SELECT
      SUM(COALESCE(Valor,0)) AS fat,
      SUM(COALESCE(Valor,0) - COALESCE(valor_liquido, COALESCE(Valor,0))) AS tx,
      COUNT(*) AS n
    FROM entrada
    WHERE date(Data) BETWEEN ? AND ?;
    """
    _debug_log("_query_entradas.start", {"db_path": db_path, "ini": ini, "fim": fim})
    try:
        with _conn(db_path) as c:
            row = c.execute(sql, (ini, fim)).fetchone()
            result = (_safe(row[0]), _safe(row[1]), int(row[2] or 0))
            _debug_log("_query_entradas.result", {"row": row, "result": result})
            return result
    except Exception as exc:
        _debug_log("_query_entradas.error", {"error": repr(exc), "db_path": db_path, "ini": ini, "fim": fim})
        return 0.0, 0.0, 0

@st.cache_data(show_spinner=False, ttl=60)
def _query_fretes(db_path: str, ini: str, fim: str) -> float:
    sql = """
    SELECT SUM(COALESCE(Frete,0))
    FROM mercadorias
    WHERE date(Data) BETWEEN ? AND ?;
    """
    _debug_log("_query_fretes.start", {"db_path": db_path, "ini": ini, "fim": fim})
    try:
        with _conn(db_path) as c:
            row = c.execute(sql, (ini, fim)).fetchone()
            result = _safe(row[0])
            _debug_log("_query_fretes.result", {"row": row, "result": result})
            return result
    except Exception as exc:
        _debug_log("_query_fretes.error", {"error": repr(exc), "db_path": db_path, "ini": ini, "fim": fim})
        return 0.0


def compute_total_saida_operacional(ano: int, mes: int, db_path: str) -> float:
    """Soma custos/despesas operacionais do mês excluindo itens financeiros e não operacionais."""
    ini, fim, _ = _periodo_ym(ano, mes)

    excluded_tokens = (
        "JURO",
        "JUROS",
        "TARIFA",
        "BANC",
        "IOF",
        "EMPREST",
        "FINANC",
        "PARCELA",
        "PRINCIPAL",
        "APORTE",
        "RETIRADA",
        "IMOBILIZ",
        "INVEST",
        "AMORTIZ",
        "MAQUIN",
        "CARTAO",
        "CARTÃO",
    )
    exclusion_checks = []
    exclusion_args: List[str] = []
    for token in excluded_tokens:
        upper_token = token.upper()
        exclusion_checks.append("instr(UPPER(TRIM(COALESCE(Categoria,''))), ?) > 0")
        exclusion_args.append(upper_token)
        exclusion_checks.append("instr(UPPER(TRIM(COALESCE(Sub_Categoria,''))), ?) > 0")
        exclusion_args.append(upper_token)

    exclusion_clause = ""
    if exclusion_checks:
        exclusion_clause = " AND NOT (" + " OR ".join(exclusion_checks) + ")"

    def _sum(sql_base: str, extra_params: List[str]) -> float:
        sql = sql_base + exclusion_clause + ";"
        params = [ini, fim] + list(extra_params) + exclusion_args
        try:
            with _conn(db_path) as c:
                row = c.execute(sql, params).fetchone()
                return _safe(row[0])
        except Exception as exc:
            _debug_log(
                "compute_total_saida_operacional.sum_error",
                {"error": repr(exc), "sql": sql_base, "params": params},
            )
            return 0.0

    fixos_sql = """
    SELECT SUM(COALESCE(Valor,0))
    FROM saida
    WHERE date(Data) BETWEEN ? AND ?
      AND TRIM(UPPER(COALESCE(Categoria,''))) = 'CUSTOS FIXOS'
    """

    extras_sql = """
    SELECT SUM(COALESCE(Valor,0))
    FROM saida
    WHERE date(Data) BETWEEN ? AND ?
      AND TRIM(UPPER(COALESCE(Categoria,''))) IN (?, ?)
      AND TRIM(UPPER(COALESCE(Sub_Categoria,''))) IN (?, ?, ?, ?)
    """

    fixos_total = _sum(fixos_sql, [])
    extras_total = _sum(
        extras_sql,
        [
            "DESPESAS",
            "OUTROS",
            "MARKETING",
            "MANUTENÇÃO/LIMPEZA",
            "MANUTENCAO/LIMPEZA",
            "OUTROS",
        ],
    )

    _debug_log(
        "compute_total_saida_operacional.result",
        {"ano": ano, "mes": mes, "fixos_total": fixos_total, "extras_total": extras_total},
    )
    return fixos_total + extras_total


@st.cache_data(show_spinner=False, ttl=60)
def _query_saidas_total(db_path: str, ini: str, fim: str,
                        categoria: str, subcat: str | None = None) -> float:
    _debug_log(
        "_query_saidas_total.start",
        {"db_path": db_path, "ini": ini, "fim": fim, "categoria": categoria, "subcat": subcat},
    )
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
        try:
            with _conn(db_path) as c:
                row = c.execute(sql, (subcat, ini, fim)).fetchone()
                return _safe(row[0])
        except Exception as exc:
            _debug_log(
                "compute_total_saida_operacional.sum_only_error",
                {"error": repr(exc), "sql": sql, "params": (subcat, ini, fim)},
            )
            return 0.0

    try:
        total = _sum_with_sub("Sub_Categoria")
        if total == 0.0 and subcat:
            only = _sum_only_sub("Sub_Categoria")
            if only > 0.0:
                _debug_log(
                    "_query_saidas_total.fallback_only",
                    {"subcol": "Sub_Categoria", "only": only},
                )
                return only
        _debug_log(
            "_query_saidas_total.result",
            {"subcol": "Sub_Categoria", "total": total},
        )
        return total
    except Exception:
        _debug_log(
            "_query_saidas_total.error_primary",
            {"db_path": db_path, "ini": ini, "fim": fim, "categoria": categoria, "subcat": subcat},
        )

    try:
        total = _sum_with_sub("Sub_Categorias_saida")
        if total == 0.0 and subcat:
            only = _sum_only_sub("Sub_Categorias_saida")
            if only > 0.0:
                _debug_log(
                    "_query_saidas_total.fallback_only",
                    {"subcol": "Sub_Categorias_saida", "only": only},
                )
                return only
        _debug_log(
            "_query_saidas_total.result",
            {"subcol": "Sub_Categorias_saida", "total": total},
        )
        return total
    except Exception as exc:
        _debug_log(
            "_query_saidas_total.error_secondary",
            {"error": repr(exc), "db_path": db_path, "ini": ini, "fim": fim, "categoria": categoria, "subcat": subcat},
        )
        return 0.0

@st.cache_data(show_spinner=False, ttl=60)
def _query_cap_emprestimos(db_path: str, competencia: str) -> float:
    # desembolso de caixa do mês com EMPRESTIMO
    sql = """
    SELECT SUM(COALESCE(valor_pago_acumulado,0))
    FROM contas_a_pagar_mov
    WHERE tipo_obrigacao = 'EMPRESTIMO'
      AND competencia = ?;
    """
    _debug_log("_query_cap_emprestimos.start", {"db_path": db_path, "competencia": competencia})
    try:
        with _conn(db_path) as c:
            row = c.execute(sql, (competencia,)).fetchone()
            result = _safe(row[0])
            _debug_log("_query_cap_emprestimos.result", {"row": row, "result": result})
            return result
    except Exception as exc:
        _debug_log("_query_cap_emprestimos.error", {"error": repr(exc), "db_path": db_path, "competencia": competencia})
        return 0.0

@st.cache_data(show_spinner=False, ttl=60)
def _query_divida_estoque(db_path: str) -> float:
    sql = """
    SELECT SUM(
        CASE
          WHEN (COALESCE(valor_evento,0) - COALESCE(valor_pago_acumulado,0)) > 0
          THEN (COALESCE(valor_evento,0) - COALESCE(valor_pago_acumulado,0))
          ELSE 0
        END
    )
    FROM contas_a_pagar_mov
    WHERE tipo_obrigacao = 'EMPRESTIMO';
    """
    _debug_log("_query_divida_estoque.start", {"db_path": db_path})
    try:
        with _conn(db_path) as c:
            row = c.execute(sql).fetchone()
            result = _safe(row[0])
            _debug_log("_query_divida_estoque.result", {"row": row, "result": result})
            return result
    except Exception as exc:
        _debug_log("_query_divida_estoque.error", {"error": repr(exc), "db_path": db_path})
        return 0.0

@st.cache_data(show_spinner=False, ttl=60)
def _query_mkt_cartao(db_path: str, ini: str, fim: str) -> float:
    sql = """
    SELECT SUM(COALESCE(valor_parcela, 0))
    FROM fatura_cartao_itens
    WHERE date(data_compra) BETWEEN ? AND ?
      AND categoria = 'Despesas / Marketing';
    """
    _debug_log("_query_mkt_cartao.start", {"db_path": db_path, "ini": ini, "fim": fim})
    try:
        with _conn(db_path) as c:
            row = c.execute(sql, (ini, fim)).fetchone()
            result = _safe(row[0])
            _debug_log("_query_mkt_cartao.result", {"row": row, "result": result})
            return result
    except Exception as exc:
        _debug_log("_query_mkt_cartao.error", {"error": repr(exc), "db_path": db_path, "ini": ini, "fim": fim})
        return 0.0

# ============================== Anos disponíveis ==============================
@st.cache_data(show_spinner=False)
def _listar_anos(db_path: str) -> List[int]:
    _debug_log("_listar_anos.start", {"db_path": db_path})
    sql_all = """
    SELECT ano FROM (
        SELECT CAST(substr(Data,1,4) AS INT) AS ano FROM entrada
        UNION SELECT CAST(substr(Data,1,4) AS INT) AS ano FROM mercadorias
        UNION SELECT CAST(substr(Data,1,4) AS INT) AS ano FROM saida
        UNION SELECT CAST(substr(competencia,1,4) AS INT) AS ano FROM contas_a_pagar_mov
        UNION SELECT CAST(substr(data_compra,1,4) AS INT) AS ano FROM fatura_cartao_itens
    )
    WHERE ano IS NOT NULL
    ORDER BY ano;
    """
    sql_fallback = """
    SELECT ano FROM (
        SELECT CAST(substr(Data,1,4) AS INT) AS ano FROM entrada
        UNION SELECT CAST(substr(Data,1,4) AS INT) AS ano FROM mercadorias
        UNION SELECT CAST(substr(Data,1,4) AS INT) AS ano FROM saida
        UNION SELECT CAST(substr(competencia,1,4) AS INT) AS ano FROM contas_a_pagar_mov
    )
    WHERE ano IS NOT NULL
    ORDER BY ano;
    """
    anos: List[int] = []
    try:
        with _conn(db_path) as c:
            rows = c.execute(sql_all).fetchall()
            _debug_log("_listar_anos.rows_all", {"count": len(rows)})
            for r in rows:
                try:
                    anos.append(int(r[0]))
                except Exception:
                    pass
    except Exception as exc:
        _debug_log("_listar_anos.error_all", {"error": repr(exc)})
        try:
            with _conn(db_path) as c:
                rows = c.execute(sql_fallback).fetchall()
                _debug_log("_listar_anos.rows_fallback", {"count": len(rows)})
                for r in rows:
                    try:
                        anos.append(int(r[0]))
                    except Exception:
                        pass
        except Exception as exc:
            _debug_log("_listar_anos.error_fallback", {"error": repr(exc)})

    if not anos:
        anos = [pd.Timestamp.today().year]
    _debug_log("_listar_anos.result", {"anos": sorted(set(anos))})
    return sorted(set(anos))

# ============================== Cálculo por mês ==============================
@st.cache_data(show_spinner=False)
def _calc_mes(db_path: str, ano: int, mes: int, vars_dre: "VarsDRE") -> Dict[str, float]:
    _debug_log(
        "_calc_mes.start",
        {"db_path": db_path, "ano": ano, "mes": mes, "vars": vars(vars_dre)},
    )
    ini, fim, comp = _periodo_ym(ano, mes)

    fat, taxa_maq_rs, n_vendas = _query_entradas(db_path, ini, fim)
    fretes_rs = _query_fretes(db_path, ini, fim)
    fixas_rs = _query_saidas_total(db_path, ini, fim, "Custos Fixos")

    mkt_saida_rs  = _query_saidas_total(db_path, ini, fim, "Despesas", "Marketing")
    mkt_rs = mkt_saida_rs

    limp_rs = _query_saidas_total(db_path, ini, fim, "Despesas", "Manutenção/Limpeza")
    emp_rs  = _query_saidas_total(db_path, ini, fim, "Empréstimos e Financiamentos")

    simples_rs = fat * (vars_dre.simples / 100.0)
    fundo_rs   = fat * (vars_dre.fundo   / 100.0)
    sacolas_rs = fat * (vars_dre.sacolas / 100.0)

    # ===== CMV corrigido: faturamento ÷ markup + frete de compra (mercadorias)
    base_cmv = (fat / vars_dre.markup) if vars_dre.markup > 0 else 0.0
    cmv_rs   = base_cmv + fretes_rs

    saida_imp_maq   = simples_rs + taxa_maq_rs
    receita_liq     = fat - saida_imp_maq

    # total_var NÃO soma frete novamente (já incluso no CMV)
    total_var       = cmv_rs + sacolas_rs + fundo_rs
    margem_contrib  = receita_liq - total_var
    lucro_bruto     = receita_liq - cmv_rs

    total_oper_fixo_extra = compute_total_saida_operacional(ano, mes, db_path)
    total_cf_emprestimos = fixas_rs + emp_rs
    total_saida_oper     = total_oper_fixo_extra + total_var

    # EBITDA base
    ebitda_base = receita_liq - total_saida_oper

    # EBIT: apenas depreciação (não usamos amortização)
    dep_extra = vars_dre.dep_padrao
    ebit = ebitda_base - dep_extra

    # Lucro líquido (simplificado)
    lucro_liq = ebit

    # KPIs
    rl = receita_liq
    mc_ratio = _nz_div(margem_contrib, rl)

    break_even_rs = (fixas_rs / mc_ratio) if mc_ratio > 0 else 0.0
    break_even_pct = _nz_div(break_even_rs, rl)

    break_even_financeiro_rs = (total_cf_emprestimos / mc_ratio) if mc_ratio > 0 else 0.0
    break_even_financeiro_pct = _nz_div(break_even_financeiro_rs, rl)

    margem_seguranca_pct = _nz_div((rl - break_even_rs), rl)

    eficiencia_oper_pct = _nz_div(total_saida_oper, rl)
    rel_saida_entrada_pct = _nz_div(total_saida_oper, fat)
    emp_pct_sobre_receita = _nz_div(emp_rs, rl) * 100.0

    ticket_medio = _nz_div(fat, float(n_vendas)) if n_vendas > 0 else 0.0

    margem_bruta_pct = _nz_div(lucro_bruto, rl)
    margem_operacional_pct = _nz_div(ebit, rl)
    margem_liquida_pct = _nz_div(lucro_liq, rl)
    margem_contrib_pct = _nz_div(margem_contrib, rl)
    custo_fixo_sobre_receita_pct = _nz_div(fixas_rs, rl)

    divida_estoque_rs = _query_divida_estoque(db_path)
    indice_endividamento_pct = (_nz_div(divida_estoque_rs, vars_dre.atv_base) * 100.0) if vars_dre.atv_base > 0 else 0.0

    result = {
        # básicos/estruturais
        "fat": fat,
        "simples": simples_rs,
        "taxa_maq": taxa_maq_rs,
        "saida_imp_maq": saida_imp_maq,
        "receita_liq": rl,
        "cmv": cmv_rs,
        "fretes": fretes_rs,
        "sacolas": sacolas_rs,
        "fundo": fundo_rs,

        # resultados operacionais
        "margem_contrib": margem_contrib,
        "fixas": fixas_rs,
        "emp": emp_rs,
        "mkt": mkt_rs,
        "limp": limp_rs,
        "total_cf_emp": total_cf_emprestimos,
        "total_saida_oper": total_saida_oper,
        "total_oper_fixo_extra": total_oper_fixo_extra,

        # lucros/caixa
        "ebitda": ebitda_base,
        "ebit": ebit,
        "lucro_liq": lucro_liq,
        "lucro_bruto": lucro_bruto,

        # variáveis auxiliares
        "total_var": total_var,
        "n_vendas": n_vendas,
        "ticket_medio": ticket_medio,

        # margens
        "margem_bruta_pct": margem_bruta_pct * 100.0,
        "margem_operacional_pct": margem_operacional_pct * 100.0,
        "margem_liquida_pct": margem_liquida_pct * 100.0,
        "margem_contrib_pct": margem_contrib_pct * 100.0,

        # eficiência/gestão
        "custo_fixo_sobre_receita_pct": custo_fixo_sobre_receita_pct * 100.0,
        "break_even_rs": break_even_rs,
        "break_even_pct": break_even_pct * 100.0,
        "break_even_financeiro_rs": break_even_financeiro_rs,
        "break_even_financeiro_pct": break_even_financeiro_pct * 100.0,
        "margem_seguranca_pct": margem_seguranca_pct * 100.0,
        "eficiencia_oper_pct": eficiencia_oper_pct * 100.0,
        "rel_saida_entrada_pct": rel_saida_entrada_pct * 100.0,
        "emp_pct_sobre_receita": emp_pct_sobre_receita,

        # endividamento (estoque)
        "divida_estoque": divida_estoque_rs,
        "indice_endividamento_pct": indice_endividamento_pct,

        # avançados
        "dep_extra": dep_extra,
        "roe_pct": (_nz_div(lucro_liq, vars_dre.pl_base) * 100.0) if vars_dre.pl_base > 0 else 0.0,
        "roi_pct": (_nz_div(lucro_liq, vars_dre.inv_base) * 100.0) if vars_dre.inv_base > 0 else 0.0,
        "roa_pct": (_nz_div(lucro_liq, vars_dre.atv_base) * 100.0) if vars_dre.atv_base > 0 else 0.0,
    }
    core_keys = ["fat", "receita_liq", "cmv", "total_var", "lucro_bruto"]
    _debug_log(
        "_calc_mes.result_subset",
        {k: result.get(k) for k in core_keys},
    )
    none_values = {k: v for k, v in result.items() if v is None}
    if none_values:
        _debug_log("_calc_mes.none_values", none_values)
    _debug_log("_calc_mes.result_summary", {"keys": sorted(result.keys())})
    return result

# ============================== UI / Página ==============================

def render_dre(caminho_banco: Optional[str]):
    DEBUG_LOGS.clear()
    _debug_log("render_dre.call", {"param_caminho_banco": caminho_banco})
    cwd_value = os.getcwd()
    _debug_log("render_dre.cwd", {"cwd": cwd_value})
    caminho_banco = _ensure_db_path_or_raise(caminho_banco)
    resolved_db_path = os.path.abspath(caminho_banco)
    db_exists = os.path.exists(caminho_banco)
    db_accessible = os.access(caminho_banco, os.R_OK)
    path_context = {
        "cwd": cwd_value,
        "resolved_db_path": resolved_db_path,
        "db_exists": db_exists,
        "db_accessible": db_accessible,
        "db_dir": os.path.dirname(resolved_db_path),
    }
    _debug_log("render_dre.path_context_initial", path_context)
    session_keys = sorted(list(st.session_state.keys()))
    session_focus_before = {k: st.session_state.get(k) for k in ("ano", "mes", "perfil", "db_path", "caminho_banco")}
    _debug_log("render_dre.session_state_initial", {"keys": session_keys, "focus": session_focus_before})

    anos = _listar_anos(caminho_banco)
    ano_atual = int(pd.Timestamp.today().year)
    if ano_atual not in anos:
        anos = sorted(set(anos + [ano_atual]))

    idx_default = anos.index(ano_atual) if ano_atual in anos else len(anos) - 1
    ano = st.selectbox("Ano", options=anos, index=idx_default)

    meses_labels = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]
    mes_default = int(pd.Timestamp.today().month) if int(ano) == ano_atual else 12
    mes = st.selectbox("Mês (para KPIs)", options=list(range(1,13)), index=mes_default-1,
                       format_func=lambda m: meses_labels[m-1])
    _debug_log("render_dre.selected_period", {"ano": ano, "mes": mes})

    st.subheader("KPIs - Indicadores-chave que medem o desempenho em relação às metas.")

    vars_dre = _load_vars(caminho_banco)
    vars_dre = _vars_dynamic_overrides(caminho_banco, vars_dre)
    _persist_overrides_to_db(caminho_banco, vars_dre)
    _debug_log("render_dre.vars_dre_after_overrides", vars(vars_dre))
    if vars_dre.markup <= 0:
        st.warning("⚠️ Markup médio não configurado (ou 0). CMV estimado será 0.")
    if all(v == 0 for v in (vars_dre.simples, vars_dre.fundo, vars_dre.sacolas)) and vars_dre.markup == 0:
        st.info("ℹ️ Configure em: Cadastros > Variáveis do DRE.")

    _render_kpis_mes_cards(caminho_banco, int(ano), int(mes), vars_dre)
    _render_anual(caminho_banco, int(ano), vars_dre)

    session_focus_after = {k: st.session_state.get(k) for k in ("ano", "mes", "perfil", "db_path", "caminho_banco")}
    _debug_log("render_dre.session_state_after", session_focus_after)
    path_compare = {
        "cwd": cwd_value,
        "db_dir": os.path.dirname(resolved_db_path),
        "resolved_db_path": resolved_db_path,
        "db_exists": db_exists,
        "db_accessible": db_accessible,
    }
    _debug_log("render_dre.path_context_final", path_compare)

    session_state_dump = {k: repr(st.session_state.get(k)) for k in session_keys}
    focus_none = {k: v for k, v in session_focus_after.items() if v in (None, "", [], {}, ())}

    core_kpis = None
    calc_none = None
    load_vars_info = None
    for log in DEBUG_LOGS:
        if log["label"] == "_calc_mes.result_subset":
            core_kpis = log["data"]
        if log["label"] == "_calc_mes.none_values":
            calc_none = log["data"]
        if log["label"] == "_load_vars.df":
            load_vars_info = log["data"]

    logs_table = pd.DataFrame(
        [{"label": log["label"], "data": repr(log["data"])} for log in DEBUG_LOGS]
    )

    with st.expander("Diagnóstico DRE (temporário)", expanded=True):
        st.write("Contexto de caminhos e acesso ao banco", path_compare)
        if not db_exists or not db_accessible:
            st.error("Banco de dados inacessível ou ausente no caminho resolvido acima.")
        st.write("Session state (foco)", session_focus_after)
        if focus_none:
            st.warning(f"Valores possivelmente problemáticos no session_state: {focus_none}")
        st.write("Session state completo (repr)", session_state_dump)
        st.write(
            "Comparação os.getcwd() x pasta do banco",
            {
                "cwd": cwd_value,
                "db_dir": os.path.dirname(resolved_db_path),
                "iguais": os.path.abspath(cwd_value) == os.path.abspath(os.path.dirname(resolved_db_path)),
            },
        )

        if core_kpis:
            st.write("KPIs usados nos chips (resultado do _calc_mes)", core_kpis)
            st.write({"shape": (1, len(core_kpis))})
            st.dataframe(pd.DataFrame([core_kpis]))
        if calc_none:
            st.error(f"Chaves com valor None em _calc_mes: {calc_none}")
        if load_vars_info:
            st.write("Variáveis carregadas do banco (shape/head)", {"shape": load_vars_info.get("shape")})
            st.dataframe(pd.DataFrame(load_vars_info.get("head", [])))

        if not logs_table.empty:
            st.write("Logs coletados (últimos registros)", logs_table.tail(30))

        st.write("Total de logs coletados", len(DEBUG_LOGS))
def _render_kpis_mes_cards(db_path: str, ano: int, mes: int, vars_dre: VarsDRE) -> None:
    st.markdown(
    """
<style>
.fd-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px;margin-top:6px}
.cap-card{border:1px solid rgba(255,255,255,0.10);border-radius:16px;padding:14px 16px;background:rgba(255,255,255,0.03);box-shadow:0 1px 4px rgba(0,0,0,0.10)}
.cap-title-xl{font-size:1.05rem;font-weight:700;margin:2px 0 8px}
.fd-card-body{display:flex;flex-wrap:wrap}

/* chip base */
.fd-chip{display:inline-flex;align-items:center;border:1px solid rgba(255,255,255,0.12);background:rgba(255,255,255,0.05);padding:6px 10px;border-radius:9999px;margin:4px 6px 0 0;font-size:.92rem;line-height:1;position:relative}
.fd-chip .k{opacity:.90;margin-right:6px}
.fd-chip .v{font-weight:700;margin-right:6px}

/* "?" com <details> */
.fd-chip details.qwrap{display:inline-block;margin-left:2px;position:relative}
.fd-chip details.qwrap > summary.q{list-style:none;display:inline-flex;align-items:center;justify-content:center;width:16px;height:16px;border-radius:50%;background:#8a8a8a;color:#fff;font-size:12px;line-height:16px;border:none;cursor:pointer;outline:none}
.fd-chip details.qwrap > summary.q::-webkit-details-marker{display:none}
.fd-chip details.qwrap[open] > summary.q{background:#9a9a9a}

/* tooltip */
.fd-chip details.qwrap .tip{position:absolute;left:0;top:calc(100% + 8px);background:rgba(25,25,25,.98);color:#fff;border:1px solid rgba(255,255,255,0.08);border-radius:10px;box-shadow:0 6px 20px rgba(0,0,0,.28);padding:8px 10px;max-width:360px;min-width:240px;z-index:20;font-size:.86rem}
@media (prefers-reduced-motion:no-preference){
  .fd-chip details.qwrap .tip{animation:fd-fade .12s ease}
}
@keyframes fd-fade{from{opacity:0;transform:translateY(-4px)}to{opacity:1;transform:translateY(0)}

/* cores por categoria */
.cap-card.k-estrut{border-left:6px solid #2ecc71}
.cap-card.k-estrut .cap-title-xl{color:#2ecc71}
.cap-card.k-margens{border-left:6px solid #3498db}
.cap-card.k-margens .cap-title-xl{color:#3498db}
.cap-card.k-efic{border-left:6px solid #9b59b6}
.cap-card.k-efic .cap-title-xl{color:#9b59b6}
.cap-card.k-fluxo{border-left:6px solid #f39c12}
.cap-card.k-fluxo .cap-title-xl{color:#f39c12}
.cap-card.k-cresc{border-left:6px solid #1abc9c}
.cap-card.k-cresc .cap-title-xl{color:#1abc9c}
.cap-card.k-avanc{border-left:6px solid #e91e63}
.cap-card.k-avanc .cap-title-xl{color:#e91e63}
</style>
    """,
    unsafe_allow_html=True,
)

    HELP: Dict[str, str] = {
        "Receita Bruta": "Total vendido no período, antes de impostos e taxas. | Serve para: medir o volume total de vendas antes de qualquer dedução (base para metas e sazonalidade).",
        "Receita Líquida": "Receita após impostos e taxas sobre as vendas. | Serve para: mostrar quanto realmente entra após deduções diretas das vendas (base das margens e do Lucro Bruto).",
        "CMV": "Custo das mercadorias vendidas: faturamento ÷ markup + frete de compra (mercadorias). | Serve para: indicar o custo do que foi efetivamente vendido (driver do Lucro Bruto e da precificação).",
        "Total de Variáveis (R$)": "Soma dos custos variáveis: CMV (já inclui frete), Sacolas e Fundo de Promoção. | Serve para: somar os custos que variam com a venda (base da Margem de Contribuição e do Ponto de Equilíbrio).",
        "Total de Saída Operacional (R$)": "Total de despesas operacionais (fixas e variáveis). | Serve para: mostrar quanto a operação gasta no mês (fixos + variáveis + extras), excluindo despesas financeiras e itens não operacionais. Base para EBITDA, eficiência e margens.",
        "Lucro Bruto": "Receita líquida menos o CMV. | Serve para: mostrar o ganho sobre as vendas antes das despesas operacionais (sinal da eficiência de compra e preço).",
        "Custo Fixo Mensal (R$)": "Soma das saídas classificadas como Custos Fixos no mês (aluguel, energia, internet etc.).",
        "Margem Bruta": "Quanto da receita líquida sobra após o CMV. | Serve para: medir a eficiência de precificação e compra — quanto sobra das vendas depois do CMV; base para avaliar se preço e custo estão saudáveis antes das despesas operacionais.",
        "Margem Bruta (%)": "Serve para: medir a eficiência de precificação e compra — quanto sobra das vendas depois do CMV; base para avaliar se preço e custo estão saudáveis antes das despesas operacionais.",
        "Margem Operacional": "Lucro operacional após depreciação.",
        "Margem Líquida": "Lucro final como % da receita líquida.",
        "Margem de Contribuição": "Quanto sobra para cobrir fixos e gerar lucro. | Serve para: indicar quanto de cada R$ vendido sobra para pagar despesas fixas e gerar lucro depois de todos os custos variáveis (CMV, taxas de cartão, sacolas, fundo de promoção, comissões etc.); base do Ponto de Equilíbrio e decisões de preço.",
        "Margem de Contribuição (%)": "Serve para: indicar quanto de cada R$ vendido sobra para pagar despesas fixas e gerar lucro depois de todos os custos variáveis (CMV, taxas de cartão, sacolas, fundo de promoção, comissões etc.); base do Ponto de Equilíbrio e decisões de preço.",
        "Margem de Contribuição (R$)": "Serve para: mostrar, em reais, quanto sobra das vendas após todos os custos variáveis; valor que efetivamente contribui para cobrir despesas fixas e lucro.",
        "Custo Fixo / Receita": "Peso dos custos fixos sobre a receita.",
        "Ponto de Equilíbrio (Contábil) (R$)": "Receita mínima para zerar o resultado considerando apenas custos fixos.",
        "Ponto de Equilíbrio Financeiro (R$)": "Receita mínima para o caixa não ficar negativo: (Custos Fixos + Empréstimos) ÷ Margem de Contribuição.",
        "Ponto de Equilíbrio (Contábil) (%)": "Percentual da receita líquida que representa o ponto de equilíbrio contábil.",
        "Ponto de Equilíbrio Financeiro (%)": "Percentual da receita líquida que representa o ponto de equilíbrio financeiro.",
        "Margem de Segurança": "Folga da receita acima do ponto de equilíbrio.",
        "Eficiência Operacional": "Quanto da receita líquida é consumida pelas saídas operacionais (fixos + marketing + limpeza + empréstimos).",
        "Relação Saídas/Entradas": "Quanto do faturamento bruto é consumido pelas saídas operacionais.",
        "Gasto c/ Empréstimos (R$)": "Desembolso do mês com parcelas pagas.",
        "Gasto c/ Empréstimos (%)": "Gasto com empréstimos como % da receita líquida.",
        "Dívida (Estoque)": "Saldo devedor ainda em aberto.",
        "Índice de Endividamento (%)": "Quanto dos ativos está comprometido com dívidas.",
        "Ticket Médio": "Média de valor por venda.",
        "Nº de Vendas": "Quantidade de vendas no período.",
        "Crescimento de Receita (m/m)": "Variação do faturamento comparado ao mês anterior.",
        "EBITDA": "Lucro operacional antes de juros, impostos, depreciação e amortização. | Serve para: avaliar a geração de caixa das operações, sem efeitos financeiros ou contábeis.",
        "EBIT": "Lucro operacional após depreciação.",
        "Lucro Líquido": "Resultado final no modelo simplificado.",
        "ROE": "Retorno do lucro sobre o patrimônio líquido.",
        "ROI": "Retorno do lucro sobre o investimento total.",
        "ROA": "Retorno do lucro sobre os ativos totais.",
    }

    def _chip(lbl: str, val: str) -> str:
        tip = HELP.get(lbl, "")
        if tip:
            return (f'<span class="fd-chip"><span class="k">{lbl}</span>'
                    f'<span class="v">{val}</span>'
                    f'<details class="qwrap"><summary class="q">?</summary><div class="tip">{tip}</div></details></span>')
        return f'<span class="fd-chip"><span class="k">{lbl}</span><span class="v">{val}</span></span>'

    def _chip_duo(lbl: str, val_rs: float, val_pct: float, help_key: Optional[str] = None) -> str:
        tip = HELP.get(help_key or lbl, "")
        val_comb = f'{_fmt_brl(val_rs)} | (%) {_fmt_pct(val_pct)}'
        if tip:
            return (f'<span class="fd-chip"><span class="k">{lbl}</span>'
                    f'<span class="v">{val_comb}</span>'
                    f'<details class="qwrap"><summary class="q">?</summary><div class="tip">{tip}</div></details></span>')
        return f'<span class="fd-chip"><span class="k">{lbl}</span><span class="v">{val_comb}</span></span>'

    def _card(title: str, chips: List[str], cls: str) -> str:
        return f'<div class="cap-card {cls}"><div class="cap-title-xl">{title}</div><div class="fd-card-body">{"".join(chips)}</div></div>'

    m = _calc_mes(db_path, ano, mes, vars_dre)

    try:
        crec = _crescimento_mtd(db_path, ano, mes)
    except Exception:
        crec = 0.0

    fixas_rs = m.get("fixas")
    if fixas_rs is None:
        try:
            ini, fim, _ = _periodo_ym(ano, mes)
            fixas_rs = _query_saidas_total(db_path, ini, fim, "Custos Fixos")
        except Exception:
            fixas_rs = 0.0
    fixas_rs = _safe(fixas_rs)

    cards_html: List[str] = []

    cards_html.append(_card("Estruturais", [
        _chip("Receita Bruta", _fmt_brl(m["fat"])),
        _chip("Receita Líquida", _fmt_brl(m["receita_liq"])),
        _chip("CMV", _fmt_brl(m["cmv"])),                 # <- chip CMV adicionado
        _chip("Total de Variáveis (R$)", _fmt_brl(m["total_var"])),
        _chip("Total de Saída Operacional (R$)", _fmt_brl(m["total_saida_oper"])),
        _chip("Lucro Bruto", _fmt_brl(m["lucro_bruto"])),
    ], "k-estrut"))

    cards_html.append(_card("Margens", [
        _chip("Margem Bruta", _fmt_pct(m["margem_bruta_pct"])),
        _chip("Margem Operacional", _fmt_pct(m["margem_operacional_pct"])),
        _chip("Margem Líquida", _fmt_pct(m["margem_liquida_pct"])),
        _chip_duo("Margem de Contribuição", m["margem_contrib"], m["margem_contrib_pct"],
                  help_key="Margem de Contribuição"),
    ], "k-margens"))

    cards_html.append(_card("Eficiência e Gestão", [
        _chip("Custo Fixo Mensal (R$)", _fmt_brl(fixas_rs)),
        _chip("Custo Fixo / Receita", _fmt_pct(m["custo_fixo_sobre_receita_pct"])),
        _chip_duo("Ponto de Equilíbrio (Contábil)", m["break_even_rs"], m["break_even_pct"],
                  help_key="Ponto de Equilíbrio (Contábil) (R$)"),
        _chip_duo("Ponto de Equilíbrio Financeiro", m["break_even_financeiro_rs"], m["break_even_financeiro_pct"],
                  help_key="Ponto de Equilíbrio Financeiro (R$)"),
        _chip("Margem de Segurança", _fmt_pct(m["margem_seguranca_pct"])),
        _chip("Eficiência Operacional", _fmt_pct(m["eficiencia_oper_pct"])),
        _chip("Relação Saídas/Entradas", _fmt_pct(m["rel_saida_entrada_pct"])),
    ], "k-efic"))

    cards_html.append(_card("Fluxo e Endividamento", [
        _chip_duo("Gasto c/ Empréstimos", m["emp"], m["emp_pct_sobre_receita"],
                  help_key="Gasto c/ Empréstimos (R$)"),
        _chip("Dívida (Estoque)", _fmt_brl(m["divida_estoque"])),
        _chip("Índice de Endividamento (%)", _fmt_pct(m["indice_endividamento_pct"])),
    ], "k-fluxo"))

    cards_html.append(_card("Crescimento e Vendas", [
        _chip("Ticket Médio", _fmt_brl(m["ticket_medio"])),
        _chip("Nº de Vendas", f"{int(m['n_vendas'])}"),
        _chip("Crescimento de Receita (m/m)", _fmt_pct(crec)),
    ], "k-cresc"))

    cards_html.append(_card("Avançados", [
        _chip("EBITDA", _fmt_brl(m["ebitda"])),
        _chip("EBIT", _fmt_brl(m["ebit"])),
        _chip("Lucro Líquido", _fmt_brl(m["lucro_liq"])),
        _chip("ROE", _fmt_pct(m["roe_pct"])),
        _chip("ROI", _fmt_pct(m["roi_pct"])),
        _chip("ROA", _fmt_pct(m["roa_pct"])),
    ], "k-avanc"))

    st.markdown('<div class="fd-grid">' + "".join(cards_html) + '</div>', unsafe_allow_html=True)

def _render_anual(db_path: str, ano: int, vars_dre: VarsDRE):
    st.caption(f"Cada mês mostra **Valores R$** e **Análise Vertical (%)** • Ano: **{ano}**")

    meses = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]

    CAT_BG = {
        "Estruturais": "rgba(46, 204, 113, 0.18)",
        "Margens": "rgba(52, 152, 219, 0.18)",
        "Eficiência e Gestão": "rgba(155, 89, 182, 0.18)",
        "Fluxo e Endividamento": "rgba(243, 156, 18, 0.20)",
        "Crescimento e Vendas": "rgba(26, 188, 156, 0.18)",
        "Avançados": "rgba(233, 30, 99, 0.18)",
        "Totais": "rgba(255, 77, 79, 0.20)",
    }
    CAT_BG_HEADER = {k: v.replace("0.18", "0.35").replace("0.20", "0.40") for k, v in CAT_BG.items()}

    rows_by_cat = {
        "Estruturais": [
            "Faturamento","Simples Nacional","Taxa Maquineta","Saída Imposto e Maquininha","Receita Líquida",
            "CMV (Mercadorias)","Total de Variáveis (R$)","Total de Saída Operacional (R$)","Lucro Bruto",
            "Fretes","Sacolas","Fundo de Promoção","Margem de Contribuição"
        ],
        "Margens": [
            "Margem Bruta (%)","Margem Operacional (%)","Margem Líquida (%)","Margem de Contribuição (%)"
        ],
        "Eficiência e Gestão": [
            "Custo Fixo Mensal","Ponto de Equilíbrio (Contábil)","Ponto de Equilíbrio Financeiro",
            "Margem de Segurança (%)","Eficiência Operacional (%)","Relação Saídas/Entradas (%)"
        ],
        "Fluxo e Endividamento": [
            "Gasto com Empréstimos/Financiamentos","Índice de Endividamento (%)"
        ],
        "Crescimento e Vendas": [
            "Ticket Médio","Crescimento de Receita (m/m) (%)"
        ],
        "Avançados": [
            "EBIT","EBITDA Lucro/Prejuízo","Lucro Líquido","ROE (%)","ROI (%)","ROA (%)"
        ],
        "Totais": [
            "Total CF + Empréstimos","Total de Saída"
        ],
    }

    # Garantir "Custo Fixo Mensal" na primeira posição da seção Eficiência e Gestão (idempotente)
    try:
        efic = rows_by_cat.get("Eficiência e Gestão", [])
        if isinstance(efic, list):
            efic = [item for item in efic if item != "Custo Fixo Mensal"]
            efic.insert(0, "Custo Fixo Mensal")
            rows_by_cat["Eficiência e Gestão"] = efic
    except Exception:
        pass

    cats_order = ["Estruturais","Margens","Eficiência e Gestão","Fluxo e Endividamento","Crescimento e Vendas","Avançados","Totais"]

    def _cat_header(cat: str) -> str:
        return f"◆ {cat}"

    # Garantir linha "Marketing" em Estruturais na grade anual (idempotente)
    try:
        estr = rows_by_cat.get("Estruturais", [])
        if isinstance(estr, list) and "Marketing" not in estr:
            for i, item in enumerate(estr):
                if "Fundo de Promo" in item:
                    estr.insert(i + 1, "Marketing")
                    break
    except Exception:
        pass

    ordered_rows: List[str] = []
    for cat in cats_order:
        ordered_rows.append(_cat_header(cat))
        ordered_rows.extend(rows_by_cat[cat])

    columns = pd.MultiIndex.from_product([meses, ["Valores R$", "Análise Vertical"]])
    df = pd.DataFrame(index=ordered_rows, columns=columns, dtype=object)

    for i, mes in enumerate(range(1, 12 + 1), start=0):
        pre_start = (ano < START_YEAR) or (ano == START_YEAR and mes < START_MONTH)
        m = _calc_mes(db_path, ano, mes, vars_dre)
        fat = m["fat"]
        fixas_rs = _safe(m.get("fixas"))

        try:
            crec_pct = _crescimento_mtd(db_path, ano, mes)
        except Exception:
            crec_pct = 0.0

        if pre_start:
            for r in ordered_rows:
                df.loc[r, (meses[i], "Valores R$")] = None
                df.loc[r, (meses[i], "Análise Vertical")] = None
            df.loc[ordered_rows[1], (meses[i], "Valores R$")] = fat
            df.loc[ordered_rows[1], (meses[i], "Análise Vertical")] = 100.0 if fat > 0 else 0.0
            continue

        vals = {
            "Faturamento": m["fat"],
            "Simples Nacional": m["simples"],
            "Taxa Maquineta": m["taxa_maq"],
            "Saída Imposto e Maquininha": m["saida_imp_maq"],
            "Receita Líquida": m["receita_liq"],
            "CMV (Mercadorias)": m["cmv"],          # <- tabela usa CMV corrigido
            "Total de Variáveis (R$)": m["total_var"],
            "Total de Saída Operacional (R$)": m["total_saida_oper"],
            "Lucro Bruto": m["lucro_bruto"],
            "Fretes": m["fretes"],
            "Sacolas": m["sacolas"],
            "Fundo de Promoção": m["fundo"],
            "Margem de Contribuição": m["margem_contrib"],

            "Custo Fixo Mensal": fixas_rs,
            "Gasto com Empréstimos/Financiamentos": m["emp"],
            "Marketing": m["mkt"],
            "Manutenção/Limpeza": m["limp"],
            "Total CF + Empréstimos": m["total_cf_emp"],
            "Total de Saída": m["total_saida_oper"],
            "Ponto de Equilíbrio (Contábil)": m["break_even_rs"],
            "Ponto de Equilíbrio Financeiro": m["break_even_financeiro_rs"],
            "Ticket Médio": m["ticket_medio"],

            "EBIT": m["ebit"],
            "EBITDA Lucro/Prejuízo": m["ebitda"],
            "Lucro Líquido": m["lucro_liq"],
        }

        overrides_pct = {
            "Margem Bruta (%)": m["margem_bruta_pct"],
            "Margem Operacional (%)": m["margem_operacional_pct"],
            "Margem Líquida (%)": m["margem_liquida_pct"],
            "Margem de Contribuição (%)": m["margem_contrib_pct"],
            "Ponto de Equilíbrio (Contábil)": m["break_even_pct"],
            "Ponto de Equilíbrio Financeiro": m["break_even_financeiro_pct"],
            "Margem de Segurança (%)": m["margem_seguranca_pct"],
            "Eficiência Operacional (%)": m["eficiencia_oper_pct"],
            "Relação Saídas/Entradas (%)": m["rel_saida_entrada_pct"],
            "Gasto com Empréstimos/Financiamentos": m["emp_pct_sobre_receita"],
            "Índice de Endividamento (%)": m["indice_endividamento_pct"],
            "Crescimento de Receita (m/m) (%)": crec_pct,
            "ROE (%)": m["roe_pct"],
            "ROI (%)": m["roi_pct"],
            "ROA (%)": m["roa_pct"],
        }

        for r in ordered_rows:
            if r.startswith("◆ "):
                df.loc[r, (meses[i], "Valores R$")] = None
                df.loc[r, (meses[i], "Análise Vertical")] = None
                continue

            if r == "Índice de Endividamento (%)":
                df.loc[r, (meses[i], "Valores R$")] = m["divida_estoque"]
            else:
                df.loc[r, (meses[i], "Valores R$")] = vals.get(r, None)

            if fat > 0:
                if r in overrides_pct:
                    df.loc[r, (meses[i], "Análise Vertical")] = overrides_pct[r]
                else:
                    v = vals.get(r, None)
                    df.loc[r, (meses[i], "Análise Vertical")] = (v / fat * 100.0) if isinstance(v, (int, float)) else None
            else:
                df.loc[r, (meses[i], "Análise Vertical")] = overrides_pct.get(r, 0.0 if vals.get(r) else None)

    def _fmt_val(v):
        if v is None:
            return "—"
        try:
            return _fmt_brl(float(v))
        except Exception:
            return "—"

    def _fmt_pct_cell(v):
        if v is None:
            return "—"
        try:
            return f"{float(v):.0f}%"
        except Exception:
            return "—"

    df_show = df.copy()
    for mes in meses:
        df_show[(mes, "Valores R$")] = df_show[(mes, "Valores R$")].map(_fmt_val)
        df_show[(mes, "Análise Vertical")] = df_show[(mes, "Análise Vertical")].map(_fmt_pct_cell)

    _KEY_ROWS = [
        "Faturamento","Receita Líquida","Saída Imposto e Maquininha",
        "Margem de Contribuição","Custo Fixo Mensal",
        "Ponto de Equilíbrio (Contábil)",
        "Ponto de Equilíbrio Financeiro",
        "Gasto com Empréstimos/Financiamentos",
        "EBIT","EBITDA Lucro/Prejuízo","Lucro Líquido",
        "Total de Saída Operacional (R$)","Total CF + Empréstimos","Total de Saída"
    ]

    styler = df_show.style
    CAT_BG = {
        "Estruturais": "rgba(46, 204, 113, 0.18)",
        "Margens": "rgba(52, 152, 219, 0.18)",
        "Eficiência e Gestão": "rgba(155, 89, 182, 0.18)",
        "Fluxo e Endividamento": "rgba(243, 156, 18, 0.20)",
        "Crescimento e Vendas": "rgba(26, 188, 156, 0.18)",
        "Avançados": "rgba(233, 30, 99, 0.18)",
        "Totais": "rgba(255, 77, 79, 0.20)",
    }
    CAT_BG_HEADER = {k: v.replace("0.18", "0.35").replace("0.20", "0.40") for k, v in CAT_BG.items()}

    cats_order = ["Estruturais","Margens","Eficiência e Gestão","Fluxo e Endividamento","Crescimento e Vendas","Avançados","Totais"]
    rows_by_cat = {
        "Estruturais": [
            "Faturamento","Simples Nacional","Taxa Maquineta","Saída Imposto e Maquininha","Receita Líquida",
            "CMV (Mercadorias)","Total de Variáveis (R$)","Total de Saída Operacional (R$)","Lucro Bruto",
            "Fretes","Sacolas","Fundo de Promoção","Margem de Contribuição"
        ],
        "Margens": [
            "Margem Bruta (%)","Margem Operacional (%)","Margem Líquida (%)","Margem de Contribuição (%)"
        ],
        "Eficiência e Gestão": [
            "Custo Fixo Mensal",
            "Ponto de Equilíbrio (Contábil)",
            "Ponto de Equilíbrio Financeiro",
            "Margem de Segurança (%)",
            "Eficiência Operacional (%)",
            "Relação Saídas/Entradas (%)"
        ],
        "Fluxo e Endividamento": [
            "Gasto com Empréstimos/Financiamentos","Índice de Endividamento (%)"
        ],
        "Crescimento e Vendas": [
            "Ticket Médio","Crescimento de Receita (m/m) (%)"
        ],
        "Avançados": [
            "EBIT","EBITDA Lucro/Prejuízo","Lucro Líquido","ROE (%)","ROI (%)","ROA (%)"
        ],
        "Totais": [
            "Total CF + Empréstimos","Total de Saída"
        ],
    }

    # Garantir linha "Marketing" em Estruturais na grade anual (styler) (idempotente)
    try:
        estr = rows_by_cat.get("Estruturais", [])
        if isinstance(estr, list) and "Marketing" not in estr:
            for i, item in enumerate(estr):
                if "Fundo de Promo" in item:
                    estr.insert(i + 1, "Marketing")
                    break
    except Exception:
        pass

    for cat in cats_order:
        header = f"◆ {cat}"
        styler = styler.set_properties(
            **{"background-color": CAT_BG_HEADER[cat], "font-weight": "900", "font-size": "1.02rem"},
            subset=pd.IndexSlice[[header], :]
        )

    for cat, linhas in rows_by_cat.items():
        if not linhas:
            continue
        styler = styler.set_properties(
            **{"background-color": CAT_BG[cat]},
            subset=pd.IndexSlice[linhas, :]
        )

    styler = styler.set_properties(
        **{"font-weight": "bold"},
        subset=pd.IndexSlice[_KEY_ROWS, :]
    )

    st.markdown("""
    <style>
    .cap-card.k-estrut{border-left:6px solid #2ecc71}
    .cap-card.k-estrut .cap-title-xl{color:#2ecc71}
    .cap-card.k-margens{border-left:6px solid #3498db}
    .cap-card.k-margens .cap-title-xl{color:#3498db}
    .cap-card.k-efic{border-left:6px solid #9b59b6}
    .cap-card.k-efic .cap-title-xl{color:#9b59b6}
    .cap-card.k-fluxo{border-left:6px solid #f39c12}
    .cap-card.k-fluxo .cap-title-xl{color:#f39c12}
    .cap-card.k-cresc{border-left:6px solid #1abc9c}
    .cap-card.k-cresc .cap-title-xl{color:#1abc9c}
    .cap-card.k-avanc{border-left:6px solid #e91e63}
    .cap-card.k-avanc .cap-title-xl{color:#e91e63}
    </style>
    """, unsafe_allow_html=True)

    rows_to_show = len(df_show.index)
    row_px = 32
    header_px = 96
    height_px = header_px + (rows_to_show + 1) * row_px

    st.dataframe(styler, use_container_width=True, height=height_px)

# Alias para retrocompatibilidade
pagina_dre = render_dre

#vamos ver se agora funciona
