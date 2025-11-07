# -*- coding: utf-8 -*-
# flowdash_pages/dre/dre.py
from __future__ import annotations

import sqlite3
from calendar import monthrange
from dataclasses import dataclass
from typing import Dict, Tuple, List, Iterable, Optional
import os

import pandas as pd
import streamlit as st
from datetime import date
from utils import formatar_moeda, formatar_percentual
import importlib

_avaliar_indicador_externo = None
try:
    _kpi_status_mod = importlib.import_module("utils.kpi_status")
    _avaliar_indicador_externo = getattr(_kpi_status_mod, "avaliar_indicador", None)
except Exception:
    _avaliar_indicador_externo = None

# ============================== Config de início do DRE ==============================
START_YEAR = 2025
START_MONTH = 10  # Outubro
KPI_TITLE = "KPIs"  # título exibido acima dos cards

FAIXAS_HELP = {
    "receita_liq_rb": "Receita Líquida (% da Receita Bruta): 🟢 Maior ou igual a 92% · 🟡 Entre 88% e 92% · 🔴 Menor que 88%",
    "cmv": "CMV (% da Receita Líquida): 🟢 Menor ou igual a 50% · 🟡 Entre 50% e 60% · 🔴 Maior que 60%",
    "total_var": "Total de Variáveis (% Receita Líquida): 🟢 Menor ou igual a 50% · 🟡 Entre 50% e 60% · 🔴 Maior que 60%",
    "total_saida_oper": "Total de Saída Operacional (% Receita Líquida): 🟢 Menor ou igual a 25% · 🟡 Entre 25% e 30% · 🔴 Maior que 30%",
    "lucro_bruto": "Lucro Bruto (%): 🔴 Menor que 45% · 🟡 Entre 45% e 50% · 🟢 Maior ou igual a 50%",
    "margem_bruta": "Margem Bruta (%): 🔴 Menor que 45% · 🟡 Entre 45% e 50% · 🟢 Maior ou igual a 50%",
    "margem_ebitda_pct": "Margem EBITDA (% da RL): 🟢 Maior ou igual a 10% · 🟡 Entre 5% e 10% · 🔴 Menor que 5%",
    "margem_operacional": "Margem Operacional (EBIT, %): 🔴 Menor que 5% · 🟡 Entre 5% e 10% · 🟢 Maior ou igual a 10%",
    "margem_liquida": "Margem Líquida (%): 🔴 Menor que 5% · 🟡 Entre 5% e 10% · 🟢 Maior ou igual a 10%",
    "margem_contribuicao": "Margem de Contribuição (%): 🔴 Menor que 35% · 🟡 Entre 35% e 45% · 🟢 Maior ou igual a 45%",
}

TOOLTIP_STRIP_HEADER_KEYS = {
    "Receita Líquida",
    "CMV",
    "Total de Variáveis (R$)",
    "Total de Saída Operacional (R$)",
    "Lucro Bruto",
    "Margem Bruta",
    "Margem Operacional",
    "Margem Líquida",
    "Margem de Contribuição",
    "EBITDA",
    "EBIT",
}

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
    if pref and isinstance(pref, str) and os.path.exists(pref):
        return pref
    try:
        for k in ("caminho_banco", "db_path"):
            v = st.session_state.get(k)
            if isinstance(v, str) and os.path.exists(v):
                return v
    except Exception:
        pass
    for p in (
        os.path.join("data", "flowdash_data.db"),
        os.path.join("data", "dashboard_rc.db"),
        "dashboard_rc.db",
        os.path.join("data", "flowdash_template.db"),
        "./flowdash_data.db",
    ):
        if os.path.exists(p):
            return p
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

def _escape_tooltip(text: Optional[str]) -> str:
    if not text:
        return ""
    escaped = (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
    return escaped.replace("\n", "<br/>")

def _safe(v) -> float:
    try:
        return float(v or 0)
    except Exception:
        return 0.0

def _nz_div(n: float, d: float) -> float:
    return (n / d) if d not in (0, None) else 0.0

def _mes_anterior(ano: int, mes: int) -> Tuple[int, int]:
    return (ano, mes - 1) if mes > 1 else (ano - 1, 12)

@dataclass
class _KPIStatusResult:
    emoji: str = "⚪"

_KPI_FAIXAS: Dict[str, List] = {
    "receita_liquida_sobre_bruta": [
        (lambda v: v is not None and v >= 92, "🟢"),
        (lambda v: v is not None and 88 <= v < 92, "🟡"),
        (lambda v: v is not None and v < 88, "🔴"),
    ],
    "cmv_percentual": [
        (lambda v: v is not None and v <= 50, "🟢"),
        (lambda v: v is not None and 50 < v <= 60, "🟡"),
        (lambda v: v is not None and v > 60, "🔴"),
    ],
    "total_variaveis_percentual": [
        (lambda v: v is not None and v <= 50, "🟢"),
        (lambda v: v is not None and 50 < v <= 60, "🟡"),
        (lambda v: v is not None and v > 60, "🔴"),
    ],
    "total_saida_oper_percentual": [
        (lambda v: v is not None and v <= 25, "🟢"),
        (lambda v: v is not None and 25 < v <= 30, "🟡"),
        (lambda v: v is not None and v > 30, "🔴"),
    ],
    "lucro_bruto": [
        (lambda v: v is not None and v >= 50, "🟢"),
        (lambda v: v is not None and 45 <= v < 50, "🟡"),
        (lambda v: v is not None and v < 45, "🔴"),
    ],
    "margem_bruta": [
        (lambda v: v is not None and v >= 50, "🟢"),
        (lambda v: v is not None and 45 <= v < 50, "🟡"),
        (lambda v: v is not None and v < 45, "🔴"),
    ],
    "margem_ebitda_pct": [
        (lambda v: v is not None and v >= 10, "🟢"),
        (lambda v: v is not None and 5 <= v < 10, "🟡"),
        (lambda v: v is not None and v < 5, "🔴"),
    ],
    "margem_operacional": [
        (lambda v: v is not None and v >= 10, "🟢"),
        (lambda v: v is not None and 5 <= v < 10, "🟡"),
        (lambda v: v is not None and v < 5, "🔴"),
    ],
    "margem_liquida": [
        (lambda v: v is not None and v >= 10, "🟢"),
        (lambda v: v is not None and 5 <= v < 10, "🟡"),
        (lambda v: v is not None and v < 5, "🔴"),
    ],
    "margem_contribuicao": [
        (lambda v: v is not None and v >= 45, "🟢"),
        (lambda v: v is not None and 35 <= v < 45, "🟡"),
        (lambda v: v is not None and v < 35, "🔴"),
    ],
}

def _strip_prefix_before_bullets(text: str) -> str:
    """Remove cabeçalho antes das bolinhas em tooltips específicos."""
    if not text:
        return text
    lines = text.splitlines()
    out = []
    for ln in lines:
        if ("🟢" in ln) or ("🟡" in ln) or ("🔴" in ln):
            idxs = [i for i in (ln.find("🟢"), ln.find("🟡"), ln.find("🔴")) if i != -1]
            cut = min(idxs) if idxs else -1
            out.append(ln[cut:].strip() if cut >= 0 else ln.strip())
        else:
            out.append(ln)
    return "\n".join(out)

def _avaliar_indicador_local(ind_key: str, valor=None, base=None) -> _KPIStatusResult:
    def _to_float(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    val = _to_float(valor)
    if ind_key == "lucro_bruto":
        if valor is None or base in (None, 0):
            val = None
        else:
            try:
                val = (float(valor) / float(base)) * 100.0
            except (TypeError, ValueError, ZeroDivisionError):
                val = None
    if val is None:
        return _KPIStatusResult()
    bands = _KPI_FAIXAS.get(ind_key)
    if not bands:
        return _KPIStatusResult()
    for check, emoji in bands:
        if check(val):
            return _KPIStatusResult(emoji=emoji)
    return _KPIStatusResult()

def _chip_status(ind_key: str, valor=None, base=None) -> str:
    if valor is None and (base is None or base == 0):
        return "⚪"
    if _avaliar_indicador_externo is not None:
        try:
            res = _avaliar_indicador_externo(ind_key, valor, base)
            if res is not None:
                emoji = getattr(res, "emoji", None)
                if emoji is None and isinstance(res, dict):
                    emoji = res.get("emoji")
                if emoji:
                    return emoji
        except Exception:
            pass
    return _avaliar_indicador_local(ind_key, valor, base).emoji

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
    except Exception:
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
            d = {r["chave"]: float(r["v"] or 0) for _, r in df.iterrows()}
    except Exception:
        pass

    return VarsDRE(
        simples=_safe(d.get("aliquota_simples_nacional")),
        markup=_safe(d.get("markup_medio")),
        sacolas=_safe(d.get("sacolas_percent")),
        fundo=_safe(d.get("fundo_promocao_percent")),
        dep_padrao=_safe(d.get("depreciacao_mensal_padrao")),
        pl_base=_safe(d.get("patrimonio_liquido_base")),
        inv_base=_safe(d.get("investimento_total_base")),
        atv_base=_safe(d.get("ativos_totais_base")),
    )

def _vars_dynamic_overrides(db_path: str, vars_dre: "VarsDRE") -> "VarsDRE":
    """Recalcula variáveis derivadas com base nos dados atuais, sem depender da tela de cadastro.

    - Ativos Totais (calc.) = Bancos+Caixa (consolidado) + Estoque atual (estimado) + Imobilizado (JSON)
    - PL (calc.) = max(0, Ativos Totais − Passivos Totais CAP)
    - Depreciação mensal padrão = Imobilizado × (taxa_dep% / 100)
    Mantém as variáveis de entrada (simples, markup, sacolas, fundo, investimento) como estão no DB.
    """
    try:
        from flowdash_pages.cadastros.variaveis_dre import (
            get_estoque_atual_estimado as _estoque_est,
            _get_total_consolidado_bancos_caixa as _bancos_total,
            _get_passivos_totais_cap as _cap_totais,
            _load_ui_prefs as _load_prefs,
        )
    except Exception:
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

        return VarsDRE(
            simples=vars_dre.simples,
            markup=vars_dre.markup,
            sacolas=vars_dre.sacolas,
            fundo=vars_dre.fundo,
            dep_padrao=dep_padrao,
            pl_base=pl_calc_nn,
            inv_base=vars_dre.inv_base,
            atv_base=ativos_totais,
        )
    except Exception:
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
    except Exception:
        pass

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
    try:
        with _conn(db_path) as c:
            row = c.execute(sql, (ini, fim)).fetchone()
            return _safe(row[0]), _safe(row[1]), int(row[2] or 0)
    except Exception:
        return 0.0, 0.0, 0

@st.cache_data(show_spinner=False, ttl=60)
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
        except Exception:
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

    return fixos_total + extras_total


@st.cache_data(show_spinner=False, ttl=60)
def _query_saidas_total(db_path: str, ini: str, fim: str,
                        categoria: str, subcat: str | None = None) -> float:
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

    try:
        total = _sum_with_sub("Sub_Categoria")
        if total == 0.0 and subcat:
            only = _sum_only_sub("Sub_Categoria")
            if only > 0.0:
                return only
        return total
    except Exception:
        pass

    try:
        total = _sum_with_sub("Sub_Categorias_saida")
        if total == 0.0 and subcat:
            only = _sum_only_sub("Sub_Categorias_saida")
            if only > 0.0:
                return only
        return total
    except Exception:
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
    try:
        with _conn(db_path) as c:
            row = c.execute(sql, (competencia,)).fetchone()
            return _safe(row[0])
    except Exception:
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
    try:
        with _conn(db_path) as c:
            row = c.execute(sql).fetchone()
            return _safe(row[0])
    except Exception:
        return 0.0

@st.cache_data(show_spinner=False, ttl=60)
def _query_mkt_cartao(db_path: str, ini: str, fim: str) -> float:
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
def _calc_mes(db_path: str, ano: int, mes: int, vars_dre: "VarsDRE") -> Dict[str, float]:
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
    total_cf_emprestimos = total_oper_fixo_extra + emp_rs
    total_saida_oper     = total_oper_fixo_extra + total_var

    # EBITDA base
    ebitda_base = margem_contrib - total_oper_fixo_extra

    # EBIT: apenas depreciação (não usamos amortização)
    dep_extra = vars_dre.dep_padrao # Depreciação
    ebit = ebitda_base - dep_extra # Cálculo do EBIT

    # Lucro líquido (simplificado)
    gasto_emprestimos = emp_rs
    lucro_liq = ebit - (gasto_emprestimos or 0)

    # KPIs
    rl = receita_liq
    mc_ratio = _nz_div(margem_contrib, rl)
    margem_ebitda_pct = _nz_div(ebitda_base, rl)

    break_even_rs = (total_oper_fixo_extra / mc_ratio) if mc_ratio > 0 else 0.0
    break_even_pct = _nz_div(break_even_rs, rl)

    break_even_financeiro_rs = (total_cf_emprestimos / mc_ratio) if mc_ratio > 0 else 0.0
    break_even_financeiro_pct = _nz_div(break_even_financeiro_rs, rl)

    margem_seguranca_pct = _nz_div((rl - break_even_rs), rl)

    eficiencia_oper_pct = _nz_div(total_oper_fixo_extra, rl)
    rel_saida_entrada_pct = _nz_div(total_oper_fixo_extra, fat)
    emp_pct_sobre_receita = _nz_div(emp_rs, rl) * 100.0

    ticket_medio = _nz_div(fat, float(n_vendas)) if n_vendas > 0 else 0.0

    margem_bruta_pct = _nz_div(lucro_bruto, rl)
    margem_operacional_pct = _nz_div(ebit, rl) # <-- Cálculo correto aqui
    margem_liquida_pct = _nz_div(lucro_liq, rl)
    margem_contrib_pct = _nz_div(margem_contrib, rl)
    custo_fixo_sobre_receita_pct = _nz_div(fixas_rs, rl)

    divida_estoque_rs = _query_divida_estoque(db_path)
    indice_endividamento_pct = (_nz_div(divida_estoque_rs, vars_dre.atv_base) * 100.0) if vars_dre.atv_base > 0 else 0.0

    return {
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
        "margem_ebitda_pct": margem_ebitda_pct * 100.0,
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

# ============================== UI / Página ==============================
def render_dre(caminho_banco: Optional[str]):
    caminho_banco = _ensure_db_path_or_raise(caminho_banco)
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

    st.subheader("KPIs - Indicadores-chave que medem o desempenho em relação às metas.")

    vars_dre = _load_vars(caminho_banco)
    # sobrescreve dinamicamente os derivados para refletirem o estado atual do banco
    vars_dre = _vars_dynamic_overrides(caminho_banco, vars_dre)
    # persiste no DB para manter consistência entre páginas
    _persist_overrides_to_db(caminho_banco, vars_dre)
    if vars_dre.markup <= 0:
        st.warning("⚠️ Markup médio não configurado (ou 0). CMV estimado será 0.")
    if all(v == 0 for v in (vars_dre.simples, vars_dre.fundo, vars_dre.sacolas)) and vars_dre.markup == 0:
        st.info("ℹ️ Configure em: Cadastros › Variáveis do DRE.")

    _render_kpis_mes_cards(caminho_banco, int(ano), int(mes), vars_dre)
    _render_anual(caminho_banco, int(ano), vars_dre)

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

/* "?" tooltip */
.fd-chip .qwrap{display:inline-flex;margin-left:2px;position:relative}
.fd-chip .qwrap .q{display:inline-flex;align-items:center;justify-content:center;width:16px;height:16px;border-radius:50%;background:#8a8a8a;color:#fff;font-size:12px;line-height:16px;border:none;cursor:pointer;outline:none}
.fd-chip .qwrap:focus .q,
.fd-chip .qwrap:focus-within .q,
.fd-chip .qwrap:hover .q{background:#9a9a9a}
.fd-chip .qwrap{outline:none}

.fd-chip .qwrap .tip{position:absolute;left:0;top:calc(100% + 8px);background:rgba(25,25,25,.98);color:#fff;border:1px solid rgba(255,255,255,0.08);border-radius:10px;box-shadow:0 6px 20px rgba(0,0,0,.28);padding:8px 10px;max-width:360px;min-width:240px;z-index:20;font-size:.86rem;visibility:hidden;opacity:0;transition:opacity .12s ease,transform .12s ease;transform:translateY(-4px)}
.fd-chip .qwrap:hover .tip,
.fd-chip .qwrap:focus .tip,
.fd-chip .qwrap:focus-within .tip{visibility:visible;opacity:1;transform:translateY(0)}
@media (prefers-reduced-motion:no-preference){
  .fd-chip .qwrap .tip{animation:fd-fade .12s ease}
}
@keyframes fd-fade{from{opacity:0;transform:translateY(-4px)}to{opacity:1;transform:translateY(0)}}

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
        "Total de Variáveis (R$)": "Soma dos custos variáveis: CMV (já inclui frete de compra), sacolas %, fundo de promoção % (+ comissão variável %, se existir). Não incluir Simples/taxas da maquininha se a Receita Líquida já estiver líquida dessas deduções.",
        "Total de Saída Operacional (R$)": "OPEX: despesas operacionais do mês (fixas + de operação), sem juros/IOF, CAPEX e depreciação. Base para EBITDA, eficiência e margens.",
        "Lucro Bruto": "Receita líquida menos o CMV. | Serve para: mostrar o ganho sobre as vendas antes das despesas operacionais (sinal da eficiência de compra e preço).",
        "Custo Fixo Mensal (R$)": "Soma das saídas classificadas como Custos Fixos no mês (aluguel, energia, internet etc.).",
        "Margem Bruta": "Quanto da receita líquida sobra após o CMV. | Serve para: medir a eficiência de precificação e compra — quanto sobra das vendas depois do CMV; base para avaliar se preço e custo estão saudáveis antes das despesas operacionais.",
        "Margem Bruta (%)": "Serve para: medir a eficiência de precificação e compra — quanto sobra das vendas depois do CMV; base para avaliar se preço e custo estão saudáveis antes das despesas operacionais.",
        "Margem Operacional": "Lucro operacional após depreciação e amortização. | Serve para: medir a rentabilidade das operações principais, mostrando quanto sobra de cada real vendido após todos os custos e despesas operacionais, antes de juros e impostos.",
        "Margem Líquida": "Lucro líquido dividido pela Receita Líquida. | Serve para: indicar a rentabilidade total do negócio, mostrando quanto sobra de cada real vendido depois de todos os custos, despesas operacionais, financeiras e tributos sobre o faturamento.",
        "Margem de Contribuição": "Receita Líquida − Total de Variáveis. Não inclua taxas de cartão/Simples aqui se a RL já veio líquida.",
        "Margem de Contribuição (%)": "Serve para: indicar quanto de cada R$ vendido sobra para pagar despesas fixas e gerar lucro depois de todos os custos variáveis (CMV, taxas de cartão, sacolas, fundo de promoção, comissões etc.); base do Ponto de Equilíbrio e decisões de preço.",
        "Margem de Contribuição (R$)": "Serve para: mostrar, em reais, quanto sobra das vendas após todos os custos variáveis; valor que efetivamente contribui para cobrir despesas fixas e lucro.",
        "Custo Fixo / Receita": "Peso dos custos fixos sobre a receita.",
        "Ponto de Equilíbrio (Contábil) (R$)": "Receita mínima para zerar o resultado considerando apenas custos fixos.",
        "Ponto de Equilíbrio Financeiro (R$)": "Receita mínima para o caixa não ficar negativo: (Custos Fixos + Empréstimos) ÷ Margem de Contribuição.",
        "Ponto de Equilíbrio (Contábil) (%)": "Percentual da receita líquida que representa o ponto de equilíbrio contábil.",
        "Ponto de Equilíbrio Financeiro (%)": "Percentual da receita líquida que representa o ponto de equilíbrio financeiro.",
        "Margem de Segurança": "Folga da receita acima do ponto de equilíbrio.",
        "Eficiência Operacional": "OPEX ÷ Receita Líquida — mede o peso das despesas operacionais sobre a receita; sem custos variáveis, juros/IOF, CAPEX e depreciação.",
        "Relação Saídas/Entradas": "Quanto do faturamento bruto é consumido pelas saídas operacionais.",
        "Gasto c/ Empréstimos (R$)": "Desembolso do mês com parcelas pagas.",
        "Gasto c/ Empréstimos (%)": "Gasto com empréstimos como % da receita líquida.",
        "Dívida (Estoque)": "Saldo devedor ainda em aberto.",
        "Índice de Endividamento (%)": "Quanto dos ativos está comprometido com dívidas.",
        "Ticket Médio": "Média de valor por venda.",
        "Nº de Vendas": "Quantidade de vendas no período.",
        "Crescimento de Receita (m/m)": "Variação do faturamento comparado ao mês anterior.",
        "EBITDA": "EBITDA (R$ | %RL) = MC - OPEX. %EBITDA = EBITDA / RL. Mede geração de caixa operacional antes de depreciação e efeitos financeiros.",
        "EBIT": "EBIT (R$ | %RL) = EBITDA - Depreciação. %EBIT = EBIT / RL (Margem Operacional). Exclui juros/IOF, CAPEX e impostos sobre lucro.",
        "Lucro Líquido": "Resultado final do período após despesas financeiras e impostos (quando aplicável). | Serve para: mostrar o quanto efetivamente sobrou no mês, após todos os custos, despesas e encargos. No Simples Nacional, representa o EBIT menos os gastos com empréstimos e juros.",
        "ROE": "Retorno do lucro sobre o patrimônio líquido.",
        "ROI": "Retorno do lucro sobre o investimento total.",
        "ROA": "Retorno do lucro sobre os ativos totais.",
    }

    def _build_tip_html(tip_text: str) -> str:
        if not tip_text:
            return ""
        return (f'<span class="qwrap" tabindex="0">'
                f'<span class="q">?</span>'
                f'<span class="tip">{tip_text}</span>'
                f'</span>')

    def _chip(lbl: str, val: str, status_emoji: Optional[str] = None,
              extra_tip: Optional[str] = None) -> str:
        tip = HELP.get(lbl, "")
        if extra_tip:
            tip = f"{tip}\n\n{extra_tip}" if tip else extra_tip
        if lbl in TOOLTIP_STRIP_HEADER_KEYS:
            tip = _strip_prefix_before_bullets(tip)
        lbl_display = f"{status_emoji} {lbl}" if status_emoji else lbl
        tip_html = _build_tip_html(_escape_tooltip(tip))
        if tip_html:
            return (f'<span class="fd-chip"><span class="k">{lbl_display}</span>'
                    f'<span class="v">{val}</span>{tip_html}</span>')
        return f'<span class="fd-chip"><span class="k">{lbl_display}</span><span class="v">{val}</span></span>'

    def _chip_duo(lbl: str, val_rs: float, val_pct: float, help_key: Optional[str] = None,
                  status_emoji: Optional[str] = None, extra_tip: Optional[str] = None) -> str:
        tip_key = help_key or lbl
        tip = HELP.get(tip_key, "")
        if extra_tip:
            tip = f"{tip}\n\n{extra_tip}" if tip else extra_tip
        if tip_key in TOOLTIP_STRIP_HEADER_KEYS:
            tip = _strip_prefix_before_bullets(tip)
        val_comb = f'{_fmt_brl(val_rs)} | (%) {_fmt_pct(val_pct)}'
        lbl_display = f"{status_emoji} {lbl}" if status_emoji else lbl
        tip_html = _build_tip_html(_escape_tooltip(tip))
        if tip_html:
            return (f'<span class="fd-chip"><span class="k">{lbl_display}</span>'
                    f'<span class="v">{val_comb}</span>{tip_html}</span>')
        return f'<span class="fd-chip"><span class="k">{lbl_display}</span><span class="v">{val_comb}</span></span>'

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

    receita_bruta = m.get("fat")
    receita_liq = m.get("receita_liq")
    cmv_rs = m.get("cmv")
    total_variaveis = m.get("total_var")
    lucro_bruto = m.get("lucro_bruto")
    total_saida_operacional = m.get("total_oper_fixo_extra")  # usar somente OPEX nos chips


    def _ratio(num, den):
        if den in (None, 0) or num is None:
            return None
        try:
            if pd.isna(num) or pd.isna(den):
                return None
        except Exception:
            pass
        try:
            num_f = float(num)
            den_f = float(den)
        except (TypeError, ValueError):
            return None
        if den_f == 0:
            return None
        return num_f / den_f

    perc_receita_liq = _ratio(receita_liq, receita_bruta)
    perc_cmv = _ratio(cmv_rs, receita_liq)
    perc_total_var = _ratio(total_variaveis, receita_liq)
    perc_lucro_bruto = _ratio(lucro_bruto, receita_liq)
    perc_total_saida_oper = _ratio(total_saida_operacional, receita_liq)
    if perc_total_saida_oper is not None:
        try:
            if float(receita_liq) <= 0:
                perc_total_saida_oper = None
        except (TypeError, ValueError):
            perc_total_saida_oper = None

    receita_liq_display = formatar_moeda(receita_liq)
    if perc_receita_liq is not None:
        receita_liq_display = (
            f"{receita_liq_display} | {formatar_percentual(perc_receita_liq, casas=1)} da Receita Bruta"
        )

    cmv_display = formatar_moeda(cmv_rs)
    if perc_cmv is not None:
        cmv_display = (
            f"{cmv_display} | {formatar_percentual(perc_cmv, casas=1)} da Receita Líquida"
        )

    total_var_display = formatar_moeda(total_variaveis)
    if perc_total_var is not None:
        total_var_display = (
            f"{total_var_display} | {formatar_percentual(perc_total_var, casas=1)} da Receita Líquida"
        )

    lucro_bruto_display = formatar_moeda(lucro_bruto)
    if perc_lucro_bruto is not None:
        lucro_bruto_display = (
            f"{lucro_bruto_display} | {formatar_percentual(perc_lucro_bruto, casas=1)} da Receita Líquida"
        )

    total_saida_oper_display = formatar_moeda(total_saida_operacional)
    if perc_total_saida_oper is not None:
        total_saida_oper_display = (
            f"{total_saida_oper_display} | {formatar_percentual(perc_total_saida_oper, casas=1)} da Receita Líquida"
        )

    perc_rl_rb_pct = (perc_receita_liq * 100.0) if perc_receita_liq is not None else None
    receita_liq_status = _chip_status("receita_liquida_sobre_bruta", perc_rl_rb_pct)
    receita_liq_tip_key = "receita_liq_rb"
    cmv_pct_val = (perc_cmv * 100.0) if perc_cmv is not None else None
    status_cmv = _chip_status("cmv_percentual", cmv_pct_val)
    total_var_pct_val = (perc_total_var * 100.0) if perc_total_var is not None else None
    status_total_var = _chip_status("total_variaveis_percentual", total_var_pct_val)
    total_saida_oper_pct_val = (perc_total_saida_oper * 100.0) if perc_total_saida_oper is not None else None
    status_total_saida_oper = _chip_status("total_saida_oper_percentual", total_saida_oper_pct_val)
    status_lucro_bruto = _chip_status("lucro_bruto", lucro_bruto, receita_liq)

    margem_bruta_pct_val = m.get("margem_bruta_pct")
    margem_ebitda_pct_val = m.get("margem_ebitda_pct")
    margem_operacional_pct_val = m.get("margem_operacional_pct")
    margem_liquida_pct_val = m.get("margem_liquida_pct")
    margem_contrib_pct_val = m.get("margem_contrib_pct")

    status_margem_bruta = _chip_status("margem_bruta", margem_bruta_pct_val)
    status_margem_ebitda = _chip_status("margem_ebitda_pct", margem_ebitda_pct_val)
    status_margem_operacional = _chip_status("margem_operacional", margem_operacional_pct_val)
    status_margem_liquida = _chip_status("margem_liquida", margem_liquida_pct_val)
    status_margem_contrib = _chip_status("margem_contribuicao", margem_contrib_pct_val)

    def _status_or_none(v: Optional[str]) -> Optional[str]:
        return v if v and v != "⚪" else None

    cards_html.append(_card("Estruturais", [
        _chip("Receita Bruta", _fmt_brl(m["fat"])),
        _chip("Receita Líquida", receita_liq_display, status_emoji=_status_or_none(receita_liq_status),
              extra_tip=FAIXAS_HELP[receita_liq_tip_key]),
        _chip("CMV", cmv_display, status_emoji=_status_or_none(status_cmv),
              extra_tip=FAIXAS_HELP["cmv"]),                 # <- chip CMV adicionado
        _chip("Total de Variáveis (R$)", total_var_display,
              status_emoji=_status_or_none(status_total_var),
              extra_tip=FAIXAS_HELP["total_var"]),
        _chip("Total de Saída Operacional (R$)", total_saida_oper_display,
              status_emoji=_status_or_none(status_total_saida_oper),
              extra_tip=FAIXAS_HELP["total_saida_oper"]),
        _chip("Lucro Bruto", lucro_bruto_display,
              status_emoji=_status_or_none(status_lucro_bruto),
              extra_tip=FAIXAS_HELP["lucro_bruto"]),
    ], "k-estrut"))

    cards_html.append(_card("Margens", [
        _chip("Margem Bruta", _fmt_pct(margem_bruta_pct_val),
              status_emoji=_status_or_none(status_margem_bruta),
              extra_tip=FAIXAS_HELP["margem_bruta"]),
        _chip("Margem Operacional", _fmt_pct(margem_operacional_pct_val),
              status_emoji=_status_or_none(status_margem_operacional),
              extra_tip=FAIXAS_HELP["margem_operacional"]),
        _chip("Margem Líquida", _fmt_pct(margem_liquida_pct_val),
              status_emoji=_status_or_none(status_margem_liquida),
              extra_tip=FAIXAS_HELP["margem_liquida"]),
        _chip_duo("Margem de Contribuição", m["margem_contrib"], m["margem_contrib_pct"],
                  help_key="Margem de Contribuição",
                  status_emoji=_status_or_none(status_margem_contrib),
                  extra_tip=FAIXAS_HELP["margem_contribuicao"]),
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

    margem_ebitda_ratio = (margem_ebitda_pct_val / 100.0) if margem_ebitda_pct_val is not None else None
    margem_operacional_ratio = (margem_operacional_pct_val / 100.0) if margem_operacional_pct_val is not None else None

    ebitda_display = _fmt_brl(m["ebitda"])
    if margem_ebitda_ratio is not None:
        ebitda_display = f"{ebitda_display} | {formatar_percentual(margem_ebitda_ratio, casas=1)} da RL"

    ebit_display = _fmt_brl(m["ebit"])
    if margem_operacional_ratio is not None:
        ebit_display = f"{ebit_display} | {formatar_percentual(margem_operacional_ratio, casas=1)} da RL"

    cards_html.append(_card("Avançados", [
        _chip("EBITDA", ebitda_display,
              status_emoji=_status_or_none(status_margem_ebitda),
              extra_tip=FAIXAS_HELP["margem_ebitda_pct"]),
        _chip("EBIT", ebit_display,
              status_emoji=_status_or_none(status_margem_operacional),
              extra_tip=FAIXAS_HELP["margem_operacional"]),
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
            "Total de Saída Operacional (R$)": m["total_oper_fixo_extra"],
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
### corrigindo commit anterior que removeu o alias acima
