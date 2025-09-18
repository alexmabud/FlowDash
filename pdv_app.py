# -*- coding: utf-8 -*-
"""
FlowDash — PDV Kiosk (login normal + PIN somente na venda)

Uso local:
    streamlit run pdv_app.py
"""

from __future__ import annotations

import hmac
import importlib
import os
import sqlite3
import sys
from datetime import date, timedelta
from types import SimpleNamespace
from typing import Dict, List, Optional, Tuple

import plotly.graph_objects as go
import streamlit as st
from utils.pin_utils import validar_pin

# ---------------------------------------------------------------------------
# Path/bootstrap
# ---------------------------------------------------------------------------

import pathlib

_CURR_DIR = pathlib.Path(__file__).resolve().parent
if str(_CURR_DIR) not in sys.path:
    sys.path.insert(0, str(_CURR_DIR))

ABS_DB = r"C:\Users\User\OneDrive\Documentos\Python\FlowDash\data\flowdash_data.db"
REL_DB = str(_CURR_DIR / "data" / "flowdash_data.db")
TEMPLATE_DB = str(_CURR_DIR / "data" / "flowdash_template.db")


def _ensure_db() -> str:
    """Garante FLOWDASH_DB e copia o template se necessário; retorna caminho do DB."""
    db = (os.environ.get("FLOWDASH_DB") or "").strip() or (ABS_DB if os.path.exists(ABS_DB) else REL_DB)
    os.makedirs(os.path.dirname(db), exist_ok=True)
    if (not os.path.exists(db)) and os.path.exists(TEMPLATE_DB):
        import shutil

        shutil.copyfile(TEMPLATE_DB, db)
    os.environ["FLOWDASH_DB"] = db
    return db


DB_PATH = _ensure_db()

# ---------------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------------

try:
    from auth import validar_login as auth_validar_login  # type: ignore
except Exception:
    try:
        from auth.auth import validar_login as auth_validar_login  # type: ignore
    except Exception:

        def auth_validar_login(email: str, senha: str, caminho_banco: Optional[str] = None) -> Optional[dict]:
            """Fallback de login direto no SQLite (apenas se módulo auth indisponível)."""
            from utils.utils import gerar_hash_senha

            senha_hash = gerar_hash_senha(senha)
            caminho_banco = caminho_banco or os.environ.get("FLOWDASH_DB") or ""
            with sqlite3.connect(caminho_banco) as conn:
                row = conn.execute(
                    "SELECT id, nome, email, perfil FROM usuarios WHERE email=? AND senha=? AND ativo=1",
                    (email, senha_hash),
                ).fetchone()
            return {"id": row[0], "nome": row[1], "email": row[2], "perfil": row[3]} if row else None


# ---------------------------------------------------------------------------
# UI base
# ---------------------------------------------------------------------------

st.set_page_config(page_title="FlowDash PDV", layout="wide", initial_sidebar_state="collapsed")
st.markdown(
    """
    <style>
    [data-testid="stSidebar"] { display: none !important; }
    header, footer {visibility: hidden;}
    .block-container {padding-top: 2rem; padding-bottom: 2rem; max-width: 1200px;}
    button[data-testid="baseButton-secondary"]{
        background: transparent !important; border: none !important; color: #64B5F6 !important;
        padding: 0 !important; box-shadow: none !important; min-width: auto !important;
    }
    button[data-testid="baseButton-secondary"]:hover{ text-decoration: underline; background: transparent !important; }
    .nv-wrap button { width: 100% !important; padding: 14px 22px !important; font-size: 1.08rem !important; border-radius: 12px !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Helpers DB / sessão
# ---------------------------------------------------------------------------


def _conn() -> sqlite3.Connection:
    """Abre conexão SQLite usando FLOWDASH_DB; aborta com mensagem se não existir."""
    db = (os.environ.get("FLOWDASH_DB") or "").strip()
    if not db or not os.path.exists(db):
        st.error(f"❌ Banco de dados não encontrado em: `{db or '(vazio)'}`")
        st.stop()
    conn = sqlite3.connect(db, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    """Retorna True se a tabela existir (case-insensitive)."""
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND lower(name)=lower(?) LIMIT 1;", (name,)
        ).fetchone()
        is not None
    )


@st.cache_data(show_spinner=False, ttl=30)
def _listar_usuarios_ativos_sem_pdv() -> List[Tuple[int, str, str]]:
    """Lista usuários ativos que não têm perfil PDV (id, nome, perfil)."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT id, nome, perfil FROM usuarios "
            "WHERE ativo = 1 AND (perfil IS NULL OR perfil <> ?) ORDER BY nome ASC",
            ("PDV",),
        ).fetchall()
    return [(int(r["id"]), str(r["nome"]), str(r["perfil"] or "")) for r in rows]


def _buscar_pin_usuario(usuario_id: int) -> Optional[str]:
    """Retorna PIN do usuário ativo ou None."""
    with _conn() as conn:
        row = conn.execute("SELECT pin FROM usuarios WHERE id = ? AND ativo = 1", (usuario_id,)).fetchone()
    return None if not row else row["pin"]


def _inc_tentativa(usuario_id: int) -> int:
    """Incrementa contador de tentativas de PIN por usuário na sessão."""
    key = "pdv_pin_tentativas"
    st.session_state.setdefault(key, {})
    st.session_state[key][usuario_id] = st.session_state[key].get(usuario_id, 0) + 1
    return st.session_state[key][usuario_id]


def _reset_tentativas(usuario_id: int) -> None:
    """Zera tentativas de PIN para o usuário na sessão."""
    key = "pdv_pin_tentativas"
    if key in st.session_state and usuario_id in st.session_state[key]:
        st.session_state[key][usuario_id] = 0


@st.cache_data(show_spinner=False, ttl=30)
def _entrada_date_bounds() -> Tuple[date, date]:
    """Faixa de datas (min, max) presentes em `entrada`; se vazia, (hoje, hoje)."""
    today = date.today()
    try:
        with _conn() as conn:
            if not _table_exists(conn, "entrada"):
                return (today, today)
            row = conn.execute("SELECT MIN(date(Data)), MAX(date(Data)) FROM entrada").fetchone()
            if not row or not row[0]:
                return (today, today)
            from datetime import datetime

            dmin = datetime.strptime(row[0], "%Y-%m-%d").date()
            dmax = datetime.strptime(row[1], "%Y-%m-%d").date() if row[1] else today
            return (dmin, dmax)
    except Exception:
        return (today, today)


# ---------------------------------------------------------------------------
# Metas / visualizações
# ---------------------------------------------------------------------------


def _fmt_moeda(v: float) -> str:
    """Formata número como moeda BR (R$)."""
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _gauge_percentual_zonas(
    titulo: str,
    percentual: float,
    bronze_pct: float,
    prata_pct: float,
    axis_max: float = 120.0,
    bar_color_rgba: str = "rgba(76,175,80,0.85)",
    valor_label: Optional[str] = None,
) -> go.Figure:
    """Cria gauge com zonas Bronze/Prata/Ouro (+120%)."""
    bronze = max(0.0, min(100.0, float(bronze_pct)))
    prata = max(bronze, min(100.0, float(prata_pct)))
    max_axis = max(100.0, float(axis_max))
    value = float(max(0.0, min(max_axis, percentual)))
    steps = [
        {"range": [0, bronze], "color": "#E53935"},
        {"range": [bronze, prata], "color": "#CD7F32"},
        {"range": [prata, 100], "color": "#C0C0C0"},
        {"range": [100, max_axis], "color": "#FFD700"},
    ]
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=value,
            number={"suffix": "%"},
            title={"text": titulo, "font": {"size": 18}},
            gauge={
                "shape": "angular",
                "axis": {"range": [0, max_axis]},
                "bgcolor": "rgba(0,0,0,0)",
                "bar": {"color": bar_color_rgba},
                "steps": steps,
                "borderwidth": 0,
            },
        )
    )
    if valor_label:
        fig.add_annotation(
            x=0.5,
            y=0.06,
            xref="paper",
            yref="paper",
            text=f"<span style='font-size:12px;color:#B0BEC5'>{valor_label}</span>",
            showarrow=False,
            align="center",
        )
    fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=260)
    return fig


def _card_periodo_html(titulo: str, ouro: float, prata: float, bronze: float, acumulado: float) -> str:
    """Retorna HTML do card de metas (ouro/prata/bronze) para um período."""
    def _linha(nivel: str, meta: float) -> str:
        falta = max(float(meta) - float(acumulado), 0.0)
        falta_txt = f"<span style='color:#00C853'>✅ {_fmt_moeda(0)}</span>" if falta <= 0.00001 else _fmt_moeda(falta)
        return (
            "<tr>"
            f"<td style='padding:10px 8px;color:#ECEFF1;font-weight:600;'>{nivel}</td>"
            f"<td style='padding:10px 8px;color:#B0BEC5;text-align:right;'>{_fmt_moeda(meta)}</td>"
            f"<td style='padding:10px 8px;color:#B0BEC5;text-align:right;'>{falta_txt}</td>"
            "</tr>"
        )

    return f"""
    <div style='border:1px solid #333; border-radius:12px; padding:12px; background-color:#121212;'>
      <div style='font-weight:700; color:#B0BEC5; margin-bottom:8px;'>📅 {titulo}</div>
      <table style='width:100%; border-collapse:collapse;'>
        <thead>
          <tr>
            <th style='text-align:left; padding:6px; color:#90A4AE; font-weight:600;'>Nível</th>
            <th style='text-align:right; padding:6px; color:#90A4AE; font-weight:600;'>Meta</th>
            <th style='text-align:right; padding:6px; color:#90A4AE; font-weight:600;'>Falta</th>
          </tr>
        </thead>
        <tbody>
          {_linha("🥇 Ouro", ouro)}
          {_linha("🥈 Prata", prata)}
          {_linha("🥉 Bronze", bronze)}
        </tbody>
      </table>
    </div>
    """


def _inicio_semana(dt: date) -> date:
    """Retorna a segunda-feira da semana de dt."""
    return dt - timedelta(days=dt.weekday())


def _col_dow(dt: date) -> str:
    """Retorna o nome da coluna de metas por dia da semana."""
    return ["segunda", "terca", "quarta", "quinta", "sexta", "sabado", "domingo"][dt.weekday()]


def _metas_loja_gauges(ref_day: date) -> None:
    """Renderiza gauges e cards de metas para Dia/Semana/Mês na data de referência."""
    ym = f"{ref_day.year:04d}-{ref_day.month:02d}"
    inicio_sem = _inicio_semana(ref_day)
    inicio_mes = ref_day.replace(day=1)
    dow_col = _col_dow(ref_day)

    meta_mensal = 0.0
    perc_prata = 87.5
    perc_bronze = 75.0
    perc_semanal = 25.0
    perc_dow = None

    with _conn() as conn:
        if _table_exists(conn, "metas"):
            cols = [r[1] for r in conn.execute("PRAGMA table_info('metas')")]
            has_mes = any(c.lower() == "mes" for c in cols)
            sql = (
                "SELECT meta_mensal, COALESCE(perc_prata,87.5), COALESCE(perc_bronze,75.0), "
                "COALESCE(perc_semanal,25.0) FROM metas "
                + ("WHERE mes=? " if has_mes else "")
                + "ORDER BY rowid DESC LIMIT 1"
            )
            row = conn.execute(sql, (ym,) if has_mes else ()).fetchone()
            if row:
                meta_mensal = float(row[0] or 0.0)
                perc_prata = float(row[1] or 87.5)
                perc_bronze = float(row[2] or 75.0)
                perc_semanal = float(row[3] or 25.0)

            col_map = {
                "segunda": "perc_segunda",
                "terca": "perc_terca",
                "quarta": "perc_quarta",
                "quinta": "perc_quinta",
                "sexta": "perc_sexta",
                "sabado": "perc_sabado",
                "domingo": "perc_domingo",
            }
            perc_col = col_map.get(dow_col, "")
            lower_cols = [c.lower() for c in cols]
            if perc_col and perc_col.lower() in lower_cols:
                perc_dow = float(
                    conn.execute(f"SELECT COALESCE({perc_col}, 0.0) FROM metas ORDER BY rowid DESC LIMIT 1").fetchone()[
                        0
                    ]
                    or 0.0
                )

        if not _table_exists(conn, "entrada"):
            st.error("Tabela `entrada` não encontrada.")
            return

        def sum_between(d1: date, d2: date) -> float:
            return float(
                conn.execute(
                    """
                    SELECT COALESCE(SUM(CAST(Valor AS REAL)), 0.0)
                      FROM entrada
                     WHERE date(Data) >= date(?)
                       AND date(Data) <= date(?)
                    """,
                    (d1.isoformat(), d2.isoformat()),
                ).fetchone()[0]
                or 0.0
            )

        vendido_dia = sum_between(ref_day, ref_day)
        vendido_sem = sum_between(inicio_sem, ref_day)
        vendido_mes = sum_between(inicio_mes, ref_day)

    meta_sem = meta_mensal * (perc_semanal / 100.0) if meta_mensal > 0 else 0.0
    meta_dia = (meta_sem / 7.0) if not perc_dow or perc_dow <= 0 else (meta_sem * (perc_dow / 100.0))

    ouro_m, prata_m, bronze_m = meta_mensal, meta_mensal * (perc_prata / 100.0), meta_mensal * (perc_bronze / 100.0)
    ouro_s, prata_s, bronze_s = meta_sem, meta_sem * (perc_prata / 100.0), meta_sem * (perc_bronze / 100.0)
    ouro_d, prata_d, bronze_d = meta_dia, meta_dia * (perc_prata / 100.0), meta_dia * (perc_bronze / 100.0)

    def pct(v: float, m: float) -> float:
        return 0.0 if not m or m <= 0 else min(120.0, round((v / m) * 100.0, 1))

    p_dia, p_sem, p_mes = pct(vendido_dia, meta_dia), pct(vendido_sem, meta_sem), pct(vendido_mes, meta_mensal)
    bronze_pct = 75.0 if ouro_m <= 0 else round(100.0 * (bronze_m / ouro_m), 1)
    prata_pct = 87.5 if ouro_m <= 0 else round(100.0 * (prata_m / ouro_m), 1)

    st.markdown("### 🏪 LOJA")
    c1, c2, c3 = st.columns(3)
    c1.plotly_chart(
        _gauge_percentual_zonas("Meta do Dia", p_dia, bronze_pct, prata_pct, valor_label=_fmt_moeda(vendido_dia)),
        use_container_width=True,
        key=f"g_dia_{ym}_{ref_day}",
    )
    c2.plotly_chart(
        _gauge_percentual_zonas("Meta da Semana", p_sem, bronze_pct, prata_pct, valor_label=_fmt_moeda(vendido_sem)),
        use_container_width=True,
        key=f"g_sem_{ym}_{ref_day}",
    )
    c3.plotly_chart(
        _gauge_percentual_zonas("Meta do Mês", p_mes, bronze_pct, prata_pct, valor_label=_fmt_moeda(vendido_mes)),
        use_container_width=True,
        key=f"g_mes_{ym}_{ref_day}",
    )

    st.divider()
    t1, t2, t3 = st.columns(3)
    with t1:
        st.markdown(_card_periodo_html("Dia", ouro_d, prata_d, bronze_d, vendido_dia), unsafe_allow_html=True)
    with t2:
        st.markdown(_card_periodo_html("Semana", ouro_s, prata_s, bronze_s, vendido_sem), unsafe_allow_html=True)
    with t3:
        st.markdown(_card_periodo_html("Mês", ouro_m, prata_m, bronze_m, vendido_mes), unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Venda (reuso do seu módulo)
# ---------------------------------------------------------------------------


def _render_form_venda(vendedor: Dict[str, object]) -> None:
    """Renderiza o formulário de venda no contexto do PDV (vendedor mascarado)."""
    st.markdown("## 🧾 Nova Venda")

    # Garantir ENV para módulos que usam FLOWDASH_DB internamente
    os.environ["FLOWDASH_DB"] = DB_PATH
    if not os.path.exists(DB_PATH):
        st.error(f"❌ DB não existe no caminho esperado:\n`{DB_PATH}`")
        st.stop()

    # Máscara de usuário: a partir daqui, o vendedor passa a ser o "usuario_logado" durante a venda
    if "pdv_original_user" not in st.session_state and "usuario_logado" in st.session_state:
        st.session_state["pdv_original_user"] = st.session_state["usuario_logado"]
        st.session_state["pdv_header_user"] = st.session_state["pdv_original_user"]  # cabeçalho mostra o real

    st.session_state["usuario_logado"] = {
        "id": vendedor["id"],
        "nome": vendedor["nome"],
        "email": f"vendedor_{vendedor['id']}@pdv.local",
        "perfil": vendedor.get("perfil") or "Vendedor",
    }

    # Contexto PDV
    st.session_state["pdv_context"] = {
        "vendedor_id": vendedor["id"],
        "vendedor_nome": vendedor["nome"],
        "origem": "PDV",
    }

    # Chamada da página de venda
    try:
        mod = importlib.import_module("flowdash_pages.lancamentos.venda.page_venda")
        render_venda = getattr(mod, "render_venda", None)
        if not callable(render_venda):
            raise ImportError("Função render_venda(state) não encontrada em page_venda.py")

        ref_day = st.session_state.get("pdv_ref_date", date.today())
        state = SimpleNamespace(caminho_banco=DB_PATH, db_path=DB_PATH, data_lanc=ref_day)
        render_venda(state)
    except Exception as e:
        with st.container(border=True):
            st.error("❌ Erro ao abrir o formulário de venda.")
            st.caption(f"Detalhe técnico: {e}")


# ---------------------------------------------------------------------------
# Login / seleção de vendedor
# ---------------------------------------------------------------------------


def _login_box() -> bool:
    """Formulário de login do PDV; retorna True se autenticado."""
    st.markdown("### 🔐 Login do PDV")
    with st.form("form_login_pdv", clear_on_submit=False):
        email = st.text_input("Email", max_chars=100)
        senha = st.text_input("Senha", type="password", max_chars=50)
        ok = st.form_submit_button("Entrar")
    if ok:
        user = auth_validar_login(email, senha, DB_PATH)
        if user:
            st.session_state["usuario_logado"] = user
            st.success(f"Bem-vindo, {user['nome']}!")
            st.rerun()
        else:
            st.error("Credenciais inválidas ou usuário inativo.")
    return bool(st.session_state.get("usuario_logado"))


def _selecionar_vendedor_e_validar_pin() -> Optional[Dict]:
    """Fluxo de seleção do vendedor + validação de PIN; retorna dict do vendedor ou None."""
    st.markdown("#### 👤 Vendedor da Venda")
    usuarios = _listar_usuarios_ativos_sem_pdv()
    if not usuarios:
        st.warning("Nenhum usuário ativo encontrado. Cadastre em **Cadastros › Usuários**.")
        return None

    labels, label_to_id = [], {}
    for uid, nome, perfil in usuarios:
        label = f"{nome} — {perfil or 'Sem perfil'}"
        labels.append(label)
        label_to_id[label] = uid

    c_vend, c_pin, c_btn = st.columns([0.55, 0.25, 0.20])
    with c_vend:
        escolha = st.selectbox("Vendedor", labels, key="pdv_sel_vendedor")
    with c_pin:
        pin_in = st.text_input("PIN (4 dígitos)", type="password", max_chars=4, key="pdv_pin_vendedor")
    with c_btn:
        st.write("")
        confirmar = st.button("Confirmar", key="btn_confirma_pin", use_container_width=True)

    if not confirmar:
        return None

    usuario_id = label_to_id[escolha]
    if _inc_tentativa(usuario_id) > 5:
        st.error("Muitas tentativas. Troque o vendedor ou tente novamente mais tarde.")
        return None

    try:
        pin_digitado = validar_pin(pin_in)
    except ValueError as e:
        st.error(str(e))
        return None
    if pin_digitado is None:
        st.error("Informe o PIN de 4 dígitos para prosseguir.")
        return None

    pin_db = _buscar_pin_usuario(usuario_id)
    if pin_db is None:
        st.error("Este usuário não possui PIN cadastrado. Defina um PIN na página de Usuários.")
        return None

    if hmac.compare_digest(str(pin_db), pin_digitado):
        nome_sel = next(n for (uid, n, _) in usuarios if uid == usuario_id)
        perfil_sel = next(p for (uid, _, p) in usuarios if uid == usuario_id)
        _reset_tentativas(usuario_id)
        return {"id": usuario_id, "nome": nome_sel, "perfil": perfil_sel}

    st.error("PIN incorreto.")
    return None


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------


def main() -> None:
    """Entrada principal do PDV (dashboard + fluxo de venda)."""
    # Banner global de sucesso (fora de formulários)
    _flash = None
    for k in ("pdv_flash_ok", "msg_ok", "flash_ok"):
        if not _flash:
            _flash = st.session_state.pop(k, None)
    if _flash:
        st.success(_flash)

    st.markdown("# 🧾 FlowDash — PDV")

    if not st.session_state.get("usuario_logado"):
        if not _login_box():
            return
    else:
        # Cabeçalho mostra SEMPRE o usuário real (não o vendedor mascarado)
        header_user = st.session_state.get("pdv_header_user", st.session_state["usuario_logado"])
        dmin, dmax = _entrada_date_bounds()
        today = date.today()
        dmax = max(dmax, today)

        c_left, c_right = st.columns([1, 0.6])
        with c_left:
            st.markdown(
                f"""
                <div style="margin-top:-0.5rem; color:#90A4AE; font-size:0.95rem;">
                  👤 <strong>{header_user['nome']}</strong> — <code>{header_user['perfil']}</code>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with c_right:
            dd, logout = st.columns([0.7, 0.3])
            with dd:
                st.date_input(
                    "📅 Data de referência",
                    value=st.session_state.get("pdv_ref_date", today),
                    min_value=dmin,
                    max_value=dmax,
                    format="YYYY/MM/DD",
                    key="pdv_ref_date",
                )
            with logout:
                st.write("")
                if st.button("Sair", key="btn_logout", type="secondary"):
                    # Restaura usuário original (se estava mascarado)
                    if "pdv_original_user" in st.session_state:
                        st.session_state["usuario_logado"] = st.session_state.pop("pdv_original_user")
                    st.session_state.pop("pdv_header_user", None)
                    for k in ("pdv_mostrar_form", "pdv_vendedor_venda", "pdv_context"):
                        st.session_state.pop(k, None)
                    st.rerun()

    # Metas (gauges + cards) na data escolhida
    ref_day = st.session_state.get("pdv_ref_date", date.today())
    st.markdown(f"**Metas do dia — {ref_day:%Y-%m-%d}**")
    _metas_loja_gauges(ref_day)
    st.divider()

    # Fluxo de nova venda
    if not st.session_state.get("pdv_mostrar_form"):
        st.markdown('<div class="nv-wrap">', unsafe_allow_html=True)
        if st.button("➕ Nova Venda", key="btn_nova_venda", use_container_width=True):
            st.session_state["pdv_mostrar_form"] = True
            st.session_state.pop("pdv_vendedor_venda", None)
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
        return

    vendedor = st.session_state.get("pdv_vendedor_venda")
    if not vendedor:
        vendedor = _selecionar_vendedor_e_validar_pin()
        if vendedor:
            st.session_state["pdv_vendedor_venda"] = vendedor
            st.session_state["pdv_flash_ok"] = f"Vendedor **{vendedor['nome']}** identificado para a venda."
            st.rerun()
        else:
            return

    _render_form_venda(vendedor)

    col_a, col_b = st.columns([1, 1])
    with col_a:
        if st.button("🔁 Trocar vendedor desta venda", key="btn_trocar_vend", use_container_width=True):
            if "pdv_original_user" in st.session_state:
                st.session_state["usuario_logado"] = st.session_state["pdv_original_user"]
            st.session_state.pop("pdv_vendedor_venda", None)
            st.rerun()
    with col_b:
        if st.button("❌ Cancelar venda", key="btn_cancelar_venda", use_container_width=True):
            if "pdv_original_user" in st.session_state:
                st.session_state["usuario_logado"] = st.session_state.pop("pdv_original_user")
            st.session_state.pop("pdv_header_user", None)
            for k in ("pdv_mostrar_form", "pdv_vendedor_venda", "pdv_context"):
                st.session_state.pop(k, None)
            st.session_state["pdv_flash_ok"] = "Venda cancelada."
            st.rerun()


if __name__ == "__main__":
    main()
