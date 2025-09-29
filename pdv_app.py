# -*- coding: utf-8 -*-
"""
FlowDash — PDV Kiosk
====================
Login normal + PIN somente na venda. Otimizado para navegação rápida.
"""
from __future__ import annotations

import hmac
import importlib
import os
import pathlib
import shutil
import sqlite3
import sys
from datetime import date, timedelta, datetime, timezone
from types import SimpleNamespace
from typing import Dict, List, Optional, Tuple, Callable

import plotly.graph_objects as go
import streamlit as st
import pandas as pd

from utils.pin_utils import validar_pin
from shared.branding import sidebar_brand, page_header, login_brand
from shared.db_from_dropbox_api import ensure_local_db_api
from shared.dropbox_config import load_dropbox_settings, mask_token  # noqa: F401
from shared.dbx_io import enviar_db_local, baixar_db_para_local
from shared.dropbox_client import get_dbx, download_bytes

# ------------------------- Config inicial -------------------------
st.set_page_config(page_title="FlowDash PDV", layout="wide")

_CURR_DIR = pathlib.Path(__file__).resolve().parent
if str(_CURR_DIR) not in sys.path:
    sys.path.insert(0, str(_CURR_DIR))

_LOGO_LOGIN_SIDEBAR_PATH = "assets/flowdash1.png"
_LOGO_HEADER_PATH        = "assets/flowdash2.PNG"

def aplicar_branding_pdv(is_login: bool = False) -> None:
    try:
        if is_login:
            login_brand(custom_path=_LOGO_LOGIN_SIDEBAR_PATH, height_px=230, show_title=False)
            return
        try:
            sidebar_brand(custom_path=_LOGO_LOGIN_SIDEBAR_PATH, height_px=200)
        except Exception:
            pass
        page_header(custom_path=_LOGO_HEADER_PATH, logo_height_px=130, show_title=False)
    except Exception as e:
        st.caption(f"[branding PDV] aviso: {e}")

# ------------------------- Flags/Dropbox -------------------------
def _truthy(v) -> bool: return str(v).strip().lower() in {"1","true","yes","y","on"}
def _flag_debug() -> bool:
    try: return _truthy(dict(st.secrets.get("dropbox", {})).get("debug", "0"))
    except Exception: return _truthy(os.getenv("FLOWDASH_DEBUG","0"))
def _flag_dropbox_disable() -> bool:
    try: return _truthy(dict(st.secrets.get("dropbox", {})).get("disable", "0"))
    except Exception: return _truthy(os.getenv("DROPBOX_DISABLE","0"))

_DEBUG = _flag_debug()
_cfg = load_dropbox_settings(prefer_env_first=True)
ACCESS_TOKEN_CFG = _cfg.get("access_token") or ""
DROPBOX_PATH_CFG = _cfg.get("file_path") or "/FlowDash/data/flowdash_data.db"
FORCE_DOWNLOAD_CFG = _truthy(_cfg.get("force_download", "0"))
TOKEN_SOURCE_CFG = _cfg.get("token_source", "none")
_DROPBOX_DISABLED = _flag_dropbox_disable()

if _DEBUG:
    import requests  # type: ignore

def _db_local_path() -> pathlib.Path:
    p = _CURR_DIR / "data" / "flowdash_data.db"
    p.parent.mkdir(parents=True, exist_ok=True); return p

def _is_sqlite(path: pathlib.Path) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(16).startswith(b"SQLite format 3")
    except Exception:
        return False

def _has_table(path: pathlib.Path, table: str) -> bool:
    try:
        with sqlite3.connect(str(path)) as conn:
            return conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;", (table,)
            ).fetchone() is not None
    except Exception:
        return False

def _debug_file_info(path: pathlib.Path) -> str:
    try:
        size = path.stat().st_size
        with open(path, "rb") as f: head = f.read(16)
        return f"size={size}B, head={head!r}"
    except Exception as e:
        return f"(falha ao inspecionar: {e})"

def _throttle(key: str, min_seconds: int) -> bool:
    import time
    last = float(st.session_state.get(key) or 0.0); now = time.time()
    if (now - last) >= float(min_seconds):
        st.session_state[key] = now; return True
    return False

@st.cache_resource(show_spinner=True)
def ensure_db_available(access_token: str, dropbox_path: str, force_download: bool) -> tuple[str, str]:
    db_local = _db_local_path()
    if _DROPBOX_DISABLED:
        if db_local.exists() and db_local.stat().st_size > 0 and _is_sqlite(db_local) and _has_table(db_local, "usuarios"):
            os.environ["FLOWDASH_DB"] = str(db_local)
            return str(db_local), "Local"
        st.error("❌ Modo offline: não há DB local válido (tabela 'usuarios')."); st.stop()

    # 1) Token curto (legado)
    if access_token and dropbox_path:
        try:
            candidate_path = ensure_local_db_api(
                access_token=access_token, dropbox_path=dropbox_path,
                dest_path=str(db_local), force_download=force_download,
                validate_table="usuarios",
            )
            p = pathlib.Path(candidate_path)
            if p.exists() and p.stat().st_size > 0 and _is_sqlite(p) and _has_table(p,"usuarios"):
                os.environ["FLOWDASH_DB"] = str(p); return str(p), "Dropbox"
        except Exception:
            pass
    # 2) SDK/refresh
    try:
        candidate_path = baixar_db_para_local()
        p = pathlib.Path(candidate_path)
        if p.exists() and p.stat().st_size > 0 and _is_sqlite(p) and _has_table(p,"usuarios"):
            os.environ["FLOWDASH_DB"] = str(p); return str(p), "Dropbox"
    except Exception:
        pass
    # 3) Local
    if db_local.exists() and db_local.stat().st_size > 0 and _is_sqlite(db_local) and _has_table(db_local,"usuarios"):
        os.environ["FLOWDASH_DB"] = str(db_local); return str(db_local), "Local"

    info = _debug_file_info(db_local) if db_local.exists() else "(arquivo não existe)"
    st.error("❌ Não foi possível obter um banco válido.\n" + info); st.stop()

_effective_token = "" if _DROPBOX_DISABLED else (ACCESS_TOKEN_CFG or "")
_effective_path = DROPBOX_PATH_CFG
_effective_force = FORCE_DOWNLOAD_CFG
DB_PATH, DB_ORIG = ensure_db_available(_effective_token, _effective_path, _effective_force)
st.session_state.setdefault("caminho_banco", DB_PATH)

# ------------------------- Sync -------------------------
_PULL_THROTTLE_SECONDS = 45
def _auto_pull_if_remote_newer() -> None:
    if DB_ORIG != "Dropbox" or _DROPBOX_DISABLED: return
    if not _throttle("_pdv_pull_check", _PULL_THROTTLE_SECONDS): return
    try:
        dbx = get_dbx(); meta = dbx.files_get_metadata(_effective_path)
        remote_dt = getattr(meta, "server_modified", None)
        if not remote_dt: return
        if remote_dt.tzinfo is None: remote_dt = remote_dt.replace(tzinfo=timezone.utc)
        remote_ts = remote_dt.timestamp()
    except Exception:
        return
    try: local_ts = os.path.getmtime(DB_PATH)
    except Exception: local_ts = 0.0
    last_pull = float(st.session_state.get("_pdv_db_last_pull_ts") or 0.0)
    if remote_ts > max(local_ts, last_pull):
        try:
            data = download_bytes(dbx, _effective_path)
            tmp = DB_PATH + ".tmp"
            with open(tmp, "wb") as f: f.write(data)
            shutil.move(tmp, DB_PATH)
            st.session_state["_pdv_db_last_pull_ts"] = remote_ts
            st.toast("☁️ PDV: banco atualizado.", icon="🔄")
            st.cache_data.clear()
        except Exception as e:
            st.warning(f"PDV: falha no pull refresh: {e}")

def _auto_push_if_local_changed() -> None:
    if DB_ORIG != "Dropbox" or _DROPBOX_DISABLED: return
    try: mtime = os.path.getmtime(DB_PATH)
    except Exception: return
    last_sent = float(st.session_state.get("_pdv_db_last_push_ts") or 0.0)
    if mtime > (last_sent + 0.1):
        try:
            enviar_db_local()
            st.session_state["_pdv_db_last_push_ts"] = mtime
            st.toast("☁️ PDV sincronizado.", icon="✅")
        except Exception as e:
            st.warning(f"PDV: falha no push: {e}")

_auto_pull_if_remote_newer()

# ------------------------- Estilo -------------------------
st.markdown(
    """
    <style>
    [data-testid="stSidebar"] { display: none !important; }
    .block-container { padding-top: 3.2rem; padding-bottom: 2rem; padding-left: 1.5rem; padding-right: 1.5rem; }
    .db-badge { display:block; margin: 6px 0 12px; font-size: 0.86rem; color: #9AA0A6; }

    /* Botão "Sair" só do topo — super compacto */
    #top-logout button[data-testid="baseButton-secondary"]{
        padding: 2px 10px !important;
        font-size: 0.80rem !important;
        min-width: auto !important;
        line-height: 1.0 !important;
        border-radius: 8px !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ------------------------- DB helpers -------------------------
def _conn() -> sqlite3.Connection:
    if not DB_PATH or not os.path.exists(DB_PATH):
        st.error(f"❌ Banco de dados não encontrado: `{DB_PATH or '(vazio)'}`"); st.stop()
    conn = sqlite3.connect(DB_PATH, check_same_thread=False); conn.row_factory = sqlite3.Row; return conn

@st.cache_data(show_spinner=False, ttl=30)
def _entrada_date_bounds() -> Tuple[date, date]:
    today = date.today()
    try:
        with _conn() as conn:
            row = conn.execute("SELECT MIN(date(Data)), MAX(date(Data)) FROM entrada").fetchone()
            if not row or not row[0]: return (today, today)
            from datetime import datetime as _dt
            dmin = _dt.strptime(row[0], "%Y-%m-%d").date()
            dmax = _dt.strptime(row[1], "%Y-%m-%d").date() if row[1] else today
            return (dmin, dmax)
    except Exception:
        return (today, today)

# ------------------------- Metas helpers (LOJA) -------------------------
def _fmt_moeda(v: float) -> str:
    return f"R$ {float(v or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def _inicio_semana(dt: date) -> date: return dt - timedelta(days=dt.weekday())
def _calcular_percentual(valor: float, meta: float) -> float:
    if not meta or meta <= 0: return 0.0
    return round((float(valor)/float(meta))*100.0, 1)

def _gauge_percentual_zonas(titulo: str, percentual: float, bronze_pct: float, prata_pct: float,
                            axis_max: float = 120.0, bar_color_rgba: str = "rgba(0,200,83,0.75)",
                            valor_label: Optional[str] = None) -> go.Figure:
    bronze = max(0.0, min(100.0, float(bronze_pct)))
    prata  = max(bronze, min(100.0, float(prata_pct)))
    max_axis = max(100.0, float(axis_max)); value = float(max(0.0, min(max_axis, percentual)))
    steps = [
        {"range":[0,bronze],"color":"#E53935"},
        {"range":[bronze,prata],"color":"#CD7F32"},
        {"range":[prata,100],"color":"#C0C0C0"},
        {"range":[100,max_axis],"color":"#FFD700"},
    ]
    fig = go.Figure(go.Indicator(
        mode="gauge+number", value=value, number={"suffix":"%"},
        title={"text": titulo, "font":{"size":18}},
        gauge={"shape":"angular","axis":{"range":[0,max_axis]},"bgcolor":"rgba(0,0,0,0)","bar":{"color":bar_color_rgba},"steps":steps,"borderwidth":0},
    ))
    if valor_label:
        fig.add_annotation(x=0.5,y=0.0,xref="paper",yref="paper",yanchor="top",yshift=-6,
                           text=f"<span style='font-size:18px;font-weight:700;color:#00C853'>{valor_label}</span>",
                           showarrow=False, align="center")
    fig.update_layout(margin=dict(l=10,r=10,t=80,b=80), height=300); return fig

def _metas_loja_vigente(conn: sqlite3.Connection, ref_day: date) -> Tuple[float,float,float,float,float,float]:
    ref_key = f"{ref_day:%Y-%m}"
    df = pd.read_sql("SELECT * FROM metas;", conn) if conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='metas'").fetchone() else pd.DataFrame()
    if df.empty: return (0,0,0,0,0,0)
    df["vendedor"] = df.get("vendedor","LOJA").astype(str).fillna("LOJA").str.strip()
    df = df[df["vendedor"].str.upper()=="LOJA"].copy()
    if df.empty: return (0,0,0,0,0,0)
    df["mes"] = (df["mes"].astype(str).str[:7]) if "mes" in df.columns else None
    df["_mes_key"] = df["mes"].fillna("0000-00")
    df = df[df["_mes_key"] <= ref_key].sort_values("_mes_key").tail(1)
    row = df.iloc[-1]
    def _num(v,d=0.0):
        try: return float(v if v is not None else d)
        except Exception: return d
    mensal  = _num(row.get("mensal", row.get("meta_mensal", 0.0)))
    semanal = mensal * (_num(row.get("perc_semanal", 25.0))/100.0)
    wd_map = {0:"segunda",1:"terca",2:"quarta",3:"quinta",4:"sexta",5:"sabado",6:"domingo"}
    col_dia = wd_map.get(ref_day.weekday(),"segunda")
    meta_dia = _num(row.get(col_dia, semanal * (_num(row.get(f"perc_{col_dia}", 0.0))/100.0)))
    ouro   = _num(row.get("meta_ouro", mensal))
    prata  = _num(row.get("meta_prata", mensal * (_num(row.get("perc_prata", 87.5))/100.0)))
    bronze = _num(row.get("meta_bronze", mensal * (_num(row.get("perc_bronze", 75.0))/100.0)))
    return (meta_dia, semanal, mensal, ouro, prata, bronze)

def _valores_loja(conn: sqlite3.Connection, ref_day: date) -> Tuple[float,float,float]:
    df_e = pd.read_sql("SELECT COALESCE(Usuario,'') AS Usuario, Data, Valor FROM entrada;", conn)
    if df_e.empty: return (0,0,0)
    df_e["UsuarioUpper"] = df_e["Usuario"].astype(str).str.upper()
    df_e["Data"] = pd.to_datetime(df_e["Data"], errors="coerce")
    df = df_e[df_e["UsuarioUpper"] != "LOJA"].copy()
    inicio_sem, inicio_mes = _inicio_semana(ref_day), ref_day.replace(day=1)
    m_dia = (df["Data"].dt.date == ref_day)
    m_sem = (df["Data"].dt.date >= inicio_sem) & (df["Data"].dt.date <= ref_day)
    m_mes = (df["Data"].dt.date >= inicio_mes) & (df["Data"].dt.date <= ref_day)
    v_dia = pd.to_numeric(df.loc[m_dia,"Valor"], errors="coerce").fillna(0.0).sum()
    v_sem = pd.to_numeric(df.loc[m_sem,"Valor"], errors="coerce").fillna(0.0).sum()
    v_mes = pd.to_numeric(df.loc[m_mes,"Valor"], errors="coerce").fillna(0.0).sum()
    return float(v_dia), float(v_sem), float(v_mes)

def _cards_html_periodo(titulo: str, ouro: float, prata: float, bronze: float, acumulado: float) -> str:
    def _linha(nivel, meta):
        falta_nv = max(float(meta) - float(acumulado), 0.0)
        falta_txt = f"<span style='color:#00C853'>✅ {_fmt_moeda(0)}</span>" if falta_nv <= 0 else _fmt_moeda(falta_nv)
        return (f"<tr><td style='padding:8px 6px;color:#ECEFF1;font-weight:600;'>{nivel}</td>"
                f"<td style='padding:8px 6px;color:#B0BEC5;text-align:right;'>{_fmt_moeda(meta)}</td>"
                f"<td style='padding:8px 6px;color:#B0BEC5;text-align:right;'>{falta_txt}</td></tr>")
    return f"""
    <div style='border:1px solid #333; border-radius:12px; padding:12px; background-color:#121212;'>
      <div style='font-weight:700; color:#B0BEC5; margin-bottom:8px;'>{titulo}</div>
      <table style='width:100%; border-collapse:collapse;'>
        <thead><tr>
          <th style='text-align:left; padding:6px; color:#90A4AE; font-weight:600;'>Nível</th>
          <th style='text-align:right; padding:6px; color:#90A4AE; font-weight:600;'>Meta</th>
          <th style='text-align:right; padding:6px; color:#90A4AE; font-weight:600;'>Falta</th>
        </tr></thead>
        <tbody>
          {_linha("🥇 Ouro", ouro)}
          {_linha("🥈 Prata", prata)}
          {_linha("🥉 Bronze", bronze)}
        </tbody>
      </table>
    </div>
    """

def _metas_loja_gauges(ref_day: date) -> None:
    with _conn() as conn:
        val_dia, val_sem, val_mes = _valores_loja(conn, ref_day)
        meta_dia, meta_sem, meta_mes, ouro, prata, bronze = _metas_loja_vigente(conn, ref_day)

    p_dia = _calcular_percentual(val_dia, meta_dia)
    p_sem = _calcular_percentual(val_sem, meta_sem)
    p_mes = _calcular_percentual(val_mes, meta_mes)

    bronze_pct = 75.0 if ouro <= 0 else round(100.0 * (bronze / max(ouro, 1e-9)), 1)
    prata_pct  = 87.5 if ouro <= 0 else round(100.0 * (prata  / max(ouro, 1e-9)), 1)

    st.markdown(f"<h5 style='margin: 5px 0;'>🏪 LOJA</h5>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    c1.plotly_chart(_gauge_percentual_zonas("Meta do Dia", p_dia, bronze_pct, prata_pct, valor_label=_fmt_moeda(val_dia)),
                    use_container_width=True, key=f"pdv_g_loja_dia_{ref_day}")
    c2.plotly_chart(_gauge_percentual_zonas("Meta da Semana", p_sem, bronze_pct, prata_pct, valor_label=_fmt_moeda(val_sem)),
                    use_container_width=True, key=f"pdv_g_loja_sem_{ref_day}")
    c3.plotly_chart(_gauge_percentual_zonas("Meta do Mês", p_mes, bronze_pct, prata_pct, valor_label=_fmt_moeda(val_mes)),
                    use_container_width=True, key=f"pdv_g_loja_mes_{ref_day}")

    t1, t2, t3 = st.columns(3)
    prata_pct_calc  = 87.5 if ouro<=0 else 100.0*(prata/max(ouro,1e-9))
    bronze_pct_calc = 75.0 if ouro<=0 else 100.0*(bronze/max(ouro,1e-9))
    prata_d, bronze_d = meta_dia*(prata_pct_calc/100.0), meta_dia*(bronze_pct_calc/100.0)
    prata_s, bronze_s = meta_sem*(prata_pct_calc/100.0), meta_sem*(bronze_pct_calc/100.0)
    with t1: st.markdown(_cards_html_periodo("📅 Dia", meta_dia, prata_d, bronze_d, val_dia), unsafe_allow_html=True)
    with t2: st.markdown(_cards_html_periodo("🗓️ Semana", meta_sem, prata_s, bronze_s, val_sem), unsafe_allow_html=True)
    with t3: st.markdown(_cards_html_periodo("📆 Mês", ouro, prata, bronze, val_mes), unsafe_allow_html=True)

# ------------------------- Venda -------------------------
@st.cache_resource
def _resolve_render_venda() -> Callable[[SimpleNamespace], None] | None:
    try:
        mod = importlib.import_module("flowdash_pages.lancamentos.venda.page_venda")
        fn = getattr(mod, "render_venda", None)
        return fn if callable(fn) else None
    except Exception:
        return None

def _render_form_venda(vendedor: Dict[str, object]) -> None:
    st.markdown("## 🧾 Nova Venda")
    os.environ["FLOWDASH_DB"] = DB_PATH
    if not os.path.exists(DB_PATH):
        st.error(f"❌ DB não existe no caminho esperado:\n`{DB_PATH}`"); st.stop()

    if "pdv_original_user" not in st.session_state and "usuario_logado" in st.session_state:
        st.session_state["pdv_original_user"] = st.session_state["usuario_logado"]
        st.session_state["pdv_header_user"] = st.session_state["pdv_original_user"]

    st.session_state["usuario_logado"] = {
        "id": vendedor["id"], "nome": vendedor["nome"],
        "email": f"vendedor_{vendedor['id']}@pdv.local",
        "perfil": vendedor.get("perfil") or "Vendedor",
    }
    st.session_state["pdv_context"] = {"vendedor_id": vendedor["id"], "vendedor_nome": vendedor["nome"], "origem": "PDV"}

    fn = _resolve_render_venda()
    if not callable(fn):
        st.error("❌ Função render_venda(state) não encontrada em page_venda.py"); return

    ref_day = st.session_state.get("pdv_ref_date", date.today())
    state = SimpleNamespace(caminho_banco=DB_PATH, db_path=DB_PATH, data_lanc=ref_day)
    try:
        fn(state)
    except Exception as e:
        with st.container(border=True):
            st.error("❌ Erro ao abrir o formulário de venda.")
            st.caption(f"Detalhe técnico: {e}")

# ------------------------- Login helpers -------------------------
try:
    from auth import validar_login as auth_validar_login  # type: ignore
except Exception:
    try:
        from auth.auth import validar_login as auth_validar_login  # type: ignore
    except Exception:
        def auth_validar_login(email: str, senha: str, caminho_banco: Optional[str] = None) -> Optional[dict]:
            from utils.utils import gerar_hash_senha
            senha_hash = gerar_hash_senha(senha)
            caminho_banco = caminho_banco or DB_PATH
            with sqlite3.connect(caminho_banco) as conn:
                row = conn.execute(
                    "SELECT id, nome, email, perfil FROM usuarios WHERE email=? AND senha=? AND ativo=1",
                    (email, senha_hash),
                ).fetchone()
            return {"id": row[0], "nome": row[1], "email": row[2], "perfil": row[3]} if row else None

def _login_box() -> bool:
    with st.form("form_login", clear_on_submit=False):
        email = st.text_input("Email", max_chars=100)
        senha = st.text_input("Senha", type="password", max_chars=50)
        ok = st.form_submit_button("Entrar")
    if ok:
        user = auth_validar_login(email, senha, DB_PATH)
        if user:
            st.session_state["usuario_logado"] = user
            st.success(f"Bem-vindo, {user['nome']}!"); st.rerun()
        else:
            st.error("Credenciais inválidas ou usuário inativo.")
    return bool(st.session_state.get("usuario_logado"))

def _do_logout():
    if "pdv_original_user" in st.session_state:
        st.session_state["usuario_logado"] = st.session_state.pop("pdv_original_user", None)
    else:
        st.session_state.pop("usuario_logado", None)
    st.session_state.pop("pdv_header_user", None)
    for k in ("pdv_mostrar_form", "pdv_vendedor_venda", "pdv_context"):
        st.session_state.pop(k, None)
    st.session_state["pdv_flash_ok"] = "Sessão encerrada."
    st.rerun()

# ------------------------- App -------------------------
def main() -> None:
    # mensagens flash
    _flash = None
    for k in ("pdv_flash_ok", "msg_ok", "flash_ok"):
        if not _flash: _flash = st.session_state.pop(k, None)
    if _flash: st.success(_flash)

    # ======= Linha 1: Badge (esq) + Botão Sair (dir) =======
    logged = bool(st.session_state.get("usuario_logado"))
    top_l, top_r = st.columns([0.85, 0.15])
    with top_l:
        st.markdown(f"<span class='db-badge'>🗃️ Banco em uso: <strong>{DB_ORIG}</strong></span>", unsafe_allow_html=True)
    with top_r:
        if logged:
            st.markdown('<div id="top-logout">', unsafe_allow_html=True)
            if st.button("Sair", key="btn_logout_top", type="secondary", use_container_width=True):
                _do_logout()
            st.markdown("</div>", unsafe_allow_html=True)

    # Branding
    aplicar_branding_pdv(is_login=not logged)

    # login
    if not logged:
        # --- Centraliza TÍTULO + FORM em uma coluna estreita ---
        left, center, right = st.columns([1, 1.15, 1])
        with center:
            # título centralizado
            st.markdown("<h1 style='text-align:center; margin: 0 0 0.5rem;'>🔐 Login PDV</h1>", unsafe_allow_html=True)

            # form estreito/fixo
            st.markdown("""
            <style>
            /* Cartão fixo do formulário de login */
            div[data-testid="stForm"]{
                max-width: 420px;
                width: 100%;
                margin: 12px auto 24px;
                padding: 16px;
                border: 1px solid #333;
                border-radius: 12px;
                background: #111;
            }
            /* Botão ocupa toda a largura do cartão */
            div[data-testid="stForm"] .stButton > button { width: 100%; }
            </style>
            """, unsafe_allow_html=True)

            if not _login_box():
                _auto_push_if_local_changed(); return
    else:
        dmin, dmax = _entrada_date_bounds(); today = date.today(); dmax = max(dmax, today)

        # ======= Linha 2: Título (esq) + Data (dir) =======
        hdr_l, hdr_r = st.columns([0.7, 0.3])
        with hdr_l:
            st.markdown("# 🧾 FlowDash — PDV")
            header_user = st.session_state.get("pdv_header_user", st.session_state.get("usuario_logado"))
            if header_user:
                st.markdown(
                    f"<div style='margin-top:0.25rem; color:#90A4AE; font-size:0.95rem;'>"
                    f"👤 <strong>{header_user['nome']}</strong> — <code>{header_user['perfil']}</code>"
                    f"</div>",
                    unsafe_allow_html=True,
                )
        with hdr_r:
            st.date_input(
                "📅 Data de referência",
                value=st.session_state.get("pdv_ref_date", today),
                min_value=dmin, max_value=dmax, format="YYYY/MM/DD",
                key="pdv_ref_date",
            )

        # ======= Linha 3: NOVA VENDA (topo) + fluxo =======
        st.markdown('<div class="nv-wrap">', unsafe_allow_html=True)
        if not st.session_state.get("pdv_mostrar_form"):
            if st.button("➕ Nova Venda", key="btn_nova_venda_top", use_container_width=True):
                st.session_state["pdv_mostrar_form"] = True
                st.session_state.pop("pdv_vendedor_venda", None)
                st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

        if st.session_state.get("pdv_mostrar_form"):
            vendedor = st.session_state.get("pdv_vendedor_venda")
            if not vendedor:
                vend = _selecionar_vendedor_e_validar_pin()
                if vend:
                    st.session_state["pdv_vendedor_venda"] = vend
                    st.session_state["pdv_flash_ok"] = f"Vendedor **{vend['nome']}** identificado para a venda."
                    st.rerun()
                else:
                    _auto_push_if_local_changed()
                    return
            else:
                _render_form_venda(vendedor)
                col_a, col_b = st.columns([1, 1])
                with col_a:
                    if st.button("🔁 Trocar vendedor desta venda", key="btn_trocar_vend", use_container_width=True):
                        if "pdv_original_user" in st.session_state:
                            st.session_state["usuario_logado"] = st.session_state["pdv_original_user"]
                        st.session_state.pop("pdv_vendedor_venda", None); st.rerun()
                with col_b:
                    if st.button("❌ Cancelar venda", key="btn_cancelar_venda", use_container_width=True):
                        for k in ("pdv_mostrar_form", "pdv_vendedor_venda", "pdv_context"):
                            st.session_state.pop(k, None)
                        st.session_state["pdv_flash_ok"] = "Venda cancelada."
                        st.rerun()

        # ======= Metas LOJA =======
        ref_day = st.session_state.get("pdv_ref_date", today)
        st.markdown(f"**Metas do dia — {ref_day:%Y-%m-%d}**")
        _metas_loja_gauges(ref_day)
        st.divider()

    _auto_push_if_local_changed()

# ------------------------- Seleção de vendedor / PIN -------------------------
def _listar_usuarios_ativos_sem_pdv() -> List[Tuple[int, str, str]]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT id, nome, perfil FROM usuarios WHERE ativo = 1 AND (perfil IS NULL OR perfil <> ?) ORDER BY nome ASC",
            ("PDV",),
        ).fetchall()
    return [(int(r["id"]), str(r["nome"]), str(r["perfil"] or "")) for r in rows]

def _buscar_pin_usuario(usuario_id: int) -> Optional[str]:
    with _conn() as conn:
        row = conn.execute("SELECT pin FROM usuarios WHERE id = ? AND ativo = 1", (usuario_id,)).fetchone()
    return None if not row else row["pin"]

def _inc_tentativas(usuario_id: int) -> int:
    key = "pdv_pin_tentativas"
    st.session_state.setdefault(key, {})
    st.session_state[key][usuario_id] = st.session_state[key].get(usuario_id, 0) + 1
    return st.session_state[key][usuario_id]

def _reset_tentativas(usuario_id: int) -> None:
    key = "pdv_pin_tentativas"
    if key in st.session_state and usuario_id in st.session_state[key]:
        st.session_state[key][usuario_id] = 0

def _selecionar_vendedor_e_validar_pin() -> Optional[Dict]:
    st.markdown("#### 👤 Vendedor da Venda")
    usuarios = _listar_usuarios_ativos_sem_pdv()
    if not usuarios:
        st.warning("Nenhum usuário ativo encontrado. Cadastre em **Cadastros › Usuários**.")
        return None
    labels, label_to_id = [], {}
    for uid, nome, perfil in usuarios:
        label = f"{nome} — {perfil or 'Sem perfil'}"
        labels.append(label); label_to_id[label] = uid
    c_vend, c_pin, c_btn = st.columns([0.55, 0.25, 0.20])
    with c_vend: escolha = st.selectbox("Vendedor", labels, key="pdv_sel_vendedor")
    with c_pin: pin_in = st.text_input("PIN (4 dígitos)", type="password", max_chars=4, key="pdv_pin_vendedor")
    with c_btn:
        st.write("")
        confirmar = st.button("Confirmar", key="btn_confirma_pin", use_container_width=True)
    if not confirmar: return None
    usuario_id = label_to_id[escolha]
    if _inc_tentativas(usuario_id) > 5:
        st.error("Muitas tentativas. Troque o vendedor ou tente novamente mais tarde."); return None
    try:
        pin_digitado = validar_pin(pin_in)
    except ValueError as e:
        st.error(str(e)); return None
    if pin_digitado is None:
        st.error("Informe o PIN de 4 dígitos para prosseguir."); return None
    pin_db = _buscar_pin_usuario(usuario_id)
    if pin_db is None:
        st.error("Este usuário não possui PIN cadastrado. Defina um PIN na página de Usuários."); return None
    if hmac.compare_digest(str(pin_db), pin_digitado):
        nome_sel = next(n for (uid, n, _) in usuarios if uid == usuario_id)
        perfil_sel = next(p for (uid, _, p) in usuarios if uid == usuario_id)
        _reset_tentativas(usuario_id)
        return {"id": usuario_id, "nome": nome_sel, "perfil": perfil_sel}
    st.error("PIN incorreto."); return None

if __name__ == "__main__":
    main()
