# main.py
"""
FlowDash — Main App
===================
Ponto de entrada do aplicativo Streamlit do FlowDash.

Política do banco:
  1) Baixar via refresh_token (SDK) — shared.dbx_io.baixar_db_para_local()
     (se force_download="1", baixa sempre)
  2) (Legado opcional) Se houver access_token curto, ainda tentamos uma
     primeira cópia com shared.db_from_dropbox_api.ensure_local_db_api()
     — útil apenas para debug/migração.
  3) Se falhar, usa o DB local 'data/flowdash_data.db' (com tabela 'usuarios').
  4) Se nada der certo, erro claro.

Sincronização automática
  - Pull: antes de usar, compara com remoto (SDK). Se `force_download="1"`,
    força o download. Estratégia last-writer-wins.
  - Push: ao final do ciclo, se o arquivo local mudou, envia (SDK).

Flags úteis:
  - DEBUG:    FLOWDASH_DEBUG=1  ou  [dropbox].debug="1"
  - OFFLINE:  DROPBOX_DISABLE=1  ou  [dropbox].disable="1"
"""
from __future__ import annotations

import importlib
import inspect
import os
import pathlib
import sqlite3
import shutil
from datetime import datetime, timezone

import requests
import streamlit as st

from auth.auth import (
    validar_login,
    verificar_acesso,
    exibir_usuario_logado,
    limpar_todas_as_paginas,
)
from utils.utils import garantir_trigger_totais_saldos_caixas

# Legado (opcional, só para bootstrap com access_token curto)
from shared.db_from_dropbox_api import ensure_local_db_api

# Config
from shared.dropbox_config import load_dropbox_settings, mask_token

# NOVO: SDK com refresh token (pull/push)
from shared.dbx_io import enviar_db_local, baixar_db_para_local
from shared.dropbox_client import get_dbx  # para ler metadata (SDK)

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
st.set_page_config(page_title="FlowDash", layout="wide")

# -----------------------------------------------------------------------------
# Helpers (debug/local)
# -----------------------------------------------------------------------------
def _debug_file_info(path: pathlib.Path) -> str:
    try:
        size = path.stat().st_size
        with open(path, "rb") as f:
            head = f.read(16)
        return f"size={size}B, head={head!r}"
    except Exception as e:
        return f"(falha ao inspecionar: {e})"

def _is_sqlite(path: pathlib.Path) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(16).startswith(b"SQLite format 3")
    except Exception:
        return False

def _has_table(path: pathlib.Path, table: str) -> bool:
    try:
        with sqlite3.connect(str(path)) as conn:
            cur = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;",
                (table,),
            )
            return cur.fetchone() is not None
    except Exception:
        return False

def _db_local_path() -> pathlib.Path:
    root = pathlib.Path(__file__).resolve().parent
    p = root / "data" / "flowdash_data.db"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def _flag_debug() -> bool:
    try:
        sec = dict(st.secrets.get("dropbox", {}))
        if str(sec.get("debug", "")).strip().lower() in {"1", "true", "yes", "y", "on"}:
            return True
    except Exception:
        pass
    return str(os.getenv("FLOWDASH_DEBUG", "")).strip().lower() in {"1", "true", "yes", "y", "on"}

def _flag_dropbox_disable() -> bool:
    try:
        sec = dict(st.secrets.get("dropbox", {}))
        if str(sec.get("disable", "")).strip().lower() in {"1", "true", "yes", "y", "on"}:
            return True
    except Exception:
        pass
    return str(os.getenv("DROPBOX_DISABLE", "")).strip().lower() in {"1", "true", "yes", "y", "on"}

# -----------------------------------------------------------------------------
# Diagnóstico Dropbox (apenas se DEBUG ativo)
# -----------------------------------------------------------------------------
_DEBUG = _flag_debug()
_cfg = load_dropbox_settings(prefer_env_first=True)

ACCESS_TOKEN_CFG = _cfg.get("access_token") or ""
DROPBOX_PATH_CFG = _cfg.get("file_path") or "/FlowDash/data/flowdash_data.db"
FORCE_DOWNLOAD_CFG = str(_cfg.get("force_download", "0")).strip() in {"1", "true", "yes", "on"}
TOKEN_SOURCE_CFG = _cfg.get("token_source", "none")

if _DEBUG:
    with st.expander("🔎 Diagnóstico Dropbox (temporário)", expanded=True):
        try:
            try:
                st.write("st.secrets keys:", list(st.secrets.keys()))
                st.write("Tem seção [dropbox] nos Secrets?", "dropbox" in st.secrets)
            except Exception:
                st.write("st.secrets indisponível neste contexto (ok para CLI/local).")

            st.write("token_source:", TOKEN_SOURCE_CFG)  # "env", "st.secrets:/...", "none"
            st.write("access_token (mascarado):", mask_token(ACCESS_TOKEN_CFG))
            st.write("token_length:", len(ACCESS_TOKEN_CFG))
            st.write("file_path:", DROPBOX_PATH_CFG)
            st.write("force_download:", "1" if FORCE_DOWNLOAD_CFG else "0")

            col1, col2 = st.columns(2)
            with col1:
                if st.button("Validar token (users/get_current_account) [LEGADO]"):
                    if not ACCESS_TOKEN_CFG:
                        st.error("Sem token carregado (secrets/env).")
                    else:
                        try:
                            url = "https://api.dropboxapi.com/2/users/get_current_account"
                            r = requests.post(
                                url,
                                headers={"Authorization": f"Bearer {ACCESS_TOKEN_CFG}"},
                                timeout=30,
                            )
                            st.code(f"HTTP {r.status_code}\n{r.text}")
                        except Exception as e:
                            st.error(f"Erro na validação: {e}")
            with col2:
                if st.button("Testar path (files/get_metadata) [LEGADO]"):
                    if not ACCESS_TOKEN_CFG:
                        st.error("Sem token carregado (secrets/env).")
                    else:
                        try:
                            url = "https://api.dropboxapi.com/2/files/get_metadata"
                            headers = {
                                "Authorization": f"Bearer {ACCESS_TOKEN_CFG}",
                                "Content-Type": "application/json",
                            }
                            r = requests.post(
                                url,
                                headers=headers,
                                json={"path": DROPBOX_PATH_CFG},
                                timeout=30,
                            )
                            st.code(f"HTTP {r.status_code}\n{r.text}")
                        except Exception as e:
                            st.error(f"Probe get_metadata falhou: {e}")
        except Exception as e:
            st.warning(f"Falha lendo config Dropbox: {e}")

# -----------------------------------------------------------------------------
# Banco: Dropbox -> Local (refresh por padrão; legado opcional)
# -----------------------------------------------------------------------------
@st.cache_resource(show_spinner=True)
def ensure_db_available(access_token: str, dropbox_path: str, force_download: bool):
    """
    1) Tenta bootstrap por refresh (SDK): baixa para data/flowdash_data.db.
       Se `force_download=True`, baixa sempre.
    2) (Legado) Se houver access_token curto, tenta HTTP (útil para migração).
    3) Se falhar, usa local se válido.
    """
    db_local = _db_local_path()

    # 1) Preferencial: SDK com refresh
    try:
        # Se force_download=True, baixa incondicionalmente
        if force_download:
            local_path = baixar_db_para_local()
        else:
            # Sem forçar: ainda tenta baixar; se não existir remoto, exceção cai no except
            local_path = baixar_db_para_local()
        candidate = pathlib.Path(local_path)
        if (
            candidate.exists()
            and candidate.stat().st_size > 0
            and _is_sqlite(candidate)
            and _has_table(candidate, "usuarios")
        ):
            st.session_state["db_mode"] = "online"
            st.session_state["db_origem"] = "Dropbox"
            st.session_state["db_in_use_label"] = "Dropbox"
            st.session_state["db_path"] = str(candidate)
            os.environ["FLOWDASH_DB"] = str(candidate)
            return str(candidate), "Dropbox"
    except Exception:
        pass  # cai para legado/local

    # 2) Legado (só se houver access token)
    if access_token and dropbox_path:
        try:
            candidate_path = ensure_local_db_api(
                access_token=access_token,
                dropbox_path=dropbox_path,
                dest_path=str(db_local),
                force_download=force_download,
                validate_table="usuarios",
            )
            candidate = pathlib.Path(candidate_path)
            if (
                candidate.exists()
                and candidate.stat().st_size > 0
                and _is_sqlite(candidate)
                and _has_table(candidate, "usuarios")
            ):
                st.session_state["db_mode"] = "online"
                st.session_state["db_origem"] = "Dropbox"
                st.session_state["db_in_use_label"] = "Dropbox"
                st.session_state["db_path"] = str(candidate)
                os.environ["FLOWDASH_DB"] = str(candidate)
                return str(candidate), "Dropbox"
        except Exception:
            pass

    # 3) Local
    if (
        db_local.exists()
        and db_local.stat().st_size > 0
        and _is_sqlite(db_local)
        and _has_table(db_local, "usuarios")
    ):
        st.session_state["db_mode"] = "local"
        st.session_state["db_origem"] = "Local"
        st.session_state["db_in_use_label"] = "Local"
        st.session_state["db_path"] = str(db_local)
        os.environ["FLOWDASH_DB"] = str(db_local)
        return str(db_local), "Local"

    # 4) Erro explícito
    info = _debug_file_info(db_local) if db_local.exists() else "(arquivo não existe)"
    st.error(
        "❌ Não foi possível obter um banco de dados válido.\n\n"
        "- Garanta credenciais **válidas** do Dropbox (refresh_token/app_key/app_secret) e `file_path` correto; **ou**\n"
        "- Coloque manualmente um SQLite válido em `data/flowdash_data.db` com a tabela 'usuarios'.\n"
        f"- Debug local: {info}"
    )
    st.stop()

# Flags efetivas
_DROPBOX_DISABLED = _flag_dropbox_disable()
_effective_token = "" if _DROPBOX_DISABLED else (ACCESS_TOKEN_CFG or "")
_effective_path = DROPBOX_PATH_CFG
_effective_force = FORCE_DOWNLOAD_CFG

# Recurso cacheado
_caminho_banco, _db_origem = ensure_db_available(_effective_token, _effective_path, _effective_force)

# ---- Auto PULL (antes de usar) — SDK + refresh (respeita force_download) ----
def _auto_pull_if_remote_newer():
    if _db_origem != "Dropbox" or _DROPBOX_DISABLED:
        return

    # Força download se flag ligada
    if _effective_force:
        try:
            baixar_db_para_local()
            st.session_state["_main_db_last_pull_ts"] = float(datetime.now(tz=timezone.utc).timestamp())
            st.toast("☁️ Main: banco atualizado (forçado) do Dropbox.", icon="🔄")
            st.cache_data.clear()
        except Exception as e:
            st.warning(f"Main: não foi possível baixar DB remoto (forçado): {e}")
        return

    # Comparação por metadata (SDK)
    try:
        dbx = get_dbx()
        meta = dbx.files_get_metadata(_effective_path)
        remote_dt = getattr(meta, "server_modified", None)
        if remote_dt is None:
            return
        # garante timestamp em UTC
        if remote_dt.tzinfo is None:
            remote_ts = remote_dt.replace(tzinfo=timezone.utc).timestamp()
        else:
            remote_ts = remote_dt.astimezone(timezone.utc).timestamp()
    except Exception:
        return

    try:
        local_ts = os.path.getmtime(_caminho_banco)
    except Exception:
        local_ts = 0.0
    last_pull = float(st.session_state.get("_main_db_last_pull_ts") or 0.0)

    if remote_ts > max(local_ts, last_pull):
        try:
            baixar_db_para_local()
            st.session_state["_main_db_last_pull_ts"] = remote_ts
            st.toast("☁️ Main: banco atualizado do Dropbox.", icon="🔄")
            st.cache_data.clear()
        except Exception as e:
            st.warning(f"Main: não foi possível baixar DB remoto (refresh): {e}")

_auto_pull_if_remote_newer()

# ---- Badge curto ----
st.caption(f"🗃️ Banco em uso: **{_db_origem}**")

# Garantias/infra mínimas
try:
    garantir_trigger_totais_saldos_caixas(_caminho_banco)
except Exception as e:
    st.warning(f"Trigger de totais não criada: {e}")

# -----------------------------------------------------------------------------
# Estado de sessão
# -----------------------------------------------------------------------------
if "usuario_logado" not in st.session_state:
    st.session_state.usuario_logado = None
if "pagina_atual" not in st.session_state:
    st.session_state.pagina_atual = "📊 Dashboard"

# -----------------------------------------------------------------------------
# Roteamento (import dinâmico + injeção de caminho_banco)
# -----------------------------------------------------------------------------
def _call_page(module_path: str):
    try:
        mod = importlib.import_module(module_path)
    except Exception as e:
        st.error(f"Falha ao importar módulo '{module_path}': {e}")
        return

    def _invoke(fn):
        sig = inspect.signature(fn)
        args, kwargs = [], {}
        ss = st.session_state
        usuario_logado = ss.get("usuario_logado")
        known = {
            "usuario": usuario_logado,
            "usuario_logado": usuario_logado,
            "perfil": (usuario_logado or {}).get("perfil") if usuario_logado else None,
            "pagina_atual": ss.get("pagina_atual"),
            "ir_para_formulario": ss.get("ir_para_formulario"),
            "caminho_banco": _caminho_banco,
        }
        for p in sig.parameters.values():
            name, kind, has_default = p.name, p.kind, (p.default is not inspect._empty)
            if name == "caminho_banco":
                value = _caminho_banco
            elif name in known:
                value = known[name]
            elif name in ss:
                value = ss[name]
            else:
                value = None
            should_pass = (not has_default) or (value is not None)
            if should_pass:
                if kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
                    args.append(value)
                else:
                    kwargs[name] = value
        return fn(*args, **kwargs)

    seg = module_path.rsplit(".", 1)[-1]
    parent = module_path.rsplit(".", 2)[-2] if "." in module_path else ""
    tail = seg.split("_", 1)[-1] if "_" in seg else seg
    candidates = [
        "render", "page", "main", "pagina", "show", "pagina_fechamento_caixa",
        f"render_{tail}", "render_page", f"render_{seg}", f"render_{parent}",
        f"page_{tail}", f"show_{tail}", seg,
    ]
    tried = set()
    for fn_name in candidates:
        if fn_name in tried or not hasattr(mod, fn_name):
            tried.add(fn_name); continue
        tried.add(fn_name)
        fn = getattr(mod, fn_name)
        if callable(fn):
            try:
                return _invoke(fn)
            except Exception as e:
                st.error(f"Erro ao executar {module_path}.{fn_name}: {e}")
                return
    for prefix in ("pagina_", "render_"):
        for name, obj in vars(mod).items():
            if callable(obj) and name.startswith(prefix):
                try:
                    return _invoke(obj)
                except Exception as e:
                    st.error(f"Erro ao executar {module_path}.{name}: {e}")
                    return
    st.warning(f"O módulo '{module_path}' não possui função compatível (render/page/main/pagina*/show).")

# -----------------------------------------------------------------------------
# Login
# -----------------------------------------------------------------------------
if not st.session_state.usuario_logado:
    st.title("🔐 Login")
    with st.form("form_login"):
        email = st.text_input("Email")
        senha = st.text_input("Senha", type="password")
        if st.form_submit_button("Entrar"):
            usuario = validar_login(email, senha, _caminho_banco)
            if usuario:
                st.session_state.usuario_logado = usuario
                st.session_state.pagina_atual = (
                    "📊 Dashboard" if usuario["perfil"] in ("Administrador", "Gerente") else "🧾 Lançamentos"
                )
                limpar_todas_as_paginas()
                st.rerun()
            else:
                st.error("❌ Email ou senha inválidos, ou usuário inativo.")
    # antes de parar, empurra alterações locais (se houve cadastro)
    def _auto_push_if_local_changed_login():
        if _db_origem != "Dropbox" or _DROPBOX_DISABLED:
            return
        try:
            mtime = os.path.getmtime(_caminho_banco)
        except Exception:
            return
        last_sent = float(st.session_state.get("_main_db_last_push_ts") or 0.0)
        if mtime > (last_sent + 0.1):
            try:
                enviar_db_local()  # refresh token (SDK)
                st.session_state["_main_db_last_push_ts"] = mtime
                st.toast("☁️ Main: banco sincronizado com o Dropbox.", icon="✅")
            except Exception as e:
                st.warning(f"Main: falha ao enviar DB ao Dropbox (refresh): {e}")
    _auto_push_if_local_changed_login()
    st.stop()

# -----------------------------------------------------------------------------
# Sidebar
# -----------------------------------------------------------------------------
usuario = st.session_state.get("usuario_logado")
if usuario is None:
    st.warning("Faça login para continuar.")
    st.stop()

perfil = usuario["perfil"]
st.sidebar.markdown(f"👤 **{usuario['nome']}**\n🔐 Perfil: `{perfil}`")

if st.sidebar.button("🚪 Sair", use_container_width=True):
    limpar_todas_as_paginas()
    st.session_state.usuario_logado = None
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("## 🧭 Menu de Navegação")
for title in ["📊 Dashboard", "📉 DRE", "🧾 Lançamentos", "💼 Fechamento de Caixa", "🎯 Metas"]:
    if st.sidebar.button(title, use_container_width=True):
        st.session_state.pagina_atual = title
        st.rerun()

with st.sidebar.expander("📋 DataFrames", expanded=False):
    for title in [
        "📥 Entradas", "📤 Saídas", "📦 Mercadorias",
        "💳 Fatura Cartão de Crédito", "📄 Contas a Pagar", "🏦 Empréstimos/Financiamentos"
    ]:
        if st.button(title, use_container_width=True):
            st.session_state.pagina_atual = title
            st.rerun()

if perfil == "Administrador":
    with st.sidebar.expander("🛠️ Cadastros", expanded=False):
        for title in [
            "👥 Usuários", "🎯 Cadastro de Metas", "⚙️ Taxas Maquinetas", "📇 Cartão de Crédito", "💵 Caixa",
            "🛠️ Correção de Caixa", "🏦 Saldos Bancários", "🏛️ Cadastro de Empréstimos",
            "🏦 Cadastro de Bancos", "📂 Cadastro de Saídas"
        ]:
            if st.button(title, use_container_width=True):
                st.session_state.pagina_atual = title
                st.rerun()

# -----------------------------------------------------------------------------
# Roteamento
# -----------------------------------------------------------------------------
st.title(st.session_state.pagina_atual)

ROTAS = {
    "📊 Dashboard": "flowdash_pages.dashboard.dashboard",
    "📉 DRE": "flowdash_pages.dre.dre",
    "🧾 Lançamentos": "flowdash_pages.lancamentos.pagina.page_lancamentos",
    "💼 Fechamento de Caixa": "flowdash_pages.fechamento.fechamento",
    "🎯 Metas": "flowdash_pages.metas.metas",
    "📥 Entradas": "flowdash_pages.dataframes.dataframes",
    "📤 Saídas": "flowdash_pages.dataframes.dataframes",
    "📦 Mercadorias": "flowdash_pages.dataframes.dataframes",
    "💳 Fatura Cartão de Crédito": "flowdash_pages.dataframes.dataframes",
    "📄 Contas a Pagar": "flowdash_pages.dataframes.dataframes",
    "🏦 Empréstimos/Financiamentos": "flowdash_pages.dataframes.dataframes",
    "👥 Usuários": "flowdash_pages.cadastros.pagina_usuarios",
    "🎯 Cadastro de Metas": "flowdash_pages.cadastros.pagina_metas",
    "⚙️ Taxas Maquinetas": "flowdash_pages.cadastros.pagina_maquinetas",
    "📇 Cartão de Crédito": "flowdash_pages.cadastros.pagina_cartoes",
    "💵 Caixa": "flowdash_pages.cadastros.pagina_caixa",
    "🛠️ Correção de Caixa": "flowdash_pages.cadastros.pagina_correcao_caixa",
    "🏦 Saldos Bancários": "flowdash_pages.cadastros.pagina_saldos_bancarios",
    "🏛️ Cadastro de Empréstimos": "flowdash_pages.cadastros.pagina_emprestimos",
    "🏦 Cadastro de Bancos": "flowdash_pages.cadastros.pagina_bancos_cadastrados",
    "📂 Cadastro de Saídas": "flowdash_pages.cadastros.cadastro_categorias",
}
PERMISSOES = {
    "📊 Dashboard": {"Administrador", "Gerente"},
    "📉 DRE": {"Administrador", "Gerente"},
    "🧾 Lançamentos": {"Administrador", "Gerente", "Vendedor"},
    "💼 Fechamento de Caixa": {"Administrador", "Gerente"},
    "🎯 Metas": {"Administrador", "Gerente"},
    "📥 Entradas": {"Administrador", "Gerente"},
    "📤 Saídas": {"Administrador", "Gerente"},
    "📦 Mercadorias": {"Administrador", "Gerente"},
    "💳 Fatura Cartão de Crédito": {"Administrador", "Gerente"},
    "📄 Contas a Pagar": {"Administrador", "Gerente"},
    "🏦 Empréstimos/Financiamentos": {"Administrador", "Gerente"},
    "👥 Usuários": {"Administrador"},
    "🎯 Cadastro de Metas": {"Administrador"},
    "⚙️ Taxas Maquinetas": {"Administrador"},
    "📇 Cartão de Crédito": {"Administrador"},
    "💵 Caixa": {"Administrador"},
    "🛠️ Correção de Caixa": {"Administrador"},
    "🏦 Saldos Bancários": {"Administrador"},
    "🏛️ Cadastro de Empréstimos": {"Administrador"},
    "🏦 Cadastro de Bancos": {"Administrador"},
    "📂 Cadastro de Saídas": {"Administrador"},
}

pagina = st.session_state.get("pagina_atual", "📊 Dashboard")
if pagina in ROTAS:
    perfil_atual = st.session_state.usuario_logado["perfil"]
    if pagina in PERMISSOES and perfil_atual not in PERMISSOES[pagina]:
        st.error("Acesso negado para o seu perfil.")
    else:
        _call_page(ROTAS[pagina])
else:
    st.warning("Página não encontrada.")

# ---- Auto PUSH (depois que a página executou) — SDK + refresh ----
def _auto_push_if_local_changed():
    if _db_origem != "Dropbox" or _DROPBOX_DISABLED:
        return
    try:
        mtime = os.path.getmtime(_caminho_banco)
    except Exception:
        return
    last_sent = float(st.session_state.get("_main_db_last_push_ts") or 0.0)
    if mtime > (last_sent + 0.1):
        try:
            enviar_db_local()  # refresh token (SDK)
            st.session_state["_main_db_last_push_ts"] = mtime
            st.toast("☁️ Main: banco sincronizado com o Dropbox.", icon="✅")
        except Exception as e:
            st.warning(f"Main: falha ao enviar DB ao Dropbox (refresh): {e}")

_auto_push_if_local_changed()
