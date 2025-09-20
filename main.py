# main.py
"""
FlowDash — Main App
===================
Ponto de entrada do aplicativo Streamlit do FlowDash.

Política do banco:
  1) Tentar baixar via TOKEN do Dropbox (API) usando secrets/env:
       [dropbox]
       access_token   = "sl.ABC...SEU_TOKEN..."
       file_path      = "/FlowDash/data/flowdash_data.db"
       force_download = "0"
  2) Se falhar, usar o DB local 'data/flowdash_data.db' (deve conter a tabela 'usuarios').
  3) Se nada der certo, exibir erro claro.

Flags úteis (produção x debug):
  - DEBUG:    FLOWDASH_DEBUG=1  ou  [dropbox].debug="1"     -> mostra diagnóstico
  - OFFLINE:  DROPBOX_DISABLE=1  ou  [dropbox].disable="1"  -> ignora Dropbox (força DB local)

NOVO: sincronização automática (igual ao PDV)
  - Pull: antes de usar, se no Dropbox existir uma versão mais nova, baixa e troca o arquivo local.
  - Push: ao final do ciclo de renderização, se o arquivo local mudou, envia para o Dropbox.
  - Estratégia: last-writer-wins.
"""
from __future__ import annotations

import importlib
import inspect
import os
import pathlib
import sqlite3
import json
import shutil
import requests
from datetime import datetime, timezone

import streamlit as st

from auth.auth import (
    validar_login,
    verificar_acesso,
    exibir_usuario_logado,
    limpar_todas_as_paginas,
)
from utils.utils import garantir_trigger_totais_saldos_caixas
from shared.db_from_dropbox_api import ensure_local_db_api
from shared.dropbox_config import load_dropbox_settings, mask_token  # leitor unificado

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
    # DEBUG via env ou secrets
    try:
        sec = dict(st.secrets.get("dropbox", {}))
        if str(sec.get("debug", "")).strip().lower() in {"1", "true", "yes", "y", "on"}:
            return True
    except Exception:
        pass
    return str(os.getenv("FLOWDASH_DEBUG", "")).strip().lower() in {"1", "true", "yes", "y", "on"}

def _flag_dropbox_disable() -> bool:
    # OFFLINE (ignorar Dropbox) via env ou secrets
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
FORCE_DOWNLOAD_CFG = bool(_cfg.get("force_download", False))
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
                if st.button("Validar token (users/get_current_account)"):
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
                if st.button("Testar path no Dropbox (files/get_metadata)"):
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
# Dropbox helpers (metadata / download / upload) — iguais ao PDV
# -----------------------------------------------------------------------------
def _parse_dt(dt_str: str) -> float:
    try:
        if dt_str.endswith("Z"):
            return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0

def _dropbox_get_metadata(token: str, remote_path: str):
    if not token or not remote_path:
        return None
    try:
        r = requests.post(
            "https://api.dropboxapi.com/2/files/get_metadata",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"path": remote_path},
            timeout=30,
        )
        if r.status_code != 200:
            return None
        j = r.json()
        return {
            "rev": j.get("rev"),
            "server_modified": j.get("server_modified"),
            "server_ts": _parse_dt(j.get("server_modified", "")),
            "size": j.get("size"),
        }
    except Exception:
        return None

def _dropbox_download(token: str, remote_path: str, dest_path: str) -> str:
    headers = {"Authorization": f"Bearer {token}", "Dropbox-API-Arg": json.dumps({"path": remote_path})}
    url = "https://content.dropboxapi.com/2/files/download"
    tmp = dest_path + ".tmp"
    with requests.post(url, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
    shutil.move(tmp, dest_path)
    return dest_path

def _dropbox_upload(token: str, remote_path: str, local_path: str) -> dict:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream",
        "Dropbox-API-Arg": json.dumps(
            {"path": remote_path, "mode": "overwrite", "autorename": False, "mute": False, "strict_conflict": False}
        ),
    }
    with open(local_path, "rb") as f:
        r = requests.post("https://content.dropboxapi.com/2/files/upload", headers=headers, data=f, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"upload failed: HTTP {r.status_code} — {r.text}")
    return r.json()

# -----------------------------------------------------------------------------
# Banco: Dropbox TOKEN -> Local; sem template obrigatório
#   -> agora retorna (caminho_banco, origem_label)
# -----------------------------------------------------------------------------
@st.cache_resource(show_spinner=True)
def ensure_db_available(access_token: str, dropbox_path: str, force_download: bool):
    """
    1) Se houver TOKEN do Dropbox e file_path: baixa via API para data/flowdash_data.db.
    2) Se download falhar ou não houver token/caminho: usa DB local se válido.
    3) Caso contrário: erro explícito.

    Retorna:
        (caminho_do_banco: str, origem: str)  # origem ∈ {"Dropbox", "Local"}
    """
    db_local = _db_local_path()

    # 1) Dropbox
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
            else:
                st.warning("Banco baixado via token parece inválido (ou sem tabela 'usuarios').")
                st.caption(f"Debug: {_debug_file_info(candidate)}")
        except Exception as e:
            st.warning(f"Falha ao baixar via token do Dropbox: {e}")

    # 2) Local
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

    # 3) Erro
    info = _debug_file_info(db_local) if db_local.exists() else "(arquivo não existe)"
    st.error(
        "❌ Não foi possível obter um banco de dados válido.\n\n"
        "- Garanta um token **válido** (users/get_current_account = HTTP 200) em Secrets/ENVs, "
        "e `file_path` correto; **ou**\n"
        "- Coloque manualmente um SQLite válido em `data/flowdash_data.db` com a tabela 'usuarios'.\n"
        f"- Debug local: {info}"
    )
    st.stop()

# Flags efetivas: modo offline força token vazio
_DROPBOX_DISABLED = _flag_dropbox_disable()
_effective_token = "" if _DROPBOX_DISABLED else (ACCESS_TOKEN_CFG or "")
_effective_path = DROPBOX_PATH_CFG
_effective_force = FORCE_DOWNLOAD_CFG

# Recurso cacheado
_caminho_banco, _db_origem = ensure_db_available(_effective_token, _effective_path, _effective_force)

# ---- Auto PULL (antes de usar) ----
def _auto_pull_if_remote_newer():
    if _db_origem != "Dropbox" or not _effective_token:
        return
    meta = _dropbox_get_metadata(_effective_token, _effective_path)
    if not meta:
        return
    remote_ts = float(meta.get("server_ts") or 0.0)
    try:
        local_ts = os.path.getmtime(_caminho_banco)
    except Exception:
        local_ts = 0.0
    last_pull = float(st.session_state.get("_main_db_last_pull_ts") or 0.0)
    if remote_ts > max(local_ts, last_pull):
        try:
            _dropbox_download(_effective_token, _effective_path, _caminho_banco)
            st.session_state["_main_db_last_pull_ts"] = remote_ts
            st.toast("☁️ Main: banco atualizado do Dropbox.", icon="🔄")
            st.cache_data.clear()
        except Exception as e:
            st.warning(f"Main: não foi possível baixar DB remoto: {e}")

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
    # antes de parar, empurra alterações locais (se houve algum cadastro)
    def _auto_push_if_local_changed():
        if _db_origem != "Dropbox" or not _effective_token:
            return
        try:
            mtime = os.path.getmtime(_caminho_banco)
        except Exception:
            return
        last_sent = float(st.session_state.get("_main_db_last_push_ts") or 0.0)
        if mtime > (last_sent + 0.1):
            try:
                _dropbox_upload(_effective_token, _effective_path, _caminho_banco)
                st.session_state["_main_db_last_push_ts"] = mtime
                st.toast("☁️ Main: banco sincronizado com o Dropbox.", icon="✅")
            except Exception as e:
                st.warning(f"Main: falha ao enviar DB ao Dropbox: {e}")
    _auto_push_if_local_changed()
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

# ---- Auto PUSH (depois que a página executou) ----
def _auto_push_if_local_changed():
    if _db_origem != "Dropbox" or not _effective_token:
        return
    try:
        mtime = os.path.getmtime(_caminho_banco)
    except Exception:
        return
    last_sent = float(st.session_state.get("_main_db_last_push_ts") or 0.0)
    if mtime > (last_sent + 0.1):
        try:
            _dropbox_upload(_effective_token, _effective_path, _caminho_banco)
            st.session_state["_main_db_last_push_ts"] = mtime
            st.toast("☁️ Main: banco sincronizado com o Dropbox.", icon="✅")
        except Exception as e:
            st.warning(f"Main: falha ao enviar DB ao Dropbox: {e}")

_auto_push_if_local_changed()
