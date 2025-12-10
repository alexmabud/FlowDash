# main.py
"""
FlowDash — Main App
===================
Ponto de entrada do aplicativo Streamlit do FlowDash.

Resumo
------
App com controle de login/perfil, roteamento dinâmico de páginas e sincronização
do banco via Dropbox (SDK com refresh_token), com fallback local/legado.
Este arquivo foi otimizado para reduzir latência entre trocas de página.

Estratégia de performance
-------------------------
- Cache de resolução de páginas (import/lib + descoberta de função) para evitar
  introspecção repetida a cada navegação.
- Throttle no "auto pull" do Dropbox (no máx. 1x a cada 60s) para evitar
  bloqueios de rede frequentes.
- Unificação do "auto push" (envio se mtime mudou).
- Debug lazy (importa `requests` só quando DEBUG ativo).
- Execução de garantias/infra apenas 1x por sessão.

Política do banco (ordem de tentativa)
--------------------------------------
1) SDK (refresh) — shared.dbx_io.baixar_db_para_local() (force_download opcional)
2) Legado (HTTP) — shared.db_from_dropbox_api.ensure_local_db_api() [opcional]
3) Local — data/flowdash_data.db (precisa ter tabela 'usuarios')
4) Erro claro

Flags úteis
-----------
- DEBUG:    FLOWDASH_DEBUG=1  ou  [dropbox].debug="1"
- OFFLINE:  DROPBOX_DISABLE=1  ou  [dropbox].disable="1"
- force_download (secrets/env): força pull do remoto antes de usar
"""

from __future__ import annotations

import importlib
import inspect
import os
import pathlib
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from typing import Callable, Optional

import streamlit as st
import streamlit as st
# CookieManager removido em favor de Query Params (mais robusto no Cloud)
# from streamlit_autorefresh import st_autorefresh

from auth.auth import (
    validar_login,
    obter_usuario,
    verificar_acesso,
    exibir_usuario_logado,
    limpar_todas_as_paginas,
    criar_sessao,
    validar_sessao,
    encerrar_sessao,
)
from utils.utils import garantir_trigger_totais_saldos_caixas, fechar_sidebar_automaticamente

# Legado (opcional, só para bootstrap com access_token curto)
from shared.db_from_dropbox_api import ensure_local_db_api

# Config
from shared.dropbox_config import load_dropbox_settings, mask_token

# SDK com refresh token (pull/push)
from shared.dbx_io import enviar_db_local, baixar_db_para_local
from shared.dropbox_client import get_dbx  # para ler metadata (SDK)

from shared.branding import sidebar_brand, page_header, login_brand


# -----------------------------------------------------------------------------
# Config inicial
# -----------------------------------------------------------------------------
# Define estado inicial da sidebar (padrão expanded, mas collapsed ao navegar)
initial_sidebar = st.session_state.get("sidebar_state", "expanded")
st.set_page_config(page_title="FlowDash", layout="wide", initial_sidebar_state=initial_sidebar)

# Reseta para o próximo ciclo (opcional, para que F5 volte ao normal se desejado, 
# mas aqui vamos manter o controle explícito na navegação)


# === Logos ===
_LOGO_LOGIN_SIDEBAR_PATH = "assets/flowdash1.png"
_LOGO_HEADER_PATH        = "assets/flowdash2.PNG"

# ==============================================================================
# COOKIE MANAGER (Persistência)
# ==============================================================================
# ==============================================================================
# COOKIE MANAGER (Removido: Usando st.query_params)
# ==============================================================================
# cookie_manager = stx.CookieManager(key="flowdash_main_cookies")

def aplicar_branding(is_login: bool = False) -> None:
    """
    Aplica as logos do FlowDash em toda execução (sem flags),
    para não sumirem ao navegar entre páginas.
    """
    try:
        if is_login:
            # Tela de login
            login_brand(custom_path=_LOGO_LOGIN_SIDEBAR_PATH, height_px=230, show_title=False)
            return

        # Após login (sempre redesenha)
        sidebar_brand(custom_path=_LOGO_LOGIN_SIDEBAR_PATH, height_px=200)
        page_header(custom_path=_LOGO_HEADER_PATH, logo_height_px=130, show_title=False)

    except Exception as e:
        st.caption(f"[branding] aviso: {e}")


# -----------------------------------------------------------------------------
# Helpers gerais (IO e sessão)
# -----------------------------------------------------------------------------
def _debug_file_info(path: pathlib.Path) -> str:
    """Retorna resumo do arquivo (tamanho + primeiros bytes) para diagnósticos."""
    try:
        size = path.stat().st_size
        with open(path, "rb") as f:
            head = f.read(16)
        return f"size={size}B, head={head!r}"
    except Exception as e:
        return f"(falha ao inspecionar: {e})"


def _is_sqlite(path: pathlib.Path) -> bool:
    """Verifica se o arquivo tem header de SQLite."""
    try:
        with open(path, "rb") as f:
            return f.read(16).startswith(b"SQLite format 3")
    except Exception:
        return False


def _has_table(path: pathlib.Path, table: str) -> bool:
    """Checa existência de uma tabela no SQLite."""
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
    """Caminho padrão para data/flowdash_data.db (garante diretório)."""
    root = pathlib.Path(__file__).resolve().parent
    p = root / "data" / "flowdash_data.db"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _flag_debug() -> bool:
    """DEBUG via secrets/env."""
    try:
        sec = dict(st.secrets.get("dropbox", {}))
        if str(sec.get("debug", "")).strip().lower() in {"1", "true", "yes", "y", "on"}:
            return True
    except Exception:
        pass
    return str(os.getenv("FLOWDASH_DEBUG", "")).strip().lower() in {"1", "true", "yes", "y", "on"}


def _flag_dropbox_disable() -> bool:
    """Desabilita Dropbox via secrets/env (modo offline)."""
    try:
        sec = dict(st.secrets.get("dropbox", {}))
        if str(sec.get("disable", "")).strip().lower() in {"1", "true", "yes", "y", "on"}:
            return True
    except Exception:
        pass
    return str(os.getenv("DROPBOX_DISABLE", "")).strip().lower() in {"1", "true", "yes", "y", "on"}


def _now_ts() -> float:
    """Timestamp epoch (float)."""
    return time.time()


def _throttle(key: str, min_seconds: int) -> bool:
    """
    Retorna True no máx. 1x por `min_seconds`, guardado em session_state[key].
    Usado para evitar auto-pull muito frequente.
    """
    last = float(st.session_state.get(key) or 0.0)
    now = _now_ts()
    if (now - last) >= float(min_seconds):
        st.session_state[key] = now
        return True
    return False


# -----------------------------------------------------------------------------
# Diagnóstico/Config Dropbox
# -----------------------------------------------------------------------------
_DEBUG = _flag_debug()
_cfg = load_dropbox_settings(prefer_env_first=True)

ACCESS_TOKEN_CFG = _cfg.get("access_token") or ""
DROPBOX_PATH_CFG = _cfg.get("file_path") or "/FlowDash/data/flowdash_data.db"
FORCE_DOWNLOAD_CFG = str(_cfg.get("force_download", "0")).strip().lower() in {"1", "true", "yes", "on"}
TOKEN_SOURCE_CFG = _cfg.get("token_source", "none")
_DROPBOX_DISABLED = _flag_dropbox_disable()

if _DEBUG:
    # Importa requests apenas em modo debug (lazy) para reduzir overhead em prod
    import requests  # type: ignore

    with st.expander("🔎 Diagnóstico Dropbox (temporário)", expanded=True):
        try:
            try:
                st.write("st.secrets keys:", list(st.secrets.keys()))
                st.write("Tem seção [dropbox] nos Secrets?", "dropbox" in st.secrets)
            except Exception:
                st.write("st.secrets indisponível (ok em CLI/local).")

            st.write("token_source:", TOKEN_SOURCE_CFG)
            st.write("access_token (mascarado):", mask_token(str(ACCESS_TOKEN_CFG)))
            st.write("token_length:", len(str(ACCESS_TOKEN_CFG)))
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
# Banco: Dropbox -> Local (SDK preferencial; legado opcional)  [Cacheado]
# -----------------------------------------------------------------------------
@st.cache_resource(show_spinner=True)
def ensure_db_available(
    access_token: str,
    dropbox_path: str,
    force_download: bool,
    dropbox_disabled: bool,
) -> tuple[str, str]:
    """
    Garante um SQLite válido e retorna (caminho_local, origem_label).

    Ordem:
      1) SDK (refresh)  -> baixar_db_para_local()
      2) Legado HTTP    -> ensure_local_db_api() [se houver token]
      3) Local          -> data/flowdash_data.db
      4) Erro -> st.stop()

    Returns
    -------
    (path, origem) onde origem ∈ {"Dropbox", "Local"}.
    """
    db_local = _db_local_path()

    # 1) Preferencial: SDK com refresh_token (se não estiver desabilitado)
    if not dropbox_disabled:
        try:
            _ = baixar_db_para_local()
            candidate = pathlib.Path(_)
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

    # 2) Legado (HTTP) — útil só pra migração/debug
    if (not dropbox_disabled) and access_token and dropbox_path:
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


# Flags efetivas (congeladas na cache_resource acima)
_effective_token = "" if _DROPBOX_DISABLED else (ACCESS_TOKEN_CFG or "")
_effective_path = DROPBOX_PATH_CFG
_effective_force = FORCE_DOWNLOAD_CFG
_caminho_banco, _db_origem = ensure_db_available(
    str(_effective_token), str(_effective_path), _effective_force, _DROPBOX_DISABLED
)


# -----------------------------------------------------------------------------
# Auto PULL com throttle (antes de usar) — SDK
# -----------------------------------------------------------------------------
_PULL_THROTTLE_SECONDS = 60  # mínimo entre checagens remotas

def _auto_pull_if_remote_newer() -> None:
    """Sincroniza do Dropbox para local se remoto estiver mais novo."""
    if _db_origem != "Dropbox" or _DROPBOX_DISABLED:
        return

    # throttle global
    if not _throttle("_pull_last_check_ts", _PULL_THROTTLE_SECONDS):
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
        if not remote_dt:
            return
        # normaliza para UTC
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
st.caption(f"🗃️ Banco em uso: **{_db_origem}**")


# -----------------------------------------------------------------------------
# Garantias/infra mínimas — DESATIVADO no boot
# -----------------------------------------------------------------------------
# ⚠️ Importante: esta chamada estava causando INSERT em `saldos_caixas`
# já na tela de login. A criação/alteração de linha deve ocorrer APENAS
# nas actions (salvar operação). Se precisar reativar futuramente,
# torne a função "pura" (apenas DDL das triggers) e chame após login.
#
# if not st.session_state.get("_infra_trigger_ok"):
#     try:
#         garantir_trigger_totais_saldos_caixas(_caminho_banco)
#         st.session_state["_infra_trigger_ok"] = True
#     except Exception as e:
#         st.warning(f"Trigger de totais não criada: {e}")


# -----------------------------------------------------------------------------
# Auto PUSH (definido ANTES do bloco de login para evitar NameError)
# -----------------------------------------------------------------------------
def _auto_push_if_local_changed() -> None:
    """Envia DB local para o Dropbox se detectado mtime maior que último push."""
    if _db_origem != "Dropbox" or _DROPBOX_DISABLED:
        return
    try:
        mtime = os.path.getmtime(_caminho_banco)
    except Exception:
        return
    last_sent = float(st.session_state.get("_main_db_last_push_ts") or 0.0)
    if mtime > (last_sent + 0.1):
        try:
            enviar_db_local()
            st.session_state["_main_db_last_push_ts"] = mtime
            st.toast("☁️ Main: banco sincronizado com o Dropbox.", icon="✅")
        except Exception as e:
            st.warning(f"Main: falha ao enviar DB ao Dropbox (refresh): {e}")


# -----------------------------------------------------------------------------
# Estado de sessão base
# -----------------------------------------------------------------------------
if "usuario_logado" not in st.session_state:
    st.session_state.usuario_logado = None
if "pagina_atual" not in st.session_state:
    st.session_state.pagina_atual = "📊 Dashboard"


# -----------------------------------------------------------------------------
# Roteamento (resolução de página com cache) 
# -----------------------------------------------------------------------------
def _inject_args(fn: Callable) -> object:
    """
    Injeta argumentos conhecidos na assinatura da função de página.
    Mantém o comportamento anterior, mas sem introspecção repetida.
    """
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


@st.cache_resource
def _resolve_page_callable(module_path: str) -> Optional[Callable]:
    """
    Importa o módulo e resolve a função de renderização mais provável.
    Cacheado para evitar custo em toda troca de página.
    """
    try:
        mod = importlib.import_module(module_path)
    except Exception:
        return None

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
            tried.add(fn_name)
            continue
        tried.add(fn_name)
        fn = getattr(mod, fn_name)
        if callable(fn):
            return fn

    # último fallback: primeira função que comece com prefixos conhecidos
    for prefix in ("pagina_", "render_"):
        for name, obj in vars(mod).items():
            if callable(obj) and name.startswith(prefix):
                return obj
    return None


def _call_page(module_path: str) -> None:
    """Executa a função cacheada da página, com injeção de argumentos padrão."""
    fn = _resolve_page_callable(module_path)
    if not fn:
        st.warning(f"O módulo '{module_path}' não possui função compatível (render/page/main/pagina*/show).")
        return
    try:
        _inject_args(fn)
    except Exception as e:
        st.error(f"Erro ao executar {module_path}.{getattr(fn, '__name__', '<?>')}: {e}")


# -----------------------------------------------------------------------------
# Login
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Login (visual igual ao PDV: título centralizado + formulário estreito)
# -----------------------------------------------------------------------------
if not st.session_state.usuario_logado:
    # 1. TENTA RECUPERAR SESSÃO VIA URL (Query Param)
    # Isso funciona 100% no Streamlit Online
    try:
        params = st.query_params
        token = params.get("session")
        if token:
            usr_sessao = validar_sessao(token, _caminho_banco)
            if usr_sessao:
                st.session_state.usuario_logado = usr_sessao
                st.session_state.pagina_atual = (
                    "📊 Dashboard" if usr_sessao["perfil"] in ("Administrador", "Gerente") else "🧾 Lançamentos"
                )
                limpar_todas_as_paginas()
                st.rerun()
            else:
                # Token inválido ou expirado: limpa a URL
                st.query_params.clear()
    except Exception:
        pass

if not st.session_state.usuario_logado:
    aplicar_branding(is_login=True)

    # Coluna central para título + formulário
    left, center, right = st.columns([1, 1.15, 1])
    with center:
        # Título centralizado
        st.markdown("<h1 style='text-align:center; margin:0 0 .5rem;'>🔐 Login</h1>", unsafe_allow_html=True)

        # Cartão do formulário com largura fixa/ideal
        st.markdown(
            """
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
            """,
            unsafe_allow_html=True,
        )

        with st.form("form_login"):
            email = st.text_input("Email")
            senha = st.text_input("Senha", type="password")
            entrar = st.form_submit_button("Entrar")

        if entrar:
            usuario = validar_login(email, senha, _caminho_banco)
            if usuario:
                # Cria sessão e adiciona na URL
                token = criar_sessao(usuario["email"], _caminho_banco)
                if token:
                    st.query_params["session"] = token
                
                st.session_state.usuario_logado = usuario
                st.session_state.pagina_atual = (
                    "📊 Dashboard" if usuario["perfil"] in ("Administrador", "Gerente") else "🧾 Lançamentos"
                )
                limpar_todas_as_paginas()
                st.rerun()
            else:
                st.error("❌ Email ou senha inválidos, ou usuário inativo.")

    _auto_push_if_local_changed()
    st.stop()



# -----------------------------------------------------------------------------
# Sidebar / Navegação
# -----------------------------------------------------------------------------
# ⬅️ APLICA BRANDING APÓS LOGIN (sidebar + header)
aplicar_branding()

usuario = st.session_state.get("usuario_logado")
if usuario is None:
    st.warning("Faça login para continuar.")
    st.stop()

# ATUALIZAÇÃO AUTOMÁTICA DE DADOS (30s)
# Isso garantirá que o _auto_pull rode periodicamente para buscar novidades
# st_autorefresh(interval=30000, limit=None, key="main_autorefresh")

perfil = usuario["perfil"]
st.sidebar.markdown(f"👤 **{usuario['nome']}**\n🔐 Perfil: `{perfil}`")

if st.sidebar.button("🚪 Sair", use_container_width=True):
    # Encerra sessão no banco e limpa URL
    if usuario and usuario.get("email"):
        encerrar_sessao(usuario["email"], _caminho_banco)
    
    st.query_params.clear()
    limpar_todas_as_paginas()
    st.session_state.usuario_logado = None
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("## 🧭 Menu de Navegação")
for title in ["📊 Dashboard", "📉 DRE", "🧾 Lançamentos", "💼 Fechamento de Caixa", "🎯 Metas"]:
    if st.sidebar.button(title, use_container_width=True):
        st.session_state.pagina_atual = title
        st.session_state["sidebar_state"] = "collapsed"
        st.rerun()

with st.sidebar.expander("📋 DataFrames", expanded=False):
    for title in [
        "📘 Livro Caixa","📥 Entradas", "📤 Saídas", "📦 Mercadorias",
        "💳 Fatura Cartão de Crédito", "📄 Contas a Pagar", "🏦 Empréstimos/Financiamentos"
    ]:
        if st.button(title, use_container_width=True):
            st.session_state.pagina_atual = title
            st.session_state["sidebar_state"] = "collapsed"
            st.rerun()

if perfil == "Administrador":
    with st.sidebar.expander("🛠️ Cadastros", expanded=False):
        for title in [
            "👥 Usuários", "🎯 Cadastro de Metas", "⚙️ Taxas Maquinetas", "📇 Cartão de Crédito", "💵 Caixa",
            "🛠️ Correção de Caixa", "🏦 Saldos Bancários", "🏛️ Cadastro de Empréstimos",
            "🏦 Cadastro de Bancos", "📂 Cadastro de Saídas","🧮 Variáveis do DRE"
        ]:
            if st.button(title, use_container_width=True):
                st.session_state.pagina_atual = title
                st.session_state["sidebar_state"] = "collapsed"
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
    "📘 Livro Caixa": "flowdash_pages.dataframes.livro_caixa",
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
    "🧮 Variáveis do DRE": "flowdash_pages.cadastros.variaveis_dre"
}

PERMISSOES = {
    "📊 Dashboard": {"Administrador", "Gerente"},
    "📉 DRE": {"Administrador", "Gerente"},
    "🧾 Lançamentos": {"Administrador", "Gerente", "Vendedor"},
    "💼 Fechamento de Caixa": {"Administrador", "Gerente"},
    "🎯 Metas": {"Administrador", "Gerente"},
    "📘 Livro Caixa": {"Administrador", "Gerente"},
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
    "🧮 Variáveis do DRE": {"Administrador"}
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


# -----------------------------------------------------------------------------
# Auto PUSH (depois da página) — SDK + refresh
# -----------------------------------------------------------------------------
_auto_push_if_local_changed()

# -----------------------------------------------------------------------------
# Hybrid Auto-Close: Garante fechamento via JS se o nativo falhar
# -----------------------------------------------------------------------------
# Passamos key=time.time() para forçar o componente a ser recriado a cada rerun
fechar_sidebar_automaticamente(key=str(time.time()))
