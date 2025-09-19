# main.py
"""
FlowDash — Main App
===================
Ponto de entrada do aplicativo Streamlit do FlowDash.

Política do banco (SEM TEMPLATE por padrão):
  1) Tentar baixar via TOKEN do Dropbox (API) usando secrets/env:
       [dropbox]
       access_token   = "sl.ABC...SEU_TOKEN..."
       file_path      = "/FlowDash/data/flowdash_data.db"
       force_download = "0"           # "1" força re-download a cada start
  2) Se falhar ou não houver token/caminho, usar o DB local 'data/flowdash_data.db' se for válido (SQLite + 'usuarios').
  3) Se não houver DB válido, exibir erro e interromper a execução.
"""

from __future__ import annotations
import importlib
import inspect
import os
import pathlib
import sqlite3
import streamlit as st

from auth.auth import (
    validar_login,
    verificar_acesso,      # disponível dentro das páginas (mantido para uso interno)
    exibir_usuario_logado, # disponível dentro das páginas (mantido para uso interno)
    limpar_todas_as_paginas,
)
from utils.utils import garantir_trigger_totais_saldos_caixas
from shared.db_from_dropbox_api import ensure_local_db_api  # usa a API do Dropbox (TOKEN)

# ======================================================================================
# Configuração inicial da página
# ======================================================================================
st.set_page_config(page_title="FlowDash", layout="wide")

# ======================================================================================
# Helpers de diagnóstico
# ======================================================================================
def _debug_file_info(path: pathlib.Path) -> str:
    """Retorna tamanho e primeiros bytes do arquivo (para checar cabeçalho SQLite)."""
    try:
        size = path.stat().st_size
        with open(path, "rb") as f:
            head = f.read(64)
        return f"size=%s bytes, head=%r" % (size, head)
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

def _safe_get_secrets(section: str) -> dict:
    """Lê st.secrets[section] sem quebrar quando não existir (Cloud ou local)."""
    try:
        sec = st.secrets.get(section, {})
        return dict(sec) if isinstance(sec, dict) else {}
    except Exception:
        return {}

# ======================================================================================
# Infra de BD: TOKEN (Dropbox) opcional → LOCAL; sem template obrigatório
# ======================================================================================
@st.cache_resource(show_spinner=True)
def ensure_db_available() -> str:
    """
    1) Se houver TOKEN do Dropbox (secrets/env) e file_path: baixa via API para data/flowdash_data.db.
    2) Se download falhar ou não houver token/caminho: usa DB local se for válido.
    3) Caso contrário: mostra erro e interrompe.
    """
    db_local = _db_local_path()

    # 1) Tenta via TOKEN/API Dropbox (tolerante à ausência de secrets.toml)
    dbx_cfg = _safe_get_secrets("dropbox")
    access_token = (
        (dbx_cfg.get("access_token") or "").strip()
        or os.getenv("FLOWDASH_DBX_TOKEN", "").strip()
    )
    dropbox_path = (
        (dbx_cfg.get("file_path") or "").strip()
        or os.getenv("FLOWDASH_DBX_FILE", "/FlowDash/data/flowdash_data.db").strip()
    )
    force_download = (
        (str(dbx_cfg.get("force_download", "0")) == "1")
        or (os.getenv("FLOWDASH_FORCE_DB_DOWNLOAD", "0") == "1")
    )

    if access_token and dropbox_path:
        try:
            candidate_path = ensure_local_db_api(
                access_token=access_token,
                dropbox_path=dropbox_path,
                dest_path=str(db_local),
                force_download=force_download,
                validate_table="usuarios",  # garante login
            )
            candidate = pathlib.Path(candidate_path)
            if (
                candidate.exists()
                and candidate.stat().st_size > 0
                and _is_sqlite(candidate)
                and _has_table(candidate, "usuarios")
            ):
                st.session_state["db_source"] = "dropbox_token"
                os.environ["FLOWDASH_DB"] = str(candidate)  # exporta para módulos que usam ENV
                return str(candidate)
            else:
                info = _debug_file_info(candidate)
                st.warning("Banco baixado via token é inválido (ou sem tabela 'usuarios').")
                st.caption(f"Debug: {info}")
        except Exception as e:
            st.warning(f"Falha ao baixar via token do Dropbox: {e}")

    # 2) LOCAL válido?
    if (
        db_local.exists()
        and db_local.stat().st_size > 0
        and _is_sqlite(db_local)
        and _has_table(db_local, "usuarios")
    ):
        st.session_state["db_source"] = "local"
        os.environ["FLOWDASH_DB"] = str(db_local)
        return str(db_local)

    # 3) Nada válido → erro explícito e stop (não explode por falta de secrets)
    info = _debug_file_info(db_local) if db_local.exists() else "(arquivo não existe)"
    st.error(
        "❌ Não foi possível obter um banco de dados válido.\n\n"
        "- Preencha `dropbox.access_token` e `dropbox.file_path` nos *secrets* (ou variáveis de ambiente) **ou**\n"
        "- Coloque manualmente um SQLite válido em `data/flowdash_data.db` contendo a tabela 'usuarios'.\n"
        f"- Debug local: {info}"
    )
    st.stop()

# Caminho do banco (garantido ou interrompe)
caminho_banco = ensure_db_available()

# Mostra de onde veio o banco (útil no deploy)
st.caption(f"🗃️ Banco em uso: **{st.session_state.get('db_source', '?')}** → `{caminho_banco}`")

# Disponibiliza caminho para os módulos
st.session_state.setdefault("caminho_banco", caminho_banco)

# Infra mínima de BD (idempotente)
try:
    garantir_trigger_totais_saldos_caixas(caminho_banco)
except Exception as e:
    st.warning(f"Trigger de totais não criada: {e}")

# ======================================================================================
# Estado de sessão
# ======================================================================================
if "usuario_logado" not in st.session_state:
    st.session_state.usuario_logado = None
if "pagina_atual" not in st.session_state:
    st.session_state.pagina_atual = "📊 Dashboard"

# ======================================================================================
# Roteamento (import dinâmico + injeção de caminho_banco)
# ======================================================================================
def _call_page(module_path: str):
    try:
        mod = importlib.import_module(module_path)
    except Exception as e:
        st.error(f"Falha ao importar módulo '{module_path}': {e}")
        return

    def _invoke(fn):
        sig = inspect.signature(fn)
        args: list = []
        kwargs: dict = {}

        ss = st.session_state
        usuario_logado = ss.get("usuario_logado")
        known = {
            "usuario": usuario_logado,
            "usuario_logado": usuario_logado,
            "perfil": (usuario_logado or {}).get("perfil") if usuario_logado else None,
            "pagina_atual": ss.get("pagina_atual"),
            "ir_para_formulario": ss.get("ir_para_formulario"),
            "caminho_banco": caminho_banco,
        }

        for p in sig.parameters.values():
            name = p.name
            kind = p.kind
            has_default = (p.default is not inspect._empty)

            if name == "caminho_banco":
                args.append(caminho_banco)
                continue

            if name in known:
                val = known[name]
                if kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
                    args.append(val)
                else:
                    kwargs[name] = val
                continue

            if name in ss:
                val = ss[name]
                if kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
                    args.append(val)
                else:
                    kwargs[name] = val
                continue

            if not has_default:
                if kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
                    args.append(None)
                else:
                    kwargs[name] = None

        return fn(*args, **kwargs)

    seg = module_path.rsplit(".", 1)[-1]
    parent = module_path.rsplit(".", 2)[-2] if "." in module_path else ""
    tail = seg.split("_", 1)[1] if "_" in seg else seg

    base = ["render", "page", "main", "pagina", "show", "pagina_fechamento_caixa"]
    derived = [
        f"render_{tail}",
        "render_page",
        f"render_{seg}",
        f"render_{parent}",
        f"page_{tail}",
        f"show_{tail}",
        seg,
    ]

    tried = set()
    for fn_name in base + derived:
        if fn_name in tried or not hasattr(mod, fn_name):
            tried.add(fn_name)
            continue
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

# ======================================================================================
# LOGIN
# ======================================================================================
if not st.session_state.usuario_logado:
    st.title("🔐 Login")
    with st.form("form_login"):
        email = st.text_input("Email")
        senha = st.text_input("Senha", type="password")
        submitted = st.form_submit_button("Entrar")
        if submitted:
            usuario = validar_login(email, senha, caminho_banco)
            if usuario:
                st.session_state.usuario_logado = usuario
                st.session_state.pagina_atual = (
                    "📊 Dashboard" if usuario["perfil"] in ("Administrador", "Gerente") else "🧾 Lançamentos"
                )
                limpar_todas_as_paginas()
                st.rerun()
            else:
                st.error("❌ Email ou senha inválidos, ou usuário inativo.")
    st.stop()

# ======================================================================================
# Sidebar: usuário + navegação (sem o botão "Nova Venda")
# ======================================================================================
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
        "📥 Entradas",
        "📤 Saídas",
        "📦 Mercadorias",
        "💳 Fatura Cartão de Crédito",
        "📄 Contas a Pagar",
        "🏦 Empréstimos/Financiamentos",
    ]:
        if st.button(title, use_container_width=True):
            st.session_state.pagina_atual = title
            st.rerun()

if perfil == "Administrador":
    with st.sidebar.expander("🛠️ Cadastros", expanded=False):
        for title in [
            "👥 Usuários",
            "🎯 Cadastro de Metas",
            "⚙️ Taxas Maquinetas",
            "📇 Cartão de Crédito",
            "💵 Caixa",
            "🛠️ Correção de Caixa",
            "🏦 Saldos Bancários",
            "🏛️ Cadastro de Empréstimos",
            "🏦 Cadastro de Bancos",
            "📂 Cadastro de Saídas",
        ]:
            if st.button(title, use_container_width=True):
                st.session_state.pagina_atual = title
                st.rerun()

# ======================================================================================
# Título + Roteamento
# ======================================================================================
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
