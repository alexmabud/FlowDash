# main.py
"""
FlowDash — Main App
===================
Ponto de entrada do aplicativo Streamlit do FlowDash.

Este app baixa o banco via **API do Dropbox** usando TOKEN (sem link público).
- Secrets esperados:
    [dropbox]
    access_token = "sl.ABC...SEU_TOKEN..."
    file_path    = "/FlowDash/data/flowdash_data.db"
    force_download = "0"
- Sem uso de template no Dropbox. Fallback é LOCAL:
    - se o download falhar, usa data/flowdash_data.db local (se existir e for válido)
    - se não existir, cria um arquivo vazio e tenta provisionar tabela 'usuarios'
      a partir do template LOCAL do repo (data/flowdash_template.db), apenas para boot.
"""

from __future__ import annotations
import importlib
import inspect
import os
import pathlib
import shutil
import sqlite3
import streamlit as st

from auth.auth import (
    validar_login,
    verificar_acesso,           # disponível dentro das páginas
    exibir_usuario_logado,      # disponível dentro das páginas
    limpar_todas_as_paginas,
)
from utils.utils import garantir_trigger_totais_saldos_caixas
from shared.db_from_dropbox_api import ensure_local_db_api  # ← SOMENTE TOKEN/API


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


def _list_tables_sqlite(path: pathlib.Path) -> list[str]:
    """Lista tabelas existentes num arquivo SQLite (apenas para debug)."""
    try:
        with sqlite3.connect(str(path)) as conn:
            cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY 1;")
            return [r[0] for r in cur.fetchall()]
    except Exception:
        return []


# ======================================================================================
# Infra de BD: baixa via TOKEN (API Dropbox) ou cai no LOCAL
# ======================================================================================
def _db_local_path() -> str:
    root = pathlib.Path(__file__).resolve().parent
    p = root / "data" / "flowdash_data.db"
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)


def _is_sqlite(path: pathlib.Path) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(16).startswith(b"SQLite format 3")
    except Exception:
        return False


def _has_table(path: pathlib.Path, table: str) -> bool:
    try:
        with sqlite3.connect(str(path)) as conn:
            cur = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (table,))
            return cur.fetchone() is not None
    except Exception:
        return False


def _table_exists(db_path: str, table: str) -> bool:
    return _has_table(pathlib.Path(db_path), table)


def _create_table_from_local_template(db_path: str, table: str) -> None:
    """
    Copia DDL/índices/triggers da tabela a partir do template LOCAL do repo
    (data/flowdash_template.db) — apenas para o app conseguir subir.
    """
    template_path = pathlib.Path(__file__).resolve().parent / "data" / "flowdash_template.db"
    if not template_path.exists():
        return
    with sqlite3.connect(str(template_path)) as tconn, sqlite3.connect(str(db_path)) as dconn:
        tconn.row_factory = sqlite3.Row
        # tabela
        row = tconn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?;", (table,)
        ).fetchone()
        if row and row["sql"]:
            dconn.execute(row["sql"])
        # índices
        for r in tconn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL;", (table,)
        ):
            if r["sql"]:
                dconn.execute(r["sql"])
        # triggers
        for r in tconn.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? AND sql IS NOT NULL;", (table,)
        ):
            if r["sql"]:
                dconn.execute(r["sql"])
        dconn.commit()


def ensure_required_tables(db_path: str) -> None:
    """Garante a existência das tabelas essenciais para login etc."""
    if not _table_exists(db_path, "usuarios"):
        _create_table_from_local_template(db_path, "usuarios")


@st.cache_resource(show_spinner=True)
def ensure_db_available() -> str:
    """
    Política:
      1) Se houver TOKEN do Dropbox (st.secrets/env), baixar via API (file_path) e promover.
      2) Se falhar ou não houver token, usar o DB LOCAL se for SQLite válido.
      3) Senão, criar arquivo vazio LOCAL e provisionar a tabela 'usuarios' do template local (se existir).
    """
    db_local = pathlib.Path(_db_local_path())

    # 1) TOKEN/API Dropbox
    access_token = (
        st.secrets.get("dropbox", {}).get("access_token", "").strip()
        or os.getenv("FLOWDASH_DBX_TOKEN", "").strip()
    )
    dropbox_path = (
        st.secrets.get("dropbox", {}).get("file_path", "/FlowDash/data/flowdash_data.db").strip()
        or os.getenv("FLOWDASH_DBX_FILE", "/FlowDash/data/flowdash_data.db").strip()
    )
    force_download = (
        (st.secrets.get("dropbox", {}).get("force_download", "0") == "1")
        or (os.getenv("FLOWDASH_FORCE_DB_DOWNLOAD", "0") == "1")
    )

    if access_token:
        try:
            candidate_path = ensure_local_db_api(
                access_token=access_token,
                dropbox_path=dropbox_path,
                dest_path=str(db_local),
                force_download=force_download,
                validate_table="usuarios",
            )
            candidate = pathlib.Path(candidate_path)
            if candidate.exists() and candidate.stat().st_size > 0 and _is_sqlite(candidate) and _has_table(candidate, "usuarios"):
                st.session_state["db_source"] = "dropbox_token"
                return str(candidate)
            else:
                info = _debug_file_info(candidate)
                tables = _list_tables_sqlite(candidate)
                st.warning(
                    "Arquivo baixado via token não é SQLite válido ou está sem a tabela 'usuarios'.\n"
                    f"Debug: {info}\nTabelas detectadas: {tables}"
                )
        except Exception as e:
            st.warning(f"Falha ao baixar banco via token do Dropbox: {e}")

    # 2) LOCAL válido?
    if db_local.exists() and db_local.stat().st_size > 0 and _is_sqlite(db_local) and _has_table(db_local, "usuarios"):
        st.session_state["db_source"] = "local"
        return str(db_local)

    # 3) Criar vazio e provisionar 'usuarios' do template LOCAL (se houver)
    try:
        db_local.parent.mkdir(parents=True, exist_ok=True)
        if not db_local.exists():
            db_local.touch()
        ensure_required_tables(str(db_local))
        st.session_state["db_source"] = "vazio"
    except Exception as e:
        st.error(f"Falha ao criar/provisionar DB local: {e}")
        st.session_state["db_source"] = "erro"

    return str(db_local)


# Caminho do banco e preparação mínima
caminho_banco = ensure_db_available()
ensure_required_tables(caminho_banco)  # se baixou vazio, garante 'usuarios'

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
                args.append(caminho_banco); continue

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
    derived = [f"render_{tail}", "render_page", f"render_{seg}", f"render_{parent}",
               f"page_{tail}", f"show_{tail}", seg]

    tried = set()
    for fn_name in base + derived:
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
                    "📊 Dashboard" if usuario["perfil"] in ("Administrador", "Gerente")
                    else "🧾 Lançamentos"
                )
                limpar_todas_as_paginas()
                st.rerun()
            else:
                st.error("❌ Email ou senha inválidos, ou usuário inativo.")
    st.stop()


# ======================================================================================
# Sidebar: usuário + navegação
# ======================================================================================
usuario = st.session_state.get("usuario_logado")
if usuario is None:
    st.warning("Faça login para continuar.")
    st.stop()

perfil = usuario["perfil"]
st.sidebar.markdown(f"👤 **{usuario['nome']}**\n🔐 Perfil: `{perfil}`")

if st.sidebar.button("🚪 Sair", use_container_width=True):
    limpar_todas_as_paginas(); st.session_state.usuario_logado = None; st.rerun()

st.sidebar.markdown("---")
if st.sidebar.button("➕ Nova Venda", key="nova_venda", use_container_width=True):
    st.session_state.pagina_atual = "🧾 Lançamentos"
    st.session_state.ir_para_formulario = True
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("## 🧭 Menu de Navegação")
for title in ["📊 Dashboard","📉 DRE","🧾 Lançamentos","💼 Fechamento de Caixa","🎯 Metas"]:
    if st.sidebar.button(title, use_container_width=True):
        st.session_state.pagina_atual = title; st.rerun()

with st.sidebar.expander("📋 DataFrames", expanded=False):
    for title in ["📥 Entradas","📤 Saídas","📦 Mercadorias","💳 Fatura Cartão de Crédito","📄 Contas a Pagar","🏦 Empréstimos/Financiamentos"]:
        if st.button(title, use_container_width=True):
            st.session_state.pagina_atual = title; st.rerun()

if perfil == "Administrador":
    with st.sidebar.expander("🛠️ Cadastros", expanded=False):
        for title in [
            "👥 Usuários","🎯 Cadastro de Metas","⚙️ Taxas Maquinetas","📇 Cartão de Crédito",
            "💵 Caixa","🛠️ Correção de Caixa","🏦 Saldos Bancários","🏛️ Cadastro de Empréstimos",
            "🏦 Cadastro de Bancos","📂 Cadastro de Saídas",
        ]:
            if st.button(title, use_container_width=True):
                st.session_state.pagina_atual = title; st.rerun()


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
