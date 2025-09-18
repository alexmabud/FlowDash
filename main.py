# main.py
"""
FlowDash — Main App
===================
Ponto de entrada do aplicativo Streamlit do FlowDash.
"""

from __future__ import annotations
import importlib
import inspect
import os
import pathlib
import shutil
import sqlite3
import requests  # baixar DB do OneDrive
import streamlit as st

from auth.auth import (
    validar_login,
    verificar_acesso,           # disponível para uso dentro das páginas
    exibir_usuario_logado,      # disponível para uso dentro das páginas
    limpar_todas_as_paginas,
)
from utils.utils import garantir_trigger_totais_saldos_caixas


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
        return f"size={size} bytes, head={head!r}"
    except Exception as e:
        return f"(falha ao inspecionar: {e})"


def _list_tables_sqlite(path: pathlib.Path) -> list[str]:
    """Lista tabelas existentes num arquivo SQLite."""
    try:
        with sqlite3.connect(str(path)) as conn:
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY 1;"
            )
            return [r[0] for r in cur.fetchall()]
    except Exception:
        return []


# ======================================================================================
# Infra de BD para Cloud: baixa do OneDrive (via st.secrets) ou cai no template
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


def _has_required_tables(path: pathlib.Path) -> bool:
    """Verifica se existe a tabela 'usuarios' — usada no login."""
    try:
        with sqlite3.connect(str(path)) as conn:
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='usuarios';"
            )
            return cur.fetchone() is not None
    except Exception:
        return False


@st.cache_resource(show_spinner=True)
def ensure_db_available() -> str:
    """
    1) Usa data/flowdash_data.db se existir e for válido (SQLite + tabela 'usuarios').
    2) Senão, baixa do OneDrive (st.secrets['onedrive']['shared_download_url']) e valida.
    3) Se falhar, copia o template (data/flowdash_template.db).
    """
    db_local = pathlib.Path(_db_local_path())
    tpl = pathlib.Path(__file__).resolve().parent / "data" / "flowdash_template.db"

    # 1) Já existe local e é válido?
    if db_local.exists() and db_local.stat().st_size > 0 and _is_sqlite(db_local) and _has_required_tables(db_local):
        st.session_state["db_source"] = "local"
        return str(db_local)

    # 2) Tentar OneDrive (link de compartilhamento somente leitura)
    try:
        url = st.secrets.get("onedrive", {}).get("shared_download_url", "")
        if url:
            tmp = db_local.with_suffix(".tmp")
            with requests.get(url, stream=True, timeout=60, allow_redirects=True) as r:
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            # valida e promove
            if tmp.exists() and tmp.stat().st_size > 0 and _is_sqlite(tmp) and _has_required_tables(tmp):
                shutil.move(tmp, db_local)
                st.session_state["db_source"] = "onedrive"
                return str(db_local)
            else:
                # 🔎 Diagnóstico: mostrar por que não validou
                info = _debug_file_info(tmp)
                tables = _list_tables_sqlite(tmp)
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
                st.warning(
                    "Arquivo do OneDrive não é SQLite válido ou está sem a tabela 'usuarios'.\n"
                    f"Debug: {info}\n"
                    f"Tabelas detectadas: {tables}"
                )
    except Exception as e:
        st.warning(f"Falha ao baixar banco do OneDrive: {e}")

    # 3) Fallback: template (para forks/testes)
    try:
        if tpl.exists():
            shutil.copy2(tpl, db_local)
            st.session_state["db_source"] = "template"
        else:
            db_local.touch()
            st.session_state["db_source"] = "vazio"
    except Exception as e:
        st.error(f"Falha no fallback para o template: {e}")
        st.session_state["db_source"] = "erro"

    return str(db_local)


# --- garantir a tabela 'usuarios' a partir do template, se faltar ---
def _table_exists(db_path: str, table: str) -> bool:
    try:
        with sqlite3.connect(db_path) as conn:
            cur = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;",
                (table,)
            )
            return cur.fetchone() is not None
    except Exception:
        return False


def _create_table_from_template(db_path: str, template_path: str, table: str) -> None:
    """Copia o DDL da tabela (e índices/triggers) do template para o DB ativo, se existir lá."""
    tpl = pathlib.Path(template_path)
    if not tpl.exists():
        return
    with sqlite3.connect(template_path) as tconn, sqlite3.connect(db_path) as dconn:
        tconn.row_factory = sqlite3.Row
        # tabela
        row = tconn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?;",
            (table,)
        ).fetchone()
        if row and row["sql"]:
            dconn.execute(row["sql"])
        # índices
        for r in tconn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL;",
            (table,)
        ):
            if r["sql"]:
                dconn.execute(r["sql"])
        # triggers
        for r in tconn.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? AND sql IS NOT NULL;",
            (table,)
        ):
            if r["sql"]:
                dconn.execute(r["sql"])
        dconn.commit()


def ensure_required_tables(db_path: str) -> None:
    """Garante a existência das tabelas essenciais para o app iniciar (ex.: 'usuarios')."""
    tpl = pathlib.Path(__file__).resolve().parent / "data" / "flowdash_template.db"
    if not _table_exists(db_path, "usuarios"):
        _create_table_from_template(db_path, str(tpl), "usuarios")


# Caminho do banco de dados (garantido)
caminho_banco = ensure_db_available()
ensure_required_tables(caminho_banco)  # <- garante 'usuarios' se faltar
os.makedirs("data", exist_ok=True)

# Mostra de onde veio o banco (útil no deploy)
st.caption(f"🗃️ Banco em uso: **{st.session_state.get('db_source', '?')}** → `{caminho_banco}`")

# 👉 torna o caminho visível para todos os módulos (Metas/DataFrames)
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
# Helper de roteamento — importa módulo e chama render/page/main (+ fallbacks)
# ======================================================================================
def _call_page(module_path: str):
    """
    Importa o módulo indicado e tenta chamar, nesta ordem, a função:

    - genéricas: render, page, main, pagina, show, pagina_fechamento_caixa
    - derivadas do nome do arquivo: render_<tail>, render_page, render_<seg>, render_<parent>,
      page_<tail>, show_<tail>, <seg> (se for callable)
    - fallbacks: 1ª função que comece com 'pagina_' ou 'render_'

    Suporta parâmetros:
      - sempre fornece 'caminho_banco' se a função aceitar;
      - para outros parâmetros OBRIGATÓRIOS, usa valores do session_state se existirem;
        caso contrário preenche com None (posicionais quando possível).
    """
    try:
        mod = importlib.import_module(module_path)
    except Exception as e:
        st.error(f"Falha ao importar módulo '{module_path}': {e}")
        return

    def _invoke(fn):
        sig = inspect.signature(fn)
        args = []
        kwargs = {}

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

        # 1) passa caminho_banco como posicional se existir
            if name == "caminho_banco":
                args.append(caminho_banco)
                continue

        # 2) valores conhecidos/estado
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

        # 3) obrigatórios sem default → None
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
    limpar_todas_as_paginas()
    st.session_state.usuario_logado = None
    st.rerun()

st.sidebar.markdown("---")

if st.sidebar.button("➕ Nova Venda", key="nova_venda", use_container_width=True):
    st.session_state.pagina_atual = "🧾 Lançamentos"
    st.session_state.ir_para_formulario = True
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("## 🧭 Menu de Navegação")

if st.sidebar.button("📊 Dashboard", use_container_width=True):
    st.session_state.pagina_atual = "📊 Dashboard"
    st.rerun()

if st.sidebar.button("📉 DRE", use_container_width=True):
    st.session_state.pagina_atual = "📉 DRE"
    st.rerun()

if st.sidebar.button("🧾 Lançamentos", use_container_width=True):
    st.session_state.pagina_atual = "🧾 Lançamentos"
    st.rerun()

if st.sidebar.button("💼 Fechamento de Caixa", use_container_width=True):
    st.session_state.pagina_atual = "💼 Fechamento de Caixa"
    st.rerun()

if st.sidebar.button("🎯 Metas", use_container_width=True):
    st.session_state.pagina_atual = "🎯 Metas"
    st.rerun()

with st.sidebar.expander("📋 DataFrames", expanded=False):
    if st.button("📥 Entradas", use_container_width=True):
        st.session_state.pagina_atual = "📥 Entradas"
        st.rerun()
    if st.button("📤 Saídas", use_container_width=True):
        st.session_state.pagina_atual = "📤 Saídas"
        st.rerun()
    if st.button("📦 Mercadorias", use_container_width=True):
        st.session_state.pagina_atual = "📦 Mercadorias"
        st.rerun()
    if st.button("💳 Fatura Cartão de Crédito", use_container_width=True):
        st.session_state.pagina_atual = "💳 Fatura Cartão de Crédito"
        st.rerun()
    if st.button("📄 Contas a Pagar", use_container_width=True):
        st.session_state.pagina_atual = "📄 Contas a Pagar"
        st.rerun()
    if st.button("🏦 Empréstimos/Financiamentos", use_container_width=True):
        st.session_state.pagina_atual = "🏦 Empréstimos/Financiamentos"
        st.rerun()

if perfil == "Administrador":
    with st.sidebar.expander("🛠️ Cadastros", expanded=False):
        if st.button("👥 Usuários", use_container_width=True):
            st.session_state.pagina_atual = "👥 Usuários"
            st.rerun()
        if st.button("🎯 Cadastro de Metas", use_container_width=True):
            st.session_state.pagina_atual = "🎯 Cadastro de Metas"
            st.rerun()
        if st.button("⚙️ Taxas Maquinetas", use_container_width=True):
            st.session_state.pagina_atual = "⚙️ Taxas Maquinetas"
            st.rerun()
        if st.button("📇 Cartão de Crédito", use_container_width=True):
            st.session_state.pagina_atual = "📇 Cartão de Crédito"
            st.rerun()
        if st.button("💵 Caixa", use_container_width=True):
            st.session_state.pagina_atual = "💵 Caixa"
            st.rerun()
        if st.button("🛠️ Correção de Caixa", use_container_width=True):
            st.session_state.pagina_atual = "🛠️ Correção de Caixa"
            st.rerun()
        if st.button("🏦 Saldos Bancários", use_container_width=True):
            st.session_state.pagina_atual = "🏦 Saldos Bancários"
            st.rerun()
        if st.button("🏛️ Cadastro de Empréstimos", use_container_width=True):
            st.session_state.pagina_atual = "🏛️ Cadastro de Empréstimos"
            st.rerun()
        if st.button("🏦 Cadastro de Bancos", use_container_width=True):
            st.session_state.pagina_atual = "🏦 Cadastro de Bancos"
            st.rerun()
        if st.button("📂 Cadastro de Saídas", use_container_width=True):
            st.session_state.pagina_atual = "📂 Cadastro de Saídas"
            st.rerun()


# ======================================================================================
# Título principal
# ======================================================================================
st.title(st.session_state.pagina_atual)


# ======================================================================================
# Roteamento
# ======================================================================================
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
