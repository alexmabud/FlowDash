
import sqlite3
import pandas as pd

from shared.db import get_conn

# ============================
# Whitelist de Tabelas Seguras
# ============================

# Tabelas permitidas no banco (proteção contra SQL injection)
_TABELAS_PERMITIDAS = {
    "mercadorias",
    "usuarios",
    "correcao_caixa",
    "fechamento_caixa",
    "compras",
    "contas_a_pagar",
    "cartoes_credito",
    "saldos_bancos",
    "metas",
    "fatura_cartao",
    "saida",
    "saldos_caixas",
    "emprestimos_financiamentos",
    "taxas_maquinas",
    "entrada",
    "movimentacoes",
    "variaveis_dre",
}

# ============================
# Função Genérica
# ============================

def carregar_tabela(nome_tabela: str, caminho_banco: str) -> pd.DataFrame:
    """
    Carrega qualquer tabela do banco de dados SQLite como DataFrame.

    Args:
        nome_tabela (str): Nome da tabela.
        caminho_banco (str): Caminho do banco de dados .db.

    Returns:
        pd.DataFrame: Dados da tabela ou DataFrame vazio em caso de erro.

    Raises:
        ValueError: Se o nome da tabela não estiver na whitelist de tabelas permitidas.

    Security:
        Usa whitelist para prevenir SQL injection. Apenas tabelas em _TABELAS_PERMITIDAS
        podem ser consultadas.
    """
    # Validação de segurança: whitelist de tabelas
    nome_normalizado = (nome_tabela or "").strip().lower()

    if not nome_normalizado:
        print("[ERRO] Nome de tabela vazio")
        return pd.DataFrame()

    if nome_normalizado not in _TABELAS_PERMITIDAS:
        print(f"[ERRO SEGURANÇA] Tentativa de acesso a tabela não permitida: '{nome_tabela}'")
        raise ValueError(f"Tabela '{nome_tabela}' não está na whitelist de tabelas permitidas")

    try:
        with get_conn(caminho_banco) as conn:
            # Seguro usar f-string aqui pois nome_normalizado foi validado pela whitelist
            return pd.read_sql(f"SELECT * FROM {nome_normalizado}", conn)
    except Exception as e:
        print(f"[ERRO] Não foi possível carregar a tabela '{nome_normalizado}': {e}")
        return pd.DataFrame()

# ============================
# Funções específicas por tabela
# ============================

def carregar_mercadorias(caminho_banco: str) -> pd.DataFrame:
    """Carrega a tabela de mercadorias."""
    return carregar_tabela("mercadorias", caminho_banco)

def carregar_usuarios(caminho_banco: str) -> pd.DataFrame:
    """Carrega os usuários do sistema."""
    return carregar_tabela("usuarios", caminho_banco)

def carregar_correcoes_caixa(caminho_banco: str) -> pd.DataFrame:
    """Carrega as correções manuais de caixa."""
    return carregar_tabela("correcao_caixa", caminho_banco)

def carregar_fechamento_caixa(caminho_banco: str) -> pd.DataFrame:
    """Carrega os registros de fechamento de caixa."""
    return carregar_tabela("fechamento_caixa", caminho_banco)

def carregar_compras(caminho_banco: str) -> pd.DataFrame:
    """Carrega os registros da tabela de compras."""
    return carregar_tabela("compras", caminho_banco)

def carregar_contas_a_pagar(caminho_banco: str) -> pd.DataFrame:
    """Carrega as contas a pagar."""
    return carregar_tabela("contas_a_pagar", caminho_banco)

def carregar_cartoes_credito(caminho_banco: str) -> pd.DataFrame:
    """Carrega os cartões de crédito cadastrados."""
    return carregar_tabela("cartoes_credito", caminho_banco)

def carregar_saldos_bancos(caminho_banco: str) -> pd.DataFrame:
    """Carrega os saldos dos bancos."""
    return carregar_tabela("saldos_bancos", caminho_banco)

def carregar_metas(caminho_banco: str) -> pd.DataFrame:
    """Carrega as metas cadastradas."""
    return carregar_tabela("metas", caminho_banco)

def carregar_fatura_cartao(caminho_banco: str) -> pd.DataFrame:
    """Carrega as faturas de cartões de crédito."""
    return carregar_tabela("fatura_cartao", caminho_banco)

def carregar_saidas(caminho_banco: str) -> pd.DataFrame:
    """Carrega os lançamentos de saída."""
    return carregar_tabela("saida", caminho_banco)

def carregar_saldos_caixa(caminho_banco: str) -> pd.DataFrame:
    """Carrega os saldos de caixa (caixa e caixa 2)."""
    return carregar_tabela("saldos_caixas", caminho_banco)

def carregar_emprestimos_financiamentos(caminho_banco: str) -> pd.DataFrame:
    """Carrega empréstimos e financiamentos."""
    return carregar_tabela("emprestimos_financiamentos", caminho_banco)

def carregar_taxas_maquinas(caminho_banco: str) -> pd.DataFrame:
    """Carrega as taxas das máquinas de cartão."""
    return carregar_tabela("taxas_maquinas", caminho_banco)

def carregar_entradas(caminho_banco: str) -> pd.DataFrame:
    """Carrega os lançamentos de entrada."""
    return carregar_tabela("entrada", caminho_banco)