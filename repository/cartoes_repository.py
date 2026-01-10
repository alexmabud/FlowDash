# repository/cartoes_repository.py
"""
Módulo Cartões (Repositório)
============================

Este módulo define a classe `CartoesRepository`, responsável por acessar e
gerenciar a tabela **`cartoes_credito`** no SQLite. Centraliza operações de
consulta e validação de configuração dos cartões usados em lançamentos de
crédito (vencimento e fechamento).

Funcionalidades principais
--------------------------
- Validação de configuração de cartão (dia de vencimento e dias de fechamento).
- Consulta de cartão por nome → retorna `(vencimento_dia, dias_fechamento)`.
- Listagem de nomes de cartões (ordenada e normalizada).

Detalhes técnicos
-----------------
- Conexão SQLite configurada com:
  - `PRAGMA journal_mode=WAL;`
  - `PRAGMA busy_timeout=30000;`
  - `PRAGMA foreign_keys=ON;`
- Comparações de nome **case/trim-insensitive** no SQL.
- Não altera schema nem dados; foco em leitura e validação.

Dependências
------------
- sqlite3
- typing (Optional, Tuple, List)
"""

import sqlite3
from typing import Optional, Tuple, List

from utils.utils import formatar_moeda as _fmt_brl
from shared.db import get_conn


class CartoesRepository:
    """
    Repositório para operações de leitura/validação sobre `cartoes_credito`.

    Parâmetros:
        db_path (str): Caminho do arquivo SQLite.
    """
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _get_conn(self) -> sqlite3.Connection:
        """Abre conexão SQLite com configuração centralizada."""
        return get_conn(self.db_path)

    def _validar_conf(self, vencimento_dia: int, dias_fechamento: int) -> None:
        """
        Valida parâmetros de configuração de fatura:

        - `vencimento_dia`: 1..31 (o ajuste para último dia do mês ocorre no uso).
        - `dias_fechamento`: 0..28 (dias ANTES do vencimento em que fecha).
        """
        if not (1 <= int(vencimento_dia) <= 31):
            raise ValueError(f"vencimento inválido ({vencimento_dia}); use 1..31")
        if not (0 <= int(dias_fechamento) <= 28):
            raise ValueError(f"fechamento inválido ({dias_fechamento}); use 0..28 (dias antes do vencimento)")

    def obter_por_nome(self, nome: str) -> Optional[Tuple[int, int]]:
        """
        Retorna `(vencimento_dia, dias_fechamento)` do cartão, ou `None` se não existir.
        """
        if not nome or not nome.strip():
            return None
        with self._get_conn() as conn:
            row = conn.execute(
                """
                SELECT vencimento, fechamento
                  FROM cartoes_credito
                 WHERE LOWER(TRIM(nome)) = LOWER(TRIM(?))
                 LIMIT 1
                """,
                (nome,),
            ).fetchone()
            if not row:
                return None

            vencimento_dia = int(row[0] if row[0] is not None else 0)
            dias_fechamento = int(row[1] if row[1] is not None else 0)
            self._validar_conf(vencimento_dia, dias_fechamento)
            return (vencimento_dia, dias_fechamento)

    def listar_nomes(self) -> List[str]:
        """Lista nomes de cartões cadastrados, ordenados alfabeticamente."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT nome FROM cartoes_credito ORDER BY LOWER(TRIM(nome)) ASC"
            ).fetchall()
            return [r[0] for r in rows]


# ----------------------------
# FUNÇÃO EXTRA — fora da classe
# ----------------------------
def listar_destinos_fatura_em_aberto(db_path: str):
    """
    Lista faturas de cartão **em aberto** (uma por cartão+competência) para a UI.

    Regra (simples e direta ao ponto):
        saldo = valor_evento - COALESCE(valor, 0)

    Onde:
        - `valor_evento` = valor de face da fatura (competência)
        - `valor`        = principal já coberto acumulado (coluna atualizada no Passo 3)

    Retorna list[dict] com:
        - label (str)            -> "Fatura {cartao} • Venc. {dd/mm} • Em aberto R$ {saldo}"
        - cartao (str)
        - competencia (str, YYYY-MM)
        - vencimento (str, YYYY-MM-DD)
        - valor_evento (float)
        - obrigacao_id (int)
        - saldo (float)          -> quanto falta pagar (principal)
    """
    from datetime import datetime

    def _fmt_dm(v: str) -> str:
        try:
            return datetime.strptime(v, "%Y-%m-%d").strftime("%d/%m")
        except Exception:
            return v or "—"

    conn = get_conn(db_path)
    try:

        rows = conn.execute(
            """
            WITH lanc AS (
                SELECT
                    obrigacao_id,
                    TRIM(credor)                AS cartao,
                    competencia,
                    DATE(vencimento)            AS vencimento,
                    COALESCE(valor_evento, 0)   AS valor_evento,
                    COALESCE(valor, 0)          AS principal_coberto
                FROM contas_a_pagar_mov
                WHERE tipo_obrigacao = 'FATURA_CARTAO'
                  AND categoria_evento = 'LANCAMENTO'
                  AND COALESCE(credor,'') <> ''
                  AND COALESCE(competencia,'') <> ''
            )
            SELECT
                l.obrigacao_id,
                l.cartao,
                l.competencia,
                l.vencimento,
                l.valor_evento,
                ROUND(l.valor_evento - l.principal_coberto, 2) AS saldo
            FROM lanc l
            /* somente cartões cadastrados, comparando por nome (case/trim-insensitive) */
            JOIN cartoes_credito c
              ON LOWER(TRIM(c.nome)) = LOWER(TRIM(l.cartao))
            WHERE (l.valor_evento - l.principal_coberto) > 0.00001
            ORDER BY DATE(l.vencimento) ASC, LOWER(TRIM(l.cartao)) ASC;
            """
        ).fetchall()
    finally:
        conn.close()

    itens = []
    for r in rows:
        cartao       = r["cartao"] or ""
        comp         = r["competencia"] or ""
        vcto         = r["vencimento"] or ""
        valor_evt    = float(r["valor_evento"] or 0.0)
        saldo_aberto = float(r["saldo"] or 0.0)

        label = f"Fatura {cartao} • Venc. {_fmt_dm(vcto)} • Em aberto {_fmt_brl(saldo_aberto)}"
        itens.append({
            "label": label,
            "cartao": cartao,
            "competencia": comp,
            "vencimento": vcto,
            "valor_evento": valor_evt,
            "obrigacao_id": int(r["obrigacao_id"]),
            "saldo": saldo_aberto,
        })
    return itens


# (Opcional) API pública explícita
__all__ = ["CartoesRepository", "listar_destinos_fatura_em_aberto"]
