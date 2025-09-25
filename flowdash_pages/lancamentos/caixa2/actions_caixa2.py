# ===================== Actions: Caixa 2 =====================
"""
Resumo
------
Executa a transferência (Caixa/Caixa Vendas → Caixa 2), atualiza a linha do dia
em `saldos_caixas` e registra 1 (uma) linha no livro `movimentacoes_bancarias`.

Nova Lógica (2025-09)
---------------------
- Cada linha de `saldos_caixas` representa UMA data.
- Ao criar uma nova data:
    • Replica os campos da véspera (caixa, caixa_2, caixa_vendas, caixa2_dia).
    • Recalcula SEMPRE os totais por linha:
      - caixa_total  = caixa + caixa_vendas
      - caixa2_total = caixa_2 + caixa2_dia
    • Se não houver véspera, usa seeds (caixa/caixa_2) e zera os campos do dia.
- Se a linha do dia JÁ existir:
    • Não realinha baselines; apenas recalcula:
      caixa_total = caixa + caixa_vendas; caixa2_total = caixa_2 + caixa2_dia.
- Transferência: abate PRIMEIRO de `caixa_vendas`, depois de `caixa`. Se não houver
  saldo suficiente (caixa_vendas + caixa), retorna erro claro.

Observação padronizada no livro:
  "Lançamento Transferência p/ Caixa 2 | Valor=R$ X | C=R$ Y; CV=R$ Z"

Retorno
-------
TypedDict ResultadoTransferencia:
    ok, msg, valor, usar_caixa, usar_vendas
"""

from __future__ import annotations

import sqlite3
from typing import TypedDict, Any, Optional, Tuple

from shared.db import get_conn
from services.ledger.service_ledger_infra import log_mov_bancaria, _resolve_usuario

__all__ = ["transferir_para_caixa2", "_ensure_snapshot_do_dia", "_ensure_snapshot_herdado"]


# ===================== Tipos =====================
class ResultadoTransferencia(TypedDict):
    ok: bool
    msg: str
    valor: float
    usar_caixa: float
    usar_vendas: float


# ===================== Helpers =====================
def _r2(x: Any) -> float:
    """Arredonda para 2 casas, tolerando None/str e evitando -0.00."""
    try:
        v = round(float(x or 0.0), 2)
        if v == -0.0:
            v = 0.0
        return v
    except Exception:
        return 0.0


def fmt_brl(x: float) -> str:
    """Formata BRL para observação: 'R$ 1.234,56'."""
    s = f"{float(x):,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"R$ {s}"


def fmt_brl_md(x: float) -> str:
    """Formata BRL para mensagens em Streamlit: 'R\\$ 1.234,56'."""
    s = f"{float(x):,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"R\\$ {s}"


def _ler_iniciais_cadastro(conn: sqlite3.Connection) -> Tuple[float, float]:
    """
    Tenta ler (caixa_inicial, caixa2_inicial) de tabelas/colunas de Cadastro.
    Ajuste aqui os nomes reais se já tiver definidos. Fallback: 0.0, 0.0.
    """
    candidatos = [
        ("cadastro_caixas", "caixa_inicial", "caixa2_inicial"),
        ("cadastro_financeiro", "caixa_inicial", "caixa2_inicial"),
        ("cadastro", "valor_inicial_caixa", "valor_inicial_caixa2"),
        ("parametros", "caixa_inicial", "caixa2_inicial"),
    ]
    cur = conn.cursor()
    for tabela, c1, c2 in candidatos:
        try:
            row = cur.execute(
                f"SELECT {c1}, {c2} FROM {tabela} ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
            if row:
                return (_r2(row[0]), _r2(row[1]))
        except Exception:
            pass
    return 0.0, 0.0


# ===================== API =====================
def transferir_para_caixa2(
    caminho_banco: str,
    data_lanc,
    valor: float,
    usuario: Optional[Any] = None,
) -> ResultadoTransferencia:
    """
    Transfere recursos de `caixa`/`caixa_vendas` para o `caixa_2`.

    Processo:
      1. Garante linha do dia (replica a véspera se não existir; seeds se for 1º dia).
      2. Carrega a linha do dia e valida (caixa + caixa_vendas).
      3. Abate **primeiro `caixa_vendas`**, depois `caixa`.
      4. Atualiza campos e recalcula **totais por linha**.
      5. Registra 1 linha de ENTRADA em `movimentacoes_bancarias`.
    """
    if valor is None or float(valor) <= 0:
        raise ValueError("Valor inválido.")

    data_str = str(data_lanc)  # YYYY-MM-DD
    valor_f = _r2(valor)
    usuario_norm = _resolve_usuario(usuario)

    with get_conn(caminho_banco) as conn:
        # 1) Garante snapshot do dia segundo a regra atual
        _ensure_snapshot_do_dia(conn, data_str)

        cur = conn.cursor()
        # 2) Carrega a linha do dia
        row = cur.execute(
            """
            SELECT id, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total
              FROM saldos_caixas
             WHERE DATE(data) = DATE(?)
             ORDER BY id DESC
             LIMIT 1
            """,
            (data_str,),
        ).fetchone()

        if not row:
            raise RuntimeError("Falha ao garantir o snapshot do dia em saldos_caixas.")

        snap_id = row[0]
        caixa = _r2(row[1])
        caixa_2 = _r2(row[2])
        caixa_vendas = _r2(row[3])
        caixa2_dia = _r2(row[5])

        # 3) Validação de disponibilidade
        disponivel = _r2(caixa + caixa_vendas)
        if valor_f > disponivel:
            raise ValueError(
                f"Valor indisponível. Dinheiro disponível (caixa + vendas) é {fmt_brl_md(disponivel)}."
            )

        # 4) Abatimento (PRIORIDADE: VENDAS → CAIXA)
        usar_vendas = _r2(min(valor_f, caixa_vendas))
        restante = _r2(valor_f - usar_vendas)
        usar_caixa = _r2(min(restante, caixa))

        novo_caixa_vendas = _r2(caixa_vendas - usar_vendas)
        novo_caixa = _r2(caixa - usar_caixa)
        novo_caixa2_dia = _r2(caixa2_dia + valor_f)

        # Totais SEMPRE por linha
        novo_caixa_total = _r2(novo_caixa + novo_caixa_vendas)
        novo_caixa2_total = _r2(caixa_2 + novo_caixa2_dia)

        # 5) Persiste atualização
        cur.execute(
            """
            UPDATE saldos_caixas
               SET caixa=?,
                   caixa_vendas=?,
                   caixa_total=?,
                   caixa2_dia=?,
                   caixa2_total=?
             WHERE id=?
            """,
            (
                novo_caixa,
                novo_caixa_vendas,
                novo_caixa_total,
                novo_caixa2_dia,
                novo_caixa2_total,
                snap_id,
            ),
        )

        # Livro (1 linha, entrada em Caixa 2)
        observ = (
            "Lançamento Transferência p/ Caixa 2 | "
            f"Valor={fmt_brl(valor_f)} | "
            f"C={fmt_brl(usar_caixa)}; CV={fmt_brl(usar_vendas)}"
        )
        log_mov_bancaria(
            conn,
            data=data_str,
            banco="Caixa 2",
            tipo="entrada",
            valor=valor_f,
            origem="transferencia_caixa",
            observacao=observ,
            usuario=usuario_norm,
        )

        conn.commit()

    return {
        "ok": True,
        "msg": (
            "✅ Transferência para Caixa 2 registrada: "
            f"{fmt_brl_md(valor_f)} | "
            f"Origem → Caixa: {fmt_brl_md(usar_caixa)}, "
            f"Caixa Vendas: {fmt_brl_md(usar_vendas)}"
        ),
        "valor": valor_f,
        "usar_caixa": usar_caixa,
        "usar_vendas": usar_vendas,
    }


# ===================== Snapshot do Dia =====================
def _ensure_snapshot_do_dia(conn: sqlite3.Connection, data_str: str) -> None:
    """
    Garante a existência e o baseline correto da data `data_str` em `saldos_caixas`.

    Regras:
      - Se NÃO existir linha para a data:
          a) Se houver véspera, REPLICA os campos da véspera (caixa, caixa_2, caixa_vendas, caixa2_dia)
             e recalcula os **totais por linha**:
                caixa_total = caixa + caixa_vendas
                caixa2_total = caixa_2 + caixa2_dia
          b) Senão, usa seeds do Cadastro (caixa/caixa_2) com campos do dia zerados e
             recalcula os **totais por linha**.
      - Se JÁ existir linha para a data:
          • Não realinha baselines; apenas recalcula **totais por linha**.
    """
    data_str = str(data_str)
    cur = conn.cursor()

    # Linha do dia (se existir)
    dia = cur.execute(
        """
        SELECT id, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total
          FROM saldos_caixas
         WHERE DATE(data)=DATE(?)
         ORDER BY id DESC
         LIMIT 1
        """,
        (data_str,),
    ).fetchone()

    # Linha anterior (TODOS os campos necessários)
    prev = cur.execute(
        """
        SELECT caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total
          FROM saldos_caixas
         WHERE DATE(data) = (
            SELECT MAX(DATE(data))
              FROM saldos_caixas
             WHERE DATE(data) < DATE(?)
         )
         ORDER BY id DESC
         LIMIT 1
        """,
        (data_str,),
    ).fetchone()

    if not dia:
        # Criar a linha do dia replicando a véspera e normalizando totais por linha
        if prev:
            p_cx, p_cx2, p_v, _prev_ct, p_cx2_dia, _prev_c2t = map(_r2, prev)
            caixa = p_cx
            caixa_2 = p_cx2
            caixa_vendas = p_v
            caixa2_dia = p_cx2_dia
        else:
            seed_caixa, seed_caixa2 = _ler_iniciais_cadastro(conn)
            caixa = _r2(seed_caixa)
            caixa_2 = _r2(seed_caixa2)
            caixa_vendas = 0.0
            caixa2_dia = 0.0

        caixa_total = _r2(caixa + caixa_vendas)
        caixa2_total = _r2(caixa_2 + caixa2_dia)

        cur.execute(
            """
            INSERT INTO saldos_caixas
                (data, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total)
            VALUES (DATE(?), ?, ?, ?, ?, ?, ?)
            """,
            (data_str, caixa, caixa_2, caixa_vendas, caixa_total, caixa2_dia, caixa2_total),
        )
        return

    # Já existe linha do dia → apenas normaliza **totais por linha**
    dia_id, d_cx, d_cx2, d_v, _, d_cx2_dia, _ = dia
    d_cx = _r2(d_cx)
    d_cx2 = _r2(d_cx2)
    d_v = _r2(d_v)
    d_cx2_dia = _r2(d_cx2_dia)

    caixa_total = _r2(d_cx + d_v)
    caixa2_total = _r2(d_cx2 + d_cx2_dia)

    cur.execute(
        """
        UPDATE saldos_caixas
           SET caixa_total=?,
               caixa2_total=?
         WHERE id=?
        """,
        (caixa_total, caixa2_total, dia_id),
    )


# ========= Compatibilidade retroativa (alias) =========
def _ensure_snapshot_herdado(conn: sqlite3.Connection, data_str: str) -> None:
    """Alias antigo → chama a função nova."""
    _ensure_snapshot_do_dia(conn, data_str)
