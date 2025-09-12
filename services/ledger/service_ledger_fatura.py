"""
Fatura de cartão (pagamento).

Pagamento de fatura de cartão de crédito (`FATURA_CARTAO`), com suporte a
multa/juros/desconto, atualização de saldos (caixa/banco) e registros
contábeis (`saida`, `movimentacoes_bancarias`, CAP).
"""

from __future__ import annotations

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import logging
import os
import sys
from datetime import datetime
from typing import Optional, Tuple

# Garante que a raiz do projeto (<raiz>/services/ledger/..) esteja no sys.path
_CURRENT_DIR = os.path.dirname(__file__)
_PROJECT_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Internos
from shared.db import get_conn  # noqa: E402
from shared.ids import sanitize  # noqa: E402
from services.ledger.service_ledger_infra import (  # noqa: E402
    _fmt_obs_saida,
    log_mov_bancaria,
)

logger = logging.getLogger(__name__)

__all__ = ["_FaturaLedgerMixin"]


def _cap_exprs(cur):
    """
    Descobre quais colunas existem em contas_a_pagar_mov e devolve
    expressões SQL seguras para multa e desconto, evitando 'no such column'.
    """
    cols = {r[1] for r in cur.execute("PRAGMA table_info(contas_a_pagar_mov)").fetchall()}

    # multa: preferir multa_paga, senão multa_pago; senão 0.0
    if "multa_paga" in cols:
        multa_expr = "COALESCE(multa_paga,0.0)"
    elif "multa_pago" in cols:
        multa_expr = "COALESCE(multa_pago,0.0)"
    else:
        multa_expr = "0.0"

    # desconto: preferir desconto, senão desconto_aplicado; senão 0.0
    if "desconto" in cols:
        desc_expr = "COALESCE(desconto,0.0)"
    elif "desconto_aplicado" in cols:
        desc_expr = "COALESCE(desconto_aplicado,0.0)"
    else:
        desc_expr = "0.0"

    # principal (coluna 'valor' pode não existir)
    valor_exists = "valor" in cols
    return multa_expr, desc_expr, valor_exists


class _FaturaLedgerMixin:
    """Mixin de regras para pagamento de fatura de cartão."""

    def pagar_fatura_cartao(
        self,
        *,
        data: str,
        valor: float,                 # valor BASE que deseja pagar (sem ajustes)
        forma_pagamento: str,         # "DINHEIRO" | "PIX" | "DÉBITO"
        origem: str,                  # "Caixa"/"Caixa 2" OU nome do banco
        obrigacao_id: int,
        usuario: str,
        categoria: Optional[str] = "Fatura Cartão de Crédito",
        sub_categoria: Optional[str] = None,
        descricao: Optional[str] = None,
        trans_uid: Optional[str] = None,
        multa: float = 0.0,
        juros: float = 0.0,
        desconto: float = 0.0,
        retornar_info: bool = False,  # <-- retorna (restante, status) se True
    ) -> Tuple[int, int, int]:
        # Sanitização/normalização
        v_pg    = max(0.0, float(valor))
        v_multa = max(0.0, float(multa or 0.0))
        v_juros = max(0.0, float(juros or 0.0))
        v_desc  = max(0.0, float(desconto or 0.0))

        cat  = sanitize(categoria)
        sub  = sanitize(sub_categoria)
        desc = sanitize(descricao)
        usu  = sanitize(usuario)
        org  = sanitize(origem)

        eps = 0.005  # tolerância

        with get_conn(self.db_path) as conn:
            cur = conn.cursor()
            multa_expr, desc_expr, valor_exists = _cap_exprs(cur)

            # 1) Localiza a parcela/LANCAMENTO base
            sql_base = f"""
                SELECT
                    id,
                    COALESCE(valor_evento,0.0)     AS valor_parcela,
                    COALESCE(valor_pago_acumulado,0.0) AS vpa,
                    COALESCE(juros_pago,0.0)       AS juros_pago,
                    {multa_expr}                   AS multa_pago,
                    {desc_expr}                    AS desconto_aplicado
                    {", valor AS principal_col" if valor_exists else ", NULL AS principal_col"}
                  FROM contas_a_pagar_mov
                 WHERE obrigacao_id = ?
                   AND categoria_evento = 'LANCAMENTO'
                   AND tipo_obrigacao = 'FATURA_CARTAO'
                 LIMIT 1
            """
            row = cur.execute(sql_base, (int(obrigacao_id),)).fetchone()
            if not row:
                raise ValueError(f"Fatura (obrigacao_id={obrigacao_id}) não encontrada.")

            parcela_id           = int(row["id"])
            valor_parcela        = float(row["valor_parcela"])
            vpa_atual            = float(row["vpa"])
            juros_pago_atual     = float(row["juros_pago"])
            multa_pago_atual     = float(row["multa_pago"])
            desconto_aplic_atual = float(row["desconto_aplicado"])
            principal_col        = row["principal_col"]

            # 2) PRINCIPAL já pago até aqui:
            if principal_col is not None:
                principal_pago_ate_agora = max(0.0, float(principal_col))
            else:
                principal_pago_ate_agora = max(
                    0.0,
                    vpa_atual - juros_pago_atual - multa_pago_atual + desconto_aplic_atual,
                )

            # 3) Restante do principal desta parcela
            restante_antes = max(0.0, valor_parcela - principal_pago_ate_agora)

            # Valor base aplicado ao principal
            principal_a_pagar = min(v_pg, restante_antes)

            # LÍQUIDO que sai do caixa/banco (nunca negativo)
            total_saida = round(principal_a_pagar + v_juros + v_multa - v_desc, 2)
            if total_saida < 0:
                total_saida = 0.0

            # IDs padrão
            id_saida  = -1
            id_mov    = -1

            # 4) Observação para log (somente formato de texto)
            forma_txt = (forma_pagamento if total_saida > eps else "AJUSTE")
            valor_txt = float(total_saida if total_saida > eps else 0.0)
            if (desc or "").strip().upper().startswith("PAGAMENTO"):
                # Formato solicitado: "Lançamento SAÍDA <FORMA> R$<valor> • <descricao>"
                obs = f"Lançamento SAÍDA {forma_txt} R${valor_txt:.2f} • {desc.strip()}"
            else:
                # Formato padrão legado
                obs = _fmt_obs_saida(
                    forma=forma_txt,
                    valor=valor_txt,
                    categoria=cat,
                    subcategoria=sub,
                    descricao=desc,
                    banco=(org if (total_saida > eps and forma_pagamento == "DÉBITO") else None),
                )

            # 5) Efeito financeiro — só quando houver saída (> 0)
            if total_saida > eps:
                if forma_pagamento == "DINHEIRO":
                    self._garantir_linha_saldos_caixas(conn, data)
                    col_map = {"Caixa": "caixa", "Caixa 2": "caixa_2"}
                    col = col_map.get(org)
                    if not col:
                        raise ValueError(f"Origem de dinheiro inválida para DINHEIRO: {org}")

                    # saida
                    cur.execute(
                        """
                        INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                           Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                           Origem_Dinheiro, Banco_Saida)
                        VALUES (?, ?, ?, ?, 'DINHEIRO', 1, ?, ?, ?, '')
                        """,
                        (data, cat, sub, desc, float(total_saida), usu, org),
                    )
                    id_saida = int(cur.lastrowid)

                    # saldo caixa
                    cur.execute(
                        f"UPDATE saldos_caixas SET {col} = COALESCE({col},0) - ? WHERE data = ?",
                        (float(total_saida), data),
                    )

                    # log
                    id_mov = log_mov_bancaria(
                        conn,
                        data=data,
                        banco=org,
                        tipo="saida",
                        valor=float(total_saida),
                        origem="saidas_fatura_pagamento",
                        observacao=obs,
                        usuario=usu,
                        referencia_tabela="saida",
                        referencia_id=id_saida,
                        trans_uid=trans_uid,
                        data_hora=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    )
                else:
                    # Bancos
                    try:
                        self._garantir_linha_saldos_bancos(conn, data)
                    except Exception:
                        pass

                    self._ajustar_banco_dynamic(conn, banco_col=org, delta=-float(total_saida), data=data)

                    cur.execute(
                        """
                        INSERT INTO saida (Data, Categoria, Sub_Categoria, Descricao,
                                           Forma_de_Pagamento, Parcelas, Valor, Usuario,
                                           Origem_Dinheiro, Banco_Saida)
                        VALUES (?, ?, ?, ?, ?, 1, ?, ?, '', ?)
                        """,
                        (data, cat, sub, desc, forma_pagamento, float(total_saida), usu, org),
                    )
                    id_saida = int(cur.lastrowid)

                    id_mov = log_mov_bancaria(
                        conn,
                        data=data,
                        banco=org,
                        tipo="saida",
                        valor=float(total_saida),
                        origem="saidas_fatura_pagamento",
                        observacao=obs,
                        usuario=usu,
                        referencia_tabela="saida",
                        referencia_id=id_saida,
                        trans_uid=trans_uid,
                        data_hora=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    )

                # anotação extra quando pediu mais do que o restante
                if v_pg > restante_antes + eps and id_mov != -1:
                    cur.execute(
                        """
                        UPDATE movimentacoes_bancarias
                           SET observacao = COALESCE(observacao,'') || ' [valor ajustado ao restante: R$ ' || printf('%.2f', ?) || ']'
                         WHERE id = ?
                        """,
                        (float(total_saida), id_mov),
                    )

            # 6) APLICAÇÃO NA CAP — PARCIAL/TOTAL
            res = self.cap_repo.aplicar_pagamento_parcela(
                conn,
                parcela_id=int(parcela_id),
                valor_pagamento=float(principal_a_pagar),
                juros=float(v_juros),
                multa=float(v_multa),
                desconto=float(v_desc),
                data_evento=data,
                forma_pagamento=forma_pagamento,
                origem=org,
                ledger_id=(id_saida if id_saida != -1 else 0),
                usuario=usu,
            )
            evento_id = int((res.get("id_evento_cap") or -1)) if isinstance(res, dict) else -1

            # 7) Coletar restante/status para a UI (robusto)
            if isinstance(res, dict) and res.get("restante") is not None and res.get("status") is not None:
                restante = float(res.get("restante"))
                status   = str(res.get("status"))
            else:
                sql_back = f"""
                    SELECT COALESCE(valor_evento,0.0)     AS valor_parcela,
                           COALESCE(valor_pago_acumulado,0.0) AS vpa,
                           COALESCE(juros_pago,0.0)       AS juros_pago,
                           {multa_expr}                   AS multa_pago,
                           {desc_expr}                    AS desconto_aplicado
                           {", valor AS principal_col" if valor_exists else ", NULL AS principal_col"}
                      FROM contas_a_pagar_mov
                     WHERE id = ?
                     LIMIT 1
                """
                row2 = cur.execute(sql_back, (parcela_id,)).fetchone()
                if row2:
                    v_evt = float(row2["valor_parcela"])
                    vpa2  = float(row2["vpa"])
                    j2    = float(row2["juros_pago"])
                    m2    = float(row2["multa_pago"])
                    d2    = float(row2["desconto_aplicado"])
                    pcol  = row2["principal_col"]
                    if pcol is not None:
                        principal_pago = max(0.0, float(pcol))
                    else:
                        principal_pago = max(0.0, vpa2 - j2 - m2 + d2)

                    restante_calc = max(0.0, v_evt - principal_pago)
                    restante = round(restante_calc, 2)
                    if restante <= eps:
                        status = "QUITADO"
                    elif principal_pago > 0:
                        status = "Parcial"
                    else:
                        status = "Em aberto"
                else:
                    restante = 0.0
                    status = "Em aberto"

            conn.commit()

        logger.debug(
            "pagar_fatura_cartao: obrig=%s total_saida=%.2f principal_aplicado=%.2f forma=%s origem=%s saida=%s mov=%s evento=%s restante=%.2f status=%s",
            obrigacao_id,
            total_saida,
            principal_a_pagar,
            forma_pagamento,
            org,
            id_saida,
            id_mov,
            evento_id,
            float(restante or 0.0),
            status,
        )

        if retornar_info:
            # (id_saida, id_mov, evento_id, restante, status)
            return (id_saida, id_mov, evento_id, float(restante), str(status))
        # compat: retorno antigo
        return (id_saida, id_mov, evento_id)
