# -*- coding: utf-8 -*-
"""
Actions: Página de Lançamentos (READ-ONLY)
=========================================

Resumo
------
Consulta o SQLite e calcula os dados do resumo do dia **sem efeitos colaterais**.
Trocar a data no calendário NÃO cria linha em `saldos_caixas`.

Regras
------
- Vendas: soma SOMENTE vendas cujo DATE(Data) = dia selecionado.
- Formas tratadas como "venda": DINHEIRO, PIX, DÉBITO/DEBITO, CRÉDITO/CREDITO,
  LINK_PAGAMENTO (variações).
- Caixas: usa os totais **da linha da data** em `saldos_caixas` (sem somatórios).
- Saídas do dia: por DATE(data).
- Saldos de bancos: acumulado <= data.
- Nenhum INSERT/UPDATE aqui. Apenas SELECT.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd
from shared.db import get_conn
from flowdash_pages.finance_logic import _get_saldos_bancos_acumulados, _get_bancos_ativos

# --------------------- Formas de pagamento tratadas como "venda" ---------------------
_FORMAS_VENDA = {
    "DINHEIRO", "PIX",
    "DÉBITO", "DEBITO",
    "CRÉDITO", "CREDITO",
    "LINK_PAGAMENTO", "LINK PAGAMENTO", "LINK-DE-PAGAMENTO", "LINK DE PAGAMENTO",
}

def _total_vendas_por_data(conn, data_str: str) -> float:
    """
    Soma entrada.Valor das vendas do dia, filtrando por **Data** (AAAA-MM-DD).
    Linhas com Data vazia/NULL não entram no total.
    """
    try:
        formas = tuple(_FORMAS_VENDA)
        placeholders = ",".join("?" for _ in formas)
        sql = f"""
            SELECT COALESCE(SUM(CAST(Valor AS REAL)), 0.0)
              FROM entrada
             WHERE UPPER(COALESCE(Forma_de_Pagamento,'')) IN ({placeholders})
               AND Data IS NOT NULL
               AND TRIM(Data) <> ''
               AND DATE(Data) = DATE(?)
        """
        cur = conn.execute(sql, list(formas) + [data_str])
        val = cur.fetchone()[0]
        return float(val or 0.0)
    except Exception:
        return 0.0

# ===================== API =====================
def carregar_resumo_dia(caminho_banco: str, data_lanc) -> Dict[str, Any]:
    """
    Carrega totais e listas do dia selecionado **sem criar/alterar** registros.

    Agrega:
      - total_vendas: **Data** (dia da VENDA)
      - total_saidas: soma de `saida.valor` (DATE(data) = dia)
      - caixa_total/caixa2_total: **valores da linha da data** (tabela `saldos_caixas`)
      - saldos_bancos: soma acumulada por banco <= data (tabela `saldos_bancos`)
      - listas do dia: depósitos, transferências, mercadorias (compras/recebimentos)
      - tem_snapshot: bool indicando se existe linha em `saldos_caixas` no dia
    """
    total_vendas, total_saidas = 0.0, 0.0
    caixa_total = 0.0
    caixa2_total = 0.0
    transf_caixa2_total = 0.0
    depositos_list: List[Tuple[str, float]] = []
    transf_bancos_list: List[Tuple[str, str, float]] = []
    compras_list: List[Tuple[str, str, float]] = []
    receb_list: List[Tuple[str, str, float]] = []
    saldos_bancos: Dict[str, float] = {}
    tem_snapshot = False

    # Normaliza a data (string)
    data_str = str(data_lanc)
    try:
        data_ref_date = pd.to_datetime(data_str, errors="coerce").date()
    except Exception:
        data_ref_date = None

    with get_conn(caminho_banco) as conn:
        # 🚫 Blindagem extra: impede qualquer escrita por engano nesta conexão
        try:
            conn.execute("PRAGMA query_only = ON;")
        except Exception:
            # Ignora se a versão do SQLite não suportar; as consultas abaixo são só SELECT.
            pass

        cur = conn.cursor()

        # ===== VENDAS do dia: por Data =====
        total_vendas = _total_vendas_por_data(conn, data_str)

        # ===== SAÍDAS do dia =====
        total_saidas = float(
            cur.execute(
                """
                SELECT COALESCE(SUM(COALESCE(valor,0)), 0.0)
                  FROM saida
                 WHERE DATE(data) = DATE(?)
                """,
                (data_str,),
            ).fetchone()[0] or 0.0
        )

        # ===== Caixas: TOTAIS DA LINHA DA DATA (sem somatórios) =====
        row = cur.execute(
            """
            SELECT 
                COALESCE(caixa_total, 0.0)  AS cx_total,
                COALESCE(caixa2_total, 0.0) AS cx2_total
              FROM saldos_caixas
             WHERE DATE(data) = DATE(?)
             LIMIT 1
            """,
            (data_str,),
        ).fetchone()
        if row:
            caixa_total = float(row[0] or 0.0)
            caixa2_total = float(row[1] or 0.0)
            tem_snapshot = True

        # ===== Transferência p/ Caixa 2 (dia) — dedupe por trans_uid/id =====
        transf_caixa2_total = float(
            cur.execute(
                """
                SELECT COALESCE(SUM(m.valor), 0.0)
                  FROM movimentacoes_bancarias m
                  JOIN (
                        SELECT MAX(id) AS id
                          FROM movimentacoes_bancarias
                         WHERE DATE(data)=DATE(?)
                           AND origem='transferencia_caixa'
                         GROUP BY COALESCE(trans_uid, CAST(id AS TEXT))
                       ) d ON d.id = m.id
                """,
                (data_str,),
            ).fetchone()[0] or 0.0
        )

        # ===== Depósitos do dia — dedupe por trans_uid/id =====
        depo_rows = cur.execute(
            """
            SELECT m.banco, m.valor
              FROM movimentacoes_bancarias m
              JOIN (
                    SELECT MAX(id) AS id
                      FROM movimentacoes_bancarias
                     WHERE DATE(data)=DATE(?)
                       AND origem='deposito'
                     GROUP BY COALESCE(trans_uid, CAST(id AS TEXT))
                   ) d ON d.id = m.id
             ORDER BY m.id
            """,
            (data_str,),
        ).fetchall()
        depositos_list = [(str(r[0] or ""), float(r[1] or 0.0)) for r in depo_rows]

        # ===== Transferências banco→banco do dia (pareadas) =====
        pares = listar_transferencias_bancos_do_dia(caminho_banco, data_str)
        transf_bancos_list = [
            (p["origem"], p["destino"], float(p["valor"] or 0.0)) for p in pares
        ]

        # ===== Mercadorias do dia (compras) =====
        try:
            df_compras = pd.read_sql(
                "SELECT * FROM mercadorias WHERE DATE(Data)=DATE(?)",
                conn,
                params=(data_str,),
            )
        except Exception:
            df_compras = pd.DataFrame()

        if not df_compras.empty:
            cols = {c.lower(): c for c in df_compras.columns}
            col_col = cols.get("colecao") or cols.get("coleção")
            col_forn = cols.get("fornecedor")
            col_val = cols.get("valor_mercadoria") or cols.get("valor da mercadoria")
            for _, r in df_compras.iterrows():
                compras_list.append(
                    (
                        str(r.get(col_col, "") if col_col else ""),
                        str(r.get(col_forn, "") if col_forn else ""),
                        float(r.get(col_val, 0) or 0.0) if col_val else 0.0,
                    )
                )

        # ===== Mercadorias do dia (recebimentos) =====
        try:
            df_receb = pd.read_sql(
                """
                SELECT * FROM mercadorias
                 WHERE Recebimento IS NOT NULL
                   AND TRIM(Recebimento) <> ''
                   AND DATE(Recebimento) = DATE(?)
                """,
                conn,
                params=(data_str,),
            )
        except Exception:
            df_receb = pd.DataFrame()

        if not df_receb.empty:
            cols = {c.lower(): c for c in df_receb.columns}
            col_col = cols.get("colecao") or cols.get("coleção")
            col_forn = cols.get("fornecedor")
            col_vr = cols.get("valor_recebido")
            col_vm = cols.get("valor_mercadoria") or cols.get("valor da mercadoria")
            for _, r in df_receb.iterrows():
                valor = (
                    float(r.get(col_vr))
                    if (col_vr and pd.notna(r.get(col_vr)))
                    else float(r.get(col_vm, 0) or 0.0)
                )
                receb_list.append(
                    (
                        str(r.get(col_col, "") if col_col else ""),
                        str(r.get(col_forn, "") if col_forn else ""),
                        valor,
                    )
                )

        # ===== Saldos bancos (ACUMULADO <= data) =====
        if data_ref_date:
            try:
                bancos_ativos = _get_bancos_ativos(conn)
                saldos_bancos = _get_saldos_bancos_acumulados(conn, data_ref_date, bancos_ativos)
            except Exception:
                saldos_bancos = {}
        else:
            saldos_bancos = {}

    return {
        "total_vendas": total_vendas,
        "total_saidas": total_saidas,
        "caixa_total": caixa_total,
        "caixa2_total": caixa2_total,
        "transf_caixa2_total": transf_caixa2_total,
        "depositos_list": depositos_list,
        "transf_bancos_list": transf_bancos_list,
        "compras_list": compras_list,
        "receb_list": receb_list,
        "saldos_bancos": saldos_bancos,
        "tem_snapshot": tem_snapshot,  # <- útil para a UI avisar "sem abertura neste dia"
    }


def listar_transferencias_bancos_do_dia(caminho_banco: str, data_ref) -> List[Dict[str, Any]]:
    """
    Lista pares de transferências banco→banco do dia.

    Chave de pareamento (tx):
      1) MIN(id, referencia_id) quando `referencia_id` estiver preenchido (novo fluxo)
      2) token `TX=` na observação (retrocompatibilidade)
      3) `trans_uid` (quando existir)
      4) fallback final: `id`
    """
    from utils.utils import coerce_data

    # Normaliza a data para 'YYYY-MM-DD'
    try:
        d = coerce_data(data_ref)
        data_str = d.strftime("%Y-%m-%d")
    except Exception:
        data_str = str(data_ref)

    sql = """
    WITH m AS (
        SELECT
            id,
            referencia_id,
            data,
            banco,
            tipo,
            valor,
            observacao,
            /* chave de pareamento */
            CASE
              WHEN referencia_id IS NOT NULL AND referencia_id > 0 THEN
                CASE WHEN id < referencia_id THEN CAST(id AS TEXT) ELSE CAST(referencia_id AS TEXT) END
              WHEN instr(COALESCE(observacao,''), 'TX=') > 0 THEN
                substr(observacao, instr(observacao, 'TX=') + 3, 36)
              WHEN trans_uid IS NOT NULL AND TRIM(trans_uid) <> '' THEN
                trans_uid
              ELSE
                CAST(id AS TEXT)
            END AS tx
        FROM movimentacoes_bancarias
        WHERE origem = 'transferencia'
          AND DATE(data) = DATE(?)
    )
    SELECT
      MAX(CASE WHEN tipo='saida'   THEN banco END) AS banco_origem,
      MAX(CASE WHEN tipo='entrada' THEN banco END) AS banco_destino,
      ABS(COALESCE(MAX(CASE WHEN tipo='entrada' THEN valor END),
                   MAX(CASE WHEN tipo='saida'   THEN valor END))) AS valor
    FROM m
    GROUP BY tx
    ORDER BY MIN(CASE
                   WHEN referencia_id IS NOT NULL AND referencia_id > 0
                     THEN CASE WHEN id < referencia_id THEN id ELSE referencia_id END
                   ELSE id
                 END);
    """

    with get_conn(caminho_banco) as conn:
        try:
            conn.execute("PRAGMA query_only = ON;")
        except Exception:
            pass
        df = pd.read_sql(sql, conn, params=(data_str,))

    if df is None or df.empty:
        return []

    out: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        out.append(
            {
                "origem": str(r.get("banco_origem") or "").strip(),
                "destino": str(r.get("banco_destino") or "").strip(),
                "valor": float(r.get("valor") or 0.0),
            }
        )
    return out
