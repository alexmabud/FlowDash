# ===================== Actions: Página de Lançamentos =====================
"""
Resumo
------
Consulta o SQLite e calcula os dados do resumo do dia (vendas, saídas, saldos,
transferências e mercadorias).

Regra de VENDAS (pedido):
- Soma SOMENTE vendas cujo **created_at** pertence ao dia selecionado.
- Não usa Data/Data_Venda para o card Vendas.
- Formas consideradas "venda": DINHEIRO, PIX, DÉBITO, CRÉDITO, LINK_PAGAMENTO (e variações).

Demais cartões permanecem como antes:
- Saídas do dia por DATE(data)
- Caixas e bancos: somatório acumulado <= data
- Depósitos/transferências/mercadorias: lógica inalterada
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd

from shared.db import get_conn


# --------------------- Formas de pagamento tratadas como "venda" ---------------------
_FORMAS_VENDA = {
    "DINHEIRO", "PIX",
    "DÉBITO", "DEBITO",
    "CRÉDITO", "CREDITO",
    "LINK_PAGAMENTO", "LINK PAGAMENTO", "LINK-DE-PAGAMENTO", "LINK DE PAGAMENTO",
}


def _total_vendas_por_created_at(conn, data_str: str) -> float:
    """
    Soma entrada.Valor das vendas do dia, filtrando EXCLUSIVAMENTE por created_at (AAAA-MM-DD).
    Se created_at estiver vazio/NULL, a linha NÃO entra no total.
    """
    try:
        formas = tuple(_FORMAS_VENDA)
        placeholders = ",".join("?" for _ in formas)
        sql = f"""
            SELECT COALESCE(SUM(CAST(Valor AS REAL)), 0.0)
              FROM entrada
             WHERE UPPER(COALESCE(Forma_de_Pagamento,'')) IN ({placeholders})
               AND created_at IS NOT NULL
               AND TRIM(created_at) <> ''
               AND DATE(SUBSTR(created_at, 1, 10)) = DATE(?)
        """
        cur = conn.execute(sql, list(formas) + [data_str])
        val = cur.fetchone()[0]
        return float(val or 0.0)
    except Exception:
        return 0.0


# ===================== API =====================
def carregar_resumo_dia(caminho_banco: str, data_lanc) -> Dict[str, Any]:
    """
    Carrega totais e listas do dia selecionado.

    Agrega:
      - total_vendas: SOMENTE created_at (dia da venda real)
      - total_saidas: soma de `saida.valor` (DATE(data) = dia)
      - caixa_total/caixa2_total: **somatório acumulado** <= data (tabela `saldos_caixas`)
      - saldos_bancos: soma acumulada por banco <= data (tabela `saldos_bancos`)
      - listas do dia: depósitos, transferências, mercadorias (compras/recebimentos)
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

    # Normaliza a data
    data_str = str(data_lanc)
    try:
        data_ref_date = pd.to_datetime(data_str, errors="coerce").date()
    except Exception:
        data_ref_date = None

    with get_conn(caminho_banco) as conn:
        cur = conn.cursor()

        # ===== VENDAS do dia: SOMENTE created_at =====
        total_vendas = _total_vendas_por_created_at(conn, data_str)

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

        # ===== Caixas: SOMATÓRIO acumulado <= data =====
        try:
            row = cur.execute(
                """
                SELECT 
                    COALESCE(SUM(COALESCE(caixa_total, 0.0)), 0.0)  AS cx_total,
                    COALESCE(SUM(COALESCE(caixa2_total, 0.0)), 0.0) AS cx2_total
                FROM saldos_caixas
                WHERE DATE(data) <= DATE(?)
                """,
                (data_str,),
            ).fetchone()
            if row:
                caixa_total = float(row[0] or 0.0)
                caixa2_total = float(row[1] or 0.0)
        except Exception:
            pass

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
        depositos_list = cur.execute(
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
            col_val = cols.get("valor_mercadoria")
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
            col_vm = cols.get("valor_mercadoria")
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
        try:
            df_bk = pd.read_sql("SELECT * FROM saldos_bancos", conn)
        except Exception:
            df_bk = pd.DataFrame()

        if not df_bk.empty:
            date_col_name = next((c for c in df_bk.columns if c.lower() == "data"), None)
            if date_col_name:
                df_bk[date_col_name] = pd.to_datetime(df_bk[date_col_name], errors="coerce")
                if data_ref_date is not None:
                    df_bk = df_bk[df_bk[date_col_name].dt.date <= data_ref_date]

            for c in df_bk.columns:
                if c.lower() == "data":
                    continue
                soma = (
                    pd.to_numeric(df_bk[c], errors="coerce")
                    .fillna(0.0)
                    .sum()
                )
                saldos_bancos[str(c)] = float(soma)

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
