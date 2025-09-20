"""
Módulo VendasService
====================

Serviço responsável por registrar **vendas** no sistema, aplicar a
**liquidação** (caixa/banco) na data correta e gravar **log idempotente**
em `movimentacoes_bancarias`.

Regras de datas (alinhadas):
- Dinheiro/PIX: liquidação imediata => entrada.Data = data_venda
- Crédito/Débito/Link: liquidação no próximo dia útil => entrada.Data = proximo_dia_util(data_venda + 1)
- created_at: timestamp da venda em **America/Sao_Paulo** (Brasília)
"""

from __future__ import annotations

from typing import Optional, Tuple
import re
import sqlite3
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from shared.db import get_conn
from shared.ids import uid_venda_liquidacao, sanitize

__all__ = ["VendasService"]

# -----------------------------------------------------------------------------#
# Helpers de data (próximo dia útil)
# -----------------------------------------------------------------------------#
def _is_working_day(d: date) -> bool:
    """Usa Workalendar BR-DF se disponível; senão Brasil; senão seg-sex."""
    # 1) tentar calendário específico do DF (feriados locais)
    try:
        from workalendar.registry import registry
        cal_cls = registry.get("BR-DF")
        if cal_cls:
            cal = cal_cls()
            return bool(cal.is_working_day(d))
    except Exception:
        pass
    # 2) tentar Brasil geral
    try:
        from workalendar.america import Brazil
        cal = Brazil()
        return bool(cal.is_working_day(d))
    except Exception:
        pass
    # 3) fallback simples: seg-sex
    return d.weekday() < 5  # 0..4

def _proximo_dia_util(d: date) -> date:
    """Retorna o próprio dia se for útil; do contrário, avança até ser útil."""
    while not _is_working_day(d):
        d += timedelta(days=1)
    return d

def _liq_para_forma(data_venda_str: str, forma_upper: str) -> str:
    """
    Calcula a data de liquidação para a forma de pagamento.
    - Dinheiro/PIX: mesmo dia da venda
    - Crédito/Débito/Link: D+1 útil
    """
    dv = pd.to_datetime(data_venda_str).date()
    if forma_upper in ("DINHEIRO", "PIX"):
        data_liq = dv
    else:
        data_liq = _proximo_dia_util(dv + timedelta(days=1))
    return data_liq.isoformat()

# -----------------------------------------------------------------------------#
# Helper de taxa (consulta a tabela de taxas da maquineta)
# -----------------------------------------------------------------------------#
def _resolver_taxa_percentual(
    conn: sqlite3.Connection,
    *,
    forma: str,
    bandeira: Optional[str],
    parcelas: int,
    maquineta: Optional[str],
) -> float:
    """
    Busca na tabela `taxas_maquinas` uma taxa compatível; retorna 0.0 se não encontrar.
    Tolera tanto coluna `forma_pagamento` quanto `forma`.
    """
    forma_u = (forma or "").upper()
    params = [forma_u, bandeira, int(parcelas or 1), maquineta]
    try:
        row = conn.execute(
            """
            SELECT COALESCE(taxa_percentual,0) AS taxa
              FROM taxas_maquinas
             WHERE UPPER(COALESCE(forma_pagamento, forma)) = ?
               AND (bandeira  IS NULL OR bandeira  = ?)
               AND (parcelas  IS NULL OR parcelas  = ?)
               AND (maquineta IS NULL OR maquineta = ?)
             ORDER BY 
               CASE WHEN bandeira  IS NULL THEN 1 ELSE 0 END,
               CASE WHEN parcelas  IS NULL THEN 1 ELSE 0 END,
               CASE WHEN maquineta IS NULL THEN 1 ELSE 0 END
             LIMIT 1
            """,
            params,
        ).fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0
    except Exception:
        return 0.0

class VendasService:
    """Regras de negócio para registro de vendas."""

    # ------------------------------------------------------------------ #
    # Setup
    # ------------------------------------------------------------------ #
    def __init__(self, db_path_like: object) -> None:
        """
        Args:
            db_path_like: Caminho do SQLite (str/Path) ou objeto com attr caminho (ex.: SimpleNamespace).
        """
        self.db_path_like = db_path_like

    # =============================
    # Infraestrutura interna
    # =============================
    def _garantir_linha_saldos_caixas(self, conn: sqlite3.Connection, data: str) -> None:
        cur = conn.execute("SELECT 1 FROM saldos_caixas WHERE data = ? LIMIT 1", (data,))
        if not cur.fetchone():
            conn.execute(
                """
                INSERT INTO saldos_caixas
                    (data, caixa, caixa_2, caixa_vendas, caixa2_dia, caixa_total, caixa2_total)
                VALUES (?, 0, 0, 0, 0, 0, 0)
                """,
                (data,),
            )

    def _garantir_linha_saldos_bancos(self, conn: sqlite3.Connection, data: str) -> None:
        cur = conn.execute("SELECT 1 FROM saldos_bancos WHERE data = ? LIMIT 1", (data,))
        if not cur.fetchone():
            conn.execute("INSERT OR IGNORE INTO saldos_bancos (data) VALUES (?)", (data,))

    _COL_RE = re.compile(r"^[A-Za-z0-9_ ]{1,64}$")

    def _validar_nome_coluna_banco(self, banco_col: str) -> str:
        banco_col = (banco_col or "").strip()
        if not self._COL_RE.match(banco_col):
            raise ValueError(f"Nome de banco/coluna inválido: {banco_col!r}")
        return banco_col

    def _ajustar_banco_dynamic(
        self, conn: sqlite3.Connection, banco_col: str, delta: float, data: str
    ) -> None:
        banco_col = self._validar_nome_coluna_banco(banco_col)
        cols = pd.read_sql("PRAGMA table_info(saldos_bancos);", conn)["name"].tolist()
        if banco_col not in cols:
            conn.execute(f'ALTER TABLE saldos_bancos ADD COLUMN "{banco_col}" REAL DEFAULT 0.0;')
        self._garantir_linha_saldos_bancos(conn, data)
        conn.execute(
            f'UPDATE saldos_bancos SET "{banco_col}" = COALESCE("{banco_col}", 0) + ? WHERE data = ?',
            (float(delta), data),
        )

    # =============================
    # Insert em `entrada`
    # =============================
    def _insert_entrada(
        self,
        conn: sqlite3.Connection,
        *,
        data_venda: str,
        data_liq: str,
        valor_bruto: float,
        valor_liquido: float | None,
        forma: str,
        parcelas: int,
        bandeira: Optional[str],
        maquineta: Optional[str],
        banco_destino: Optional[str],
        taxa_percentual: Optional[float],
        usuario: str,
    ) -> int:
        """
        Insere a venda na tabela `entrada`.

        - **Data** = **data de liquidação** (contábil).
        - **created_at** = data/hora da venda em America/Sao_Paulo.
        """
        cols_df = pd.read_sql("PRAGMA table_info(entrada);", conn)
        colnames = set(cols_df["name"].astype(str).tolist())

        # garantir colunas que usamos
        if "Usuario" not in colnames:
            conn.execute('ALTER TABLE entrada ADD COLUMN "Usuario" TEXT;'); colnames.add("Usuario")
        if "valor_liquido" not in colnames:
            conn.execute('ALTER TABLE entrada ADD COLUMN "valor_liquido" REAL;'); colnames.add("valor_liquido")
        if "maquineta" not in colnames:
            conn.execute('ALTER TABLE entrada ADD COLUMN "maquineta" TEXT;'); colnames.add("maquineta")
        if "created_at" not in colnames:
            conn.execute('ALTER TABLE entrada ADD COLUMN "created_at" TEXT;'); colnames.add("created_at")
        if "Data_Liq" not in colnames:
            conn.execute('ALTER TABLE entrada ADD COLUMN "Data_Liq" TEXT;'); colnames.add("Data_Liq")

        # timestamp Brasília
        try:
            created_at_value = datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds")
        except Exception:
            created_at_value = datetime.now().isoformat(timespec="seconds")

        forma_upper = (forma or "").upper()
        parcelas = int(parcelas or 1)

        # decidir taxa efetiva
        if forma_upper == "DINHEIRO":
            taxa_eff, maq_eff = 0.0, None
        elif forma_upper == "PIX" and not (maquineta and maquineta.strip()):
            taxa_eff, maq_eff = 0.0, None  # PIX direto
        else:
            if taxa_percentual is None:
                taxa_eff = _resolver_taxa_percentual(
                    conn, forma=forma_upper, bandeira=bandeira,
                    parcelas=parcelas, maquineta=maquineta
                )
            else:
                taxa_eff = float(taxa_percentual)
            maq_eff = maquineta

        # calcular líquido
        if valor_liquido is None:
            liquido = float(valor_bruto) if taxa_eff == 0.0 else round(float(valor_bruto) * (1 - taxa_eff / 100.0), 2)
        else:
            liquido = float(valor_liquido)

        # Montar INSERT
        to_insert = {
            "Data": data_liq,                     # contábil (liquidação)
            "Data_Liq": data_liq,
            "Valor": float(valor_bruto),
            "valor_liquido": liquido,
            "Forma_de_Pagamento": forma_upper,
            "Parcelas": parcelas,
            "Bandeira": bandeira or None,
            "maquineta": maq_eff,
            "Banco_Destino": banco_destino or None,
            "Usuario": usuario,
            "created_at": created_at_value,       # horário BR
        }

        # Se existir a coluna opcional Data_Venda, preenche também
        if "Data_Venda" in colnames:
            to_insert["Data_Venda"] = data_venda

        if "Taxa_percentual" in colnames:
            to_insert["Taxa_percentual"] = float(taxa_eff)
        elif "Taxa_Percentual" in colnames:
            to_insert["Taxa_Percentual"] = float(taxa_eff)

        names, values = [], []
        for k, v in to_insert.items():
            if k in colnames and v is not None:
                names.append(f'"{k}"'); values.append(v)

        placeholders = ", ".join("?" for _ in names)
        cols_sql = ", ".join(names)
        conn.execute(f"INSERT INTO entrada ({cols_sql}) VALUES ({placeholders})", values)
        return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

    # =============================
    # Regra principal (compat wrapper)
    # =============================
    def registrar_venda(self, *args, **kwargs) -> Tuple[int, int]:
        """
        Wrapper de compatibilidade:
        - Se 'data_liq' NÃO for informado, calcula automaticamente conforme a forma:
          • DINHEIRO/PIX => data_liq = data_venda
          • CRÉDITO/DÉBITO/LINK_PAGAMENTO => data_liq = próximo dia útil (D+1 útil)
        - Aceita 'caminho_banco' (legado) como db_path_like.
        """
        if "caminho_banco" in kwargs and kwargs["caminho_banco"]:
            self.db_path_like = kwargs.pop("caminho_banco")

        data_venda = kwargs.pop("data_venda", kwargs.pop("data", None))
        data_liq = kwargs.pop("data_liq", kwargs.pop("data_liquidacao", None))
        valor_bruto = kwargs.pop("valor_bruto", kwargs.pop("valor", None))
        forma = kwargs.pop("forma", kwargs.pop("forma_pagamento", None))
        parcelas = kwargs.pop("parcelas", 1)
        bandeira = kwargs.pop("bandeira", None)
        maquineta = kwargs.pop("maquineta", None)
        banco_destino = kwargs.pop("banco_destino", None)
        taxa_percentual = kwargs.pop("taxa_percentual", kwargs.pop("taxa", 0.0))
        usuario = kwargs.pop("usuario", "Sistema")

        # Se não veio data_liq -> calcula
        forma_u = (forma or "").upper()
        if not data_liq or str(data_liq).strip() == "":
            data_liq = _liq_para_forma(str(data_venda), forma_u)

        return self._registrar_venda_impl(
            data_venda=data_venda,
            data_liq=data_liq,
            valor_bruto=valor_bruto,
            forma=forma_u,
            parcelas=parcelas,
            bandeira=bandeira,
            maquineta=maquineta,
            banco_destino=banco_destino,
            taxa_percentual=taxa_percentual,
            usuario=usuario,
        )

    # =============================
    # Regra principal (implementação real)
    # =============================
    def _registrar_venda_impl(
        self,
        data_venda: str,            # YYYY-MM-DD
        data_liq: str,              # YYYY-MM-DD (já calculada)
        valor_bruto: float,
        forma: str,                 # "DINHEIRO" | "PIX" | "DÉBITO" | "CRÉDITO" | "LINK_PAGAMENTO"
        parcelas: int,
        bandeira: Optional[str],
        maquineta: Optional[str],
        banco_destino: Optional[str],
        taxa_percentual: float,
        usuario: str,
    ) -> Tuple[int, int]:
        """Registra a venda, aplica a liquidação e grava log idempotente."""
        # Validações básicas
        try:
            pd.to_datetime(data_venda)
            pd.to_datetime(data_liq)
        except Exception:
            raise ValueError("Datas inválidas; use YYYY-MM-DD.")
        if float(valor_bruto) <= 0:
            raise ValueError("valor_bruto deve ser > 0.")

        forma_u = sanitize(forma or "").upper()
        if forma_u == "DEBITO":
            forma_u = "DÉBITO"
        if forma_u not in ("DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"):
            raise ValueError(f"Forma de pagamento inválida: {forma!r}")

        parcelas = int(parcelas or 1)
        if parcelas < 1:
            raise ValueError("parcelas deve ser >= 1.")

        bandeira = sanitize(bandeira)
        maquineta = sanitize(maquineta)
        banco_destino = sanitize(banco_destino)
        usuario = sanitize(usuario)

        with get_conn(self.db_path_like) as conn:
            # decidir taxa efetiva
            if forma_u == "DINHEIRO" or (forma_u == "PIX" and not (maquineta and maquineta.strip())):
                taxa_eff = 0.0
            else:
                taxa_eff = float(taxa_percentual or 0.0)
                if taxa_eff == 0.0:
                    taxa_eff = _resolver_taxa_percentual(
                        conn, forma=forma_u, bandeira=bandeira, parcelas=int(parcelas), maquineta=maquineta
                    )

            valor_liquido = round(float(valor_bruto) * (1.0 - float(taxa_eff) / 100.0), 2)

            # Idempotência — um único log por liquidação
            trans_uid = uid_venda_liquidacao(
                data_venda, data_liq, float(valor_bruto), forma_u, int(parcelas),
                bandeira, maquineta, banco_destino, float(taxa_eff), usuario,
            )

            row = conn.execute(
                "SELECT id FROM movimentacoes_bancarias WHERE trans_uid=? LIMIT 1;",
                (trans_uid,),
            ).fetchone()
            if row:
                return (-1, -1)

            cur = conn.cursor()

            # 1) INSERT em `entrada` (Data = data_liq; created_at = horário BR)
            venda_id = self._insert_entrada(
                conn,
                data_venda=str(data_venda),
                data_liq=str(data_liq),
                valor_bruto=float(valor_bruto),
                valor_liquido=float(valor_liquido),
                forma=forma_u,
                parcelas=int(parcelas),
                bandeira=bandeira,
                maquineta=maquineta,
                banco_destino=banco_destino,
                taxa_percentual=float(taxa_eff),
                usuario=usuario,
            )

            # 2) Atualiza saldos na data de liquidação
            if forma_u == "DINHEIRO":
                self._garantir_linha_saldos_caixas(conn, data_liq)
                cur.execute(
                    "UPDATE saldos_caixas SET caixa_vendas = COALESCE(caixa_vendas,0) + ? WHERE data = ?",
                    (float(valor_liquido), data_liq),
                )
                banco_label = "Caixa_Vendas"
            else:
                if not banco_destino:
                    raise ValueError("banco_destino é obrigatório para formas não-DINHEIRO.")
                self._garantir_linha_saldos_bancos(conn, data_liq)
                self._ajustar_banco_dynamic(conn, banco_col=banco_destino, delta=float(valor_liquido), data=data_liq)
                banco_label = banco_destino

            # 3) Log em movimentacoes_bancarias
            if forma_u == "PIX" and not (maquineta and maquineta.strip()):
                detalhe_meio = f"Direto — {banco_destino or '—'}"
            elif forma_u in ("CRÉDITO", "DÉBITO", "LINK_PAGAMENTO"):
                detalhe_meio = f"{(bandeira or '—')}/{(maquineta or '—')}"
            elif forma_u == "DINHEIRO":
                detalhe_meio = "Caixa"
            else:
                detalhe_meio = f"{(bandeira or '—')}/{(maquineta or '—')}"

            obs = (
                f"Lançamento VENDA {forma_u} {parcelas}x / "
                f"{detalhe_meio} • Bruto R$ {float(valor_bruto):.2f} • "
                f"Taxa {float(taxa_eff):.2f}% -> Líquido R$ {valor_liquido:.2f}"
            ).strip()

            cols_exist = {r[1] for r in conn.execute("PRAGMA table_info(movimentacoes_bancarias)")}
            payload = {
                "data": data_liq,
                "banco": banco_label,
                "tipo": "entrada",
                "valor": float(valor_liquido),
                "origem": "lancamentos",
                "observacao": obs,
                "referencia_tabela": "entrada",
                "referencia_id": int(venda_id),
                "trans_uid": trans_uid,
            }
            if "data_hora" in cols_exist:
                # horário BR para log também
                try:
                    payload["data_hora"] = datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds")
                except Exception:
                    payload["data_hora"] = datetime.now().isoformat(timespec="seconds")
            if "usuario" in cols_exist:
                payload["usuario"] = usuario

            cols_sql = ", ".join(f'"{k}"' for k in payload.keys())
            ph_sql   = ", ".join("?" for _ in payload)
            vals     = list(payload.values())

            cur.execute(f"INSERT INTO movimentacoes_bancarias ({cols_sql}) VALUES ({ph_sql})", vals)
            mov_id = int(cur.lastrowid)

            conn.commit()

        return (int(venda_id), int(mov_id))
