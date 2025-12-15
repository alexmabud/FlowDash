# ===================== Actions: Venda =====================
"""
Fluxo de registro de venda (sem UI) — centralizado no SERVICE.

Estratégia:
- Tentamos chamar o service SEM `data_liq` (service calcula BR-DF).
- Se a assinatura do service exigir `data_liq` (TypeError), fazemos
  um fallback local: Dinheiro/PIX = D; Débito/Crédito/Link = próximo dia útil.
- `created_at` é responsabilidade do service moderno.

Regras de datas (alinhadas ao service):
- `entrada.Data`        = data da VENDA (data selecionada no formulário).
- `entrada.Data_Liq`    = data em que o dinheiro cai (D ou D+1 útil).
- `entrada.created_at`  = timestamp do salvamento (America/Sao_Paulo).

Mantemos aqui:
- Validações de formulário
- Descoberta de taxa e banco_destino (tabela taxas_maquinas)
"""

from __future__ import annotations

from typing import Optional, Tuple, Any
import pandas as pd
import sqlite3
from datetime import datetime, timedelta, date

from shared.db import get_conn
from flowdash_pages.lancamentos.shared_ui import (
    obter_banco_destino,   # só este
)

# ---- Serviço de vendas: usa services.vendas se existir; fallback para services.ledger ----
_VendasService = None
try:
    # Projeto com serviço dedicado de vendas
    from services.vendas import VendasService as _VendasService  # type: ignore
except Exception:
    try:
        # Projeto que centraliza tudo no Ledger
        from services.ledger import LedgerService as _VendasService  # type: ignore
    except Exception:
        _VendasService = None  # checado em runtime


def _r2(x) -> float:
    """Arredonda em 2 casas para evitar ruídos (ex.: -0,00)."""
    return round(float(x or 0.0), 2)


def _formas_equivalentes(forma: str):
    """Normaliza formas equivalentes de pagamento (LINK_PAGAMENTO etc)."""
    f = (forma or "").upper()
    if f == "LINK_PAGAMENTO":
        return ["LINK_PAGAMENTO", "LINK PAGAMENTO", "LINK-DE-PAGAMENTO", "LINK DE PAGAMENTO"]
    return [f]


def _descobrir_taxa_e_banco(
    db_like: Any,
    forma: str,
    maquineta: str,
    bandeira: str,
    parcelas: int,
    modo_pix: Optional[str],
    banco_pix_direto: Optional[str],
    taxa_pix_direto: float,
) -> Tuple[float, Optional[str]]:
    """
    Mesma lógica para determinar taxa% e banco_destino.
    Usa `taxas_maquinas` (forma_pagamento, maquineta, bandeira, parcelas, taxa_percentual, banco_destino).
    """
    taxa, banco_destino = 0.0, None
    forma_up = (forma or "").upper()

    if forma_up in ["DÉBITO", "CREDITO", "CRÉDITO", "LINK_PAGAMENTO"]:
        # normaliza 'CREDITO' -> 'CRÉDITO' se vier sem acento
        forma_norm = "CRÉDITO" if forma_up in ("CREDITO", "CRÉDITO") else forma_up
        formas = _formas_equivalentes(forma_norm)
        placeholders = ",".join(["?"] * len(formas))
        with get_conn(db_like) as conn:
            row = conn.execute(
                f"""
                SELECT taxa_percentual, banco_destino FROM taxas_maquinas
                WHERE UPPER(forma_pagamento) IN ({placeholders})
                  AND maquineta=? AND bandeira=? AND parcelas=?
                LIMIT 1
                """,
                [f.upper() for f in formas] + [maquineta, bandeira, int(parcelas or 1)],
            ).fetchone()
        if row:
            taxa = float(row[0] or 0.0)
            banco_destino = row[1] or None
        if not banco_destino:
            banco_destino = obter_banco_destino(db_like, forma_norm, maquineta, bandeira, parcelas)

    elif forma_up == "PIX":
        if (modo_pix or "") == "Via maquineta":
            with get_conn(db_like) as conn:
                row = conn.execute(
                    """
                    SELECT taxa_percentual, banco_destino FROM taxas_maquinas
                    WHERE UPPER(forma_pagamento)='PIX'
                      AND maquineta=? AND bandeira='' AND parcelas=1
                    LIMIT 1
                    """,
                    (maquineta,),
                ).fetchone()
            taxa = float(row[0] or 0.0) if row else 0.0
            banco_destino = (row[1] if row and row[1] else None) or obter_banco_destino(
                db_like, "PIX", maquineta, "", 1
            )
        else:
            banco_destino = banco_pix_direto
            taxa = float(taxa_pix_direto or 0.0)

    else:  # DINHEIRO
        banco_destino, taxa, parcelas = None, 0.0, 1

    # [ROBUST MODE] Fallback Final - Inferência por Nome da Maquineta
    # Se ainda não temos banco, tentamos deduzir para evitar NULL no banco de dados.
    if not banco_destino and maquineta:
        m = maquineta.upper().strip()
        if 'INFINITE' in m or 'INFINITY' in m: banco_destino = 'InfinitePay'
        elif 'INTER' in m: banco_destino = 'Inter'
        elif 'BRADESCO' in m: banco_destino = 'Bradesco'
        elif 'PAGSEGURO' in m or 'PAGBANK' in m: banco_destino = 'PagBank'
        elif 'MERCADO' in m: banco_destino = 'Mercado Pago'
        elif 'STONE' in m or 'TON' in m: banco_destino = 'Stone'

    return _r2(taxa), (banco_destino or None)


# ------------------ Fallback de liquidação se o service exigir ------------------ #

def _is_working_day_br(df: date) -> bool:
    """Tenta calendário BR-DF; se não houver, usa seg-sex."""
    try:
        from workalendar.registry import registry
        cal_cls = registry.get("BR-DF")
        if cal_cls:
            cal = cal_cls()
            return bool(cal.is_working_day(df))
    except Exception:
        pass
    return df.weekday() < 5  # fallback: seg-sex


def _next_working_day_br(df: date) -> date:
    while not _is_working_day_br(df):
        df += timedelta(days=1)
    return df


def _calc_data_liq_fallback(data_venda_str: str, forma_up: str) -> str:
    """Regra: Dinheiro/PIX = D; Débito/Crédito/Link = D+1 útil (BR-DF)."""
    dv = pd.to_datetime(data_venda_str).date()
    if forma_up in ("DINHEIRO", "PIX"):
        liq = dv
    else:
        liq = _next_working_day_br(dv + timedelta(days=1))
    return liq.isoformat()


# ------------------ Chamadas ao service (com e sem data_liq) ------------------ #

def _chamar_service_registrar_venda_sem_dataliq(
    service: Any,
    *,
    db_like: Any,
    data_venda: str,
    valor: float,
    forma: str,
    parcelas: int,
    bandeira: str,
    maquineta: str,
    banco_destino: Optional[str],
    taxa_percentual: float,
    usuario: str,
):
    """Tenta 3 assinaturas modernas/antigas, SEM data_liq."""
    last_type_error = None

    # 1) Moderna + db_like
    try:
        return service.registrar_venda(
            db_like=db_like,
            data=data_venda,
            valor=valor,
            forma_pagamento=forma,
            parcelas=int(parcelas or 1),
            bandeira=bandeira or "",
            maquineta=maquineta or "",
            banco_destino=banco_destino,
            taxa_percentual=_r2(taxa_percentual or 0.0),
            usuario=usuario,
        )
    except TypeError as e:
        last_type_error = e
    except Exception:
        raise

    # 2) Moderna sem db_like
    try:
        return service.registrar_venda(
            data=data_venda,
            valor=valor,
            forma_pagamento=forma,
            parcelas=int(parcelas or 1),
            bandeira=bandeira or "",
            maquineta=maquineta or "",
            banco_destino=banco_destino,
            taxa_percentual=_r2(taxa_percentual or 0.0),
            usuario=usuario,
        )
    except TypeError as e:
        last_type_error = e
    except Exception:
        raise

    # 3) Antiga
    try:
        return service.registrar_venda(
            data_venda=data_venda,
            valor_bruto=_r2(valor),
            forma=forma,
            parcelas=int(parcelas or 1),
            bandeira=bandeira or "",
            maquineta=maquineta or "",
            banco_destino=banco_destino,
            taxa_percentual=_r2(taxa_percentual or 0.0),
            usuario=usuario,
        )
    except TypeError as e:
        last_type_error = e
    except Exception:
        raise

    raise last_type_error or TypeError("Assinatura incompatível (sem data_liq).")


def _chamar_service_registrar_venda_com_dataliq(
    service: Any,
    *,
    db_like: Any,
    data_venda: str,
    data_liq: str,
    valor: float,
    forma: str,
    parcelas: int,
    bandeira: str,
    maquineta: str,
    banco_destino: Optional[str],
    taxa_percentual: float,
    usuario: str,
):
    """Tenta 3 assinaturas modernas/antigas, COM data_liq (fallback de compatibilidade)."""
    last_type_error = None

    # 1) Moderna + db_like
    try:
        return service.registrar_venda(
            db_like=db_like,
            data=data_venda,
            data_liq=data_liq,
            valor=valor,
            forma_pagamento=forma,
            parcelas=int(parcelas or 1),
            bandeira=bandeira or "",
            maquineta=maquineta or "",
            banco_destino=banco_destino,
            taxa_percentual=_r2(taxa_percentual or 0.0),
            usuario=usuario,
        )
    except TypeError as e:
        last_type_error = e
    except Exception:
        raise

    # 2) Moderna sem db_like
    try:
        return service.registrar_venda(
            data=data_venda,
            data_liq=data_liq,
            valor=valor,
            forma_pagamento=forma,
            parcelas=int(parcelas or 1),
            bandeira=bandeira or "",
            maquineta=maquineta or "",
            banco_destino=banco_destino,
            taxa_percentual=_r2(taxa_percentual or 0.0),
            usuario=usuario,
        )
    except TypeError as e:
        last_type_error = e
    except Exception:
        raise

    # 3) Antiga
    try:
        return service.registrar_venda(
            data_venda=data_venda,
            data_liq=data_liq,
            valor_bruto=_r2(valor),
            forma=forma,
            parcelas=int(parcelas or 1),
            bandeira=bandeira or "",
            maquineta=maquineta or "",
            banco_destino=banco_destino,
            taxa_percentual=_r2(taxa_percentual or 0.0),
            usuario=usuario,
        )
    except TypeError as e:
        last_type_error = e
    except Exception:
        raise

    raise last_type_error or TypeError("Assinatura incompatível (com data_liq).")


def _extrair_nome_simples(x: Any) -> str | None:
    """Normaliza para um 'nome' simples (sem domínio do e-mail)."""
    if not x:
        return None
    s = str(x).strip()
    if "@" in s and " " not in s:  # se vier e-mail, pega antes do @
        s = s.split("@", 1)[0]
    return s or None


def _descobrir_data_liq_gravada(db_like: Any, venda_id: int) -> Optional[str]:
    """
    Obtém a **Data_Liq** efetivamente gravada em `entrada` para compor a mensagem.
    Tenta por rowid; se não achar, tenta por coluna `id`. Fallback para `Data` se `Data_Liq` não existir.
    """
    if venda_id is None or venda_id < 0:
        return None
    try:
        with get_conn(db_like) as conn:
            # Verifica colunas existentes
            cols = {r[1] for r in conn.execute("PRAGMA table_info(entrada)")}
            alvo = "Data_Liq" if "Data_Liq" in cols else "Data"

            # 1) tentar pelo rowid
            row = conn.execute(f'SELECT {alvo} FROM entrada WHERE rowid = ?', (venda_id,)).fetchone()
            if row and row[0]:
                return str(row[0])

            # 2) tentar por coluna id (se existir)
            if "id" in cols:
                row = conn.execute(f'SELECT {alvo} FROM entrada WHERE id = ?', (venda_id,)).fetchone()
                if row and row[0]:
                    return str(row[0])
    except Exception:
        pass
    return None


def registrar_venda(*, db_like: Any = None, data_lanc=None, payload: dict | None = None, **kwargs) -> dict:
    """
    Registra a venda (compatível com chamadas legadas).
    Usa a data selecionada na tela como **data da VENDA**.
    """
    # Compat: permitir chamadas antigas com 'caminho_banco'
    if db_like is None and "caminho_banco" in kwargs:
        db_like = kwargs.pop("caminho_banco")

    payload = payload or {}

    # ------- campos do payload -------
    valor = float(payload.get("valor") or 0.0)
    forma = (payload.get("forma") or "").strip().upper()
    if forma == "CREDITO":
        forma = "CRÉDITO"
    parcelas = int(payload.get("parcelas") or 1)
    bandeira = (payload.get("bandeira") or "").strip()
    maquineta = (payload.get("maquineta") or "").strip()
    modo_pix = payload.get("modo_pix")
    banco_pix_direto = payload.get("banco_pix_direto")

    # ⚠️ PIX direto: ignorar qualquer taxa do formulário (padroniza em 0.0)
    taxa_pix_direto = 0.0

    # ------- validações -------
    if valor <= 0:
        raise ValueError("Valor inválido.")
    if forma in ["DÉBITO", "CRÉDITO", "LINK_PAGAMENTO"] and (not maquineta or not bandeira):
        raise ValueError("Selecione maquineta e bandeira.")
    if forma == "PIX" and (modo_pix or "") == "Via maquineta" and not maquineta:
        raise ValueError("Selecione a maquineta do PIX.")
    if forma == "PIX" and (modo_pix or "") == "Direto para banco" and not banco_pix_direto:
        raise ValueError("Selecione o banco que receberá o PIX direto.")

    # ------- taxa + banco_destino -------
    taxa, banco_destino = _descobrir_taxa_e_banco(
        db_like=db_like,
        forma=forma,
        maquineta=maquineta,
        bandeira=bandeira,
        parcelas=parcelas,
        modo_pix=modo_pix,
        banco_pix_direto=banco_pix_direto,
        taxa_pix_direto=taxa_pix_direto,  # 0.0 para PIX direto
    )

    # ------- data da VENDA (não calculamos data_liq aqui por padrão) -------
    data_venda_str = pd.to_datetime(data_lanc).strftime("%Y-%m-%d")

    # ------- usuário (nome simples) -------
    usuario_atual = payload.get("usuario")
    if not usuario_atual:
        try:
            import streamlit as st
            u = st.session_state.get("usuario_logado")
            if isinstance(u, dict):
                usuario_atual = u.get("nome") or u.get("nome_completo") or u.get("usuario") or u.get("email")
            else:
                usuario_atual = getattr(u, "nome", None) or getattr(u, "nome_completo", None) \
                                or getattr(u, "usuario", None) or getattr(u, "email", None) \
                                or (u if isinstance(u, str) else None)
        except Exception:
            pass
    usuario_atual = _extrair_nome_simples(usuario_atual) or "Sistema"

    # ------- serviço -------
    if _VendasService is None:
        raise RuntimeError(
            "Serviço de Vendas não encontrado. Tenha `services.vendas.VendasService` "
            "ou `services.ledger.LedgerService` disponível."
        )
    try:
        service = _VendasService(db_like=db_like)
    except TypeError:
        service = _VendasService(db_like)

    if not hasattr(service, "registrar_venda"):
        raise RuntimeError("O serviço carregado não expõe `registrar_venda(...)`.")

    # ------- chamada ao service: 1) tentar SEM data_liq -------
    try:
        venda_id, mov_id = _chamar_service_registrar_venda_sem_dataliq(
            service,
            db_like=db_like,
            data_venda=data_venda_str,
            valor=_r2(valor),
            forma=forma,
            parcelas=int(parcelas or 1),
            bandeira=bandeira or "",
            maquineta=maquineta or "",
            banco_destino=banco_destino,
            taxa_percentual=_r2(taxa or 0.0),
            usuario=usuario_atual,
        )
    except TypeError:
        # 2) compatibilidade: service exigiu data_liq -> calcular fallback e tentar de novo
        data_liq_fallback = _calc_data_liq_fallback(data_venda_str, forma)
        venda_id, mov_id = _chamar_service_registrar_venda_com_dataliq(
            service,
            db_like=db_like,
            data_venda=data_venda_str,
            data_liq=data_liq_fallback,
            valor=_r2(valor),
            forma=forma,
            parcelas=int(parcelas or 1),
            bandeira=bandeira or "",
            maquineta=maquineta or "",
            banco_destino=banco_destino,
            taxa_percentual=_r2(taxa or 0.0),
            usuario=usuario_atual,
        )

    # ------- retorno -------
    from utils.utils import formatar_valor
    if venda_id == -1:
        msg = "⚠️ Venda já registrada (idempotência)."
    else:
        # Busca a **Data_Liq** realmente gravada para compor a mensagem (fallback para Data)
        data_liq_gravada = _descobrir_data_liq_gravada(db_like, int(venda_id))
        valor_liq = _r2(float(valor) * (1 - float(taxa or 0.0) / 100.0))
        if data_liq_gravada:
            msg_liq = (
                f"Liquidação de {formatar_valor(valor_liq)} "
                f"em {(banco_destino or 'Caixa_Vendas')} "
                f"em {pd.to_datetime(data_liq_gravada).strftime('%d/%m/%Y')}"
            )
        else:
            # fallback: mensagem genérica
            regra = "hoje (Dinheiro/PIX) ou próximo dia útil (Débito/Crédito/Link)"
            msg_liq = f"Liquidação de {formatar_valor(valor_liq)} em {(banco_destino or 'Caixa_Vendas')} ({regra})"
        msg = f"✅ Venda registrada! {msg_liq}"

    return {"ok": True, "msg": msg}
