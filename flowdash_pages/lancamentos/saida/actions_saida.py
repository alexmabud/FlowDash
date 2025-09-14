# ===================== Actions: Saída =====================
"""
Executa a MESMA lógica do módulo original de Saída (sem Streamlit aqui):
- Fluxos padrão: DINHEIRO, PIX/DÉBITO, CRÉDITO, BOLETO.
- Fluxos Pagamentos: Fatura Cartão, Boletos (parcela), Empréstimos (parcela).
- Canonicalização de banco preservada.

Validações que no original exibiam st.warning/st.error aqui geram ValueError/RuntimeError.
A página captura e exibe as mensagens.

ATUALIZAÇÃO:
- Para Boletos/Empréstimos a lista mostra o "valor em aberto" calculado como:
    em_aberto = valor_evento - valor_pago_acumulado
  Usando como base a coluna `valor_evento` (se existir). Caso não exista, cai para
  colunas conhecidas de valor (saldo, valor_a_pagar, etc).
- Para FATURA CARTÃO, o dropdown vem de `repository.cartoes_repository.listar_destinos_fatura_em_aberto`,
  com label "Fatura [cartão], Data [vencimento], Valor [valor_evento]" e campo `saldo` (restante).
"""

from __future__ import annotations

from typing import TypedDict, Optional, Callable, Tuple
from datetime import date
import pandas as pd

from shared.db import get_conn
from services.ledger import LedgerService

from repository.cartoes_repository import (
    CartoesRepository,
    listar_destinos_fatura_em_aberto,  # novo provider oficial do dropdown
)
from repository.categorias_repository import CategoriasRepository
from flowdash_pages.cadastros.cadastro_classes import BancoRepository
from repository.contas_a_pagar_mov_repository import ContasAPagarMovRepository  # fallback/compat
from flowdash_pages.lancamentos.shared_ui import canonicalizar_banco  # usado no original

# ---------- Constantes (iguais ao original)
FORMAS = ["DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "BOLETO"]
ORIGENS_DINHEIRO = ["Caixa", "Caixa 2"]


# ---------------- Utils (novos helpers apenas para validação de teto)
def _as_float(x) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return None
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def _first_key(d: dict, keys: list[str]):
    for k in keys:
        if isinstance(d, dict) and (k in d) and (d[k] is not None):
            return d[k]
    return None


def _obter_saldo_fatura_dropdown(caminho_banco: str, obrigacao_id_fatura: int) -> Optional[float]:
    """
    Busca o item da fatura no dropdown e devolve o saldo/restante.
    Tenta chaves mais comuns ('saldo'); se não houver, tenta reconstituir (valor_evento - pago).
    """
    try:
        opcoes = _listar_faturas_cartao_abertas_dropdown(caminho_banco) or []
        oid = int(obrigacao_id_fatura)
        for o in opcoes:
            oid_o = int(_first_key(o, ["obrigacao_id", "id", "cap_obrigacao_id", "obrigacao"]) or 0)
            if oid_o == oid:
                saldo = _as_float(_first_key(o, ["saldo", "restante", "saldo_restante", "valor_em_aberto", "valor_restante"]))
                if saldo is not None:
                    return float(saldo)
                valor_evt = _as_float(_first_key(o, ["valor_evento", "valor_total", "valor", "total"])) or 0.0
                pago = _as_float(_first_key(o, ["pago", "pago_acumulado", "valor_pago", "total_pago"])) or 0.0
                return max(0.0, float(valor_evt) - float(pago))
    except Exception:
        pass
    return None


def _bloqueia_pagamento_maior(valor_pagamento: float, limite: float, contexto: str) -> None:
    """
    Se o valor do pagamento for maior que o limite informado, bloqueia com ValueError.
    Mensagem amarela pedida na UI.
    contexto: 'fatura' | 'boleto' | 'emprestimo'
    """
    v = _as_float(valor_pagamento) or 0.0
    lim = _as_float(limite) or 0.0
    eps = 0.005
    if v > (lim + eps):
        if contexto == "fatura":
            raise ValueError("Valor do pagamento maior que o valor da fatura, ajuste o valor.")
        elif contexto == "boleto":
            raise ValueError("Valor do pagamento maior que o valor do boleto, ajuste o valor.")
        elif contexto == "emprestimo":
            raise ValueError("Valor do pagamento maior que o valor do empréstimo, ajuste o valor.")
        # fallback genérico (não deve ocorrer)
        raise ValueError("Valor do pagamento maior que o valor em aberto, ajuste o valor.")


# ---------------- Utils já existentes
def _distinct_lower_trim(series: pd.Series) -> list[str]:
    if series is None or series.empty:
        return []
    df = pd.DataFrame({"orig": series.fillna("").astype(str).str.strip()})
    df["key"] = df["orig"].str.lower().str.strip()
    df = df[df["key"] != ""].drop_duplicates("key", keep="first")
    return df["orig"].sort_values().tolist()


def _opcoes_pagamentos(caminho_banco: str, tipo: str) -> list[str]:
    """
    Compat LEGADA (não usada nos novos fluxos de Pagamentos).
    """
    with get_conn(caminho_banco) as conn:
        if tipo == "Fatura Cartão de Crédito":
            return []

        elif tipo == "Empréstimos e Financiamentos":
            df_emp = pd.read_sql(
                """
                SELECT DISTINCT
                    TRIM(
                        COALESCE(
                            NULLIF(TRIM(banco),''), NULLIF(TRIM(descricao),''), NULLIF(TRIM(tipo),'')
                        )
                    ) AS rotulo
                FROM emprestimos_financiamentos
                """,
                conn,
            )
            df_emp = df_emp.dropna()
            df_emp = df_emp[df_emp["rotulo"] != ""]
            return _distinct_lower_trim(df_emp["rotulo"]) if not df_emp.empty else []

        elif tipo == "Boletos":
            # evita confundir com cartões/emp.
            df_cart = pd.read_sql(
                "SELECT DISTINCT TRIM(nome) AS nome FROM cartoes_credito "
                "WHERE nome IS NOT NULL AND TRIM(nome) <> ''",
                conn,
            )
            cart_set = set(x.strip().lower() for x in (df_cart["nome"].dropna().tolist() if not df_cart.empty else []))

            df_emp = pd.read_sql(
                "SELECT DISTINCT TRIM(COALESCE(NULLIF(TRIM(banco),''),NULLIF(TRIM(descricao),''),NULLIF(TRIM(tipo),''))) AS rotulo "
                "FROM emprestimos_financiamentos",
                conn,
            )
            emp_set = set(x.strip().lower() for x in (df_emp["rotulo"].dropna().tolist() if not df_emp.empty else []))

            df_cred = pd.read_sql(
                """
                SELECT DISTINCT TRIM(credor) AS credor
                  FROM contas_a_pagar_mov
                 WHERE credor IS NOT NULL AND TRIM(credor) <> ''
                   AND UPPER(COALESCE(status,'EM ABERTO')) IN ('EM ABERTO','PARCIAL')
                 ORDER BY credor
                """,
                conn,
            )

            def eh_boleto_nome(nm: str) -> bool:
                lx = (nm or "").strip().lower()
                return bool(lx) and (lx not in cart_set) and (lx not in emp_set)

            candidatos = [c for c in (df_cred["credor"].dropna().tolist() if not df_cred.empty else []) if eh_boleto_nome(c)]
            if not candidatos:
                return []
            df = pd.DataFrame({"rotulo": candidatos})
            df["key"] = df["rotulo"].str.lower().str.strip()
            df = df[df["key"] != ""].drop_duplicates("key", keep="first")
            return df["rotulo"].sort_values().tolist()

    return []


# ---------------- Helpers de coluna de valor/saldo (para mostrar "valor em aberto")
def _resolver_coluna_preferida(conn, preferidas: list[str]) -> Optional[str]:
    cols = pd.read_sql("PRAGMA table_info(contas_a_pagar_mov)", conn)
    existentes = set((cols["name"] if "name" in cols.columns else []).tolist())
    for c in preferidas:
        if c in existentes:
            return c
    return None


def _resolver_colunas_evento_e_pago(conn) -> tuple[str, Optional[str]]:
    """
    Retorna (col_valor_evento, col_valor_pago_acumulado_ou_None)
    """
    preferidas_evento = [
        "valor_evento",  # preferida
        "saldo", "valor_saldo", "valor_em_aberto",
        "valor_a_pagar", "valor_previsto", "valor_original", "valor"
    ]
    preferidas_pago_acum = [
        "valor_pago_acumulado", "pago_acumulado",
        "total_pago", "valor_pago_total", "pago_total",
        "valor_pago", "pago"
    ]

    col_evento = _resolver_coluna_preferida(conn, preferidas_evento) or "valor"
    col_pago   = _resolver_coluna_preferida(conn, preferidas_pago_acum)  # pode ser None

    return col_evento, col_pago


# ---------------- Providers NOVOS (mostram VALOR EM ABERTO no label)

def _listar_empfin_em_aberto(caminho_banco: str) -> list[dict]:
    """
    Lista parcelas de EMPRÉSTIMO/FINANCIAMENTO em aberto/parcial, mostrando o VALOR EM ABERTO.
    em_aberto = valor_evento - valor_pago_acumulado
    Saída: [{label, obrigacao_id, parcela_id, credor, vencimento, valor_evento, pago_acumulado, em_aberto}]
    """
    with get_conn(caminho_banco) as conn:
        col_evento, col_pago = _resolver_colunas_evento_e_pago(conn)
        sel_pago = f", COALESCE({col_pago}, 0.0) AS valor_pago_acum" if col_pago else ", 0.0 AS valor_pago_acum"

        df = pd.read_sql(
            f"""
            SELECT
                id                           AS parcela_id,
                COALESCE(obrigacao_id, 0)    AS obrigacao_id,
                TRIM(
                    COALESCE(
                        NULLIF(TRIM(credor), ''),
                        NULLIF(TRIM(descricao), ''),
                        'Empréstimo'
                    )
                )                             AS credor,
                COALESCE(parcela_num, 1)     AS parcela_num,
                COALESCE(parcelas_total, 1)  AS parcelas_total,
                DATE(vencimento)             AS vencimento,
                COALESCE({col_evento}, 0.0)  AS valor_evento
                {sel_pago},
                UPPER(TRIM(REPLACE(COALESCE(tipo_obrigacao,''),'É','E'))) AS u_tipo_norm
            FROM contas_a_pagar_mov
            WHERE UPPER(COALESCE(status, 'EM ABERTO')) IN ('EM ABERTO', 'PARCIAL')
            ORDER BY DATE(vencimento) ASC, credor ASC, parcela_num ASC
            """,
            conn,
        )

    if df is None or df.empty:
        return []

    df = df[(df["u_tipo_norm"] == "EMPRESTIMO") | (df["u_tipo_norm"].str.startswith("EMPR"))]
    if df.empty:
        return []

    df["em_aberto"] = (df["valor_evento"] - df["valor_pago_acum"]).clip(lower=0.0)

    # 🔒 remover registros com em_aberto <= 0
    df = df[df["em_aberto"] > 0.0]

    def _fmt_row(r):
        credor = (r["credor"] or "").strip() or "Empréstimo"
        par    = int(r["parcela_num"] or 1)
        tot    = int(r["parcelas_total"] or par)
        venc   = str(r["vencimento"] or "")
        try:
            venc_pt = pd.to_datetime(venc).strftime("%d/%m/%Y") if venc else "—"
        except Exception:
            venc_pt = "—"
        em_aberto = float(r["em_aberto"] or 0.0)
        rotulo = f"{credor} • Parc {par}/{tot} • Venc {venc_pt} • R$ {em_aberto:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return {
            "label": rotulo,
            "obrigacao_id": int(r["obrigacao_id"] or 0),
            "parcela_id": int(r["parcela_id"]),
            "credor": credor,
            "vencimento": venc,
            "valor_evento": float(r["valor_evento"] or 0.0),
            "pago_acumulado": float(r["valor_pago_acum"] or 0.0),
            "em_aberto": em_aberto,
            "parcela_num": par,
            "parcelas_total": tot,
        }

    return [_fmt_row(r) for _, r in df.iterrows()]


def _listar_faturas_cartao_abertas_dropdown(caminho_banco: str) -> list[dict]:
    """
    Provider do dropdown de FATURAS EM ABERTO.
    Preferência: usar repository.cartoes_repository.listar_destinos_fatura_em_aberto (label e saldo).
    Fallback: ContasAPagarMovRepository, adaptando o shape.
    """
    try:
        return listar_destinos_fatura_em_aberto(caminho_banco) or []
    except Exception:
        pass  # tenta fallback abaixo

    try:
        repo = ContasAPagarMovRepository(caminho_banco)
        faturas = repo.listar_faturas_cartao_abertas() or []
        opcoes = []
        for f in faturas:
            cartao = (f.get("credor") or "").strip()
            vcto   = (f.get("vencimento") or f.get("data_evento") or "").strip()
            valor_evt = float(f.get("valor_evento") or f.get("valor_total") or f.get("saldo_restante") or 0.0)
            saldo = float(f.get("saldo_restante") or 0.0)
            valor_fmt = f"R$ {valor_evt:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            rotulo = f"Fatura {cartao}, Data {vcto}, Valor {valor_fmt}"
            opcoes.append({
                "label": rotulo,
                "cartao": cartao,
                "competencia": (f.get("competencia") or "").strip(),
                "vencimento": vcto,
                "valor_evento": valor_evt,
                "obrigacao_id": int(f.get("obrigacao_id") or 0),
                "saldo": saldo,
            })
        return opcoes
    except Exception:
        return []


# ---------------- Resultado
class ResultadoSaida(TypedDict):
    ok: bool
    msg: str


# ---------------- Carregamentos para a UI (listas)
def carregar_listas_para_form(
    caminho_banco: str,
) -> Tuple[
    list[str],                          # nomes_bancos
    list[str],                          # nomes_cartoes
    pd.DataFrame,                       # df_categorias
    Callable[[int], pd.DataFrame],      # listar_subcategorias(cat_id)->DataFrame
    Callable[[], list[dict]],           # listar_destinos_fatura_em_aberto()->list[dict]
    Callable[[str], list[str]],         # _opcoes_pagamentos(tipo)->list[str] (legacy)
    Callable[[], list[dict]],           # listar_boletos_em_aberto()->list[dict]
    Callable[[], list[dict]],           # listar_empfin_em_aberto()->list[dict]
]:
    """
    Carrega listas necessárias para o formulário.
    Returns:
      (nomes_bancos, nomes_cartoes, df_categorias,
       listar_subcategorias_fn,
       listar_destinos_fatura_em_aberto_fn,
       carregar_opcoes_pagamentos_fn,  # compat
       listar_boletos_em_aberto_fn,
       listar_empfin_em_aberto_fn)
    """
    bancos_repo = BancoRepository(caminho_banco)
    cartoes_repo = CartoesRepository(caminho_banco)
    cats_repo = CategoriasRepository(caminho_banco)

    df_bancos = bancos_repo.carregar_bancos()
    nomes_bancos = df_bancos["nome"].tolist() if df_bancos is not None and not df_bancos.empty else []
    nomes_cartoes = cartoes_repo.listar_nomes()
    df_categorias = cats_repo.listar_categorias()

    return (
        nomes_bancos,
        nomes_cartoes,
        df_categorias,
        cats_repo.listar_subcategorias,
        lambda: _listar_faturas_cartao_abertas_dropdown(caminho_banco),  # usa provider novo (com fallback)
        lambda tipo: _opcoes_pagamentos(caminho_banco, tipo),
        lambda: _listar_boletos_em_aberto(caminho_banco),
        lambda: _listar_empfin_em_aberto(caminho_banco),
    )


# ---------------- Util: restante/status pós-pagamento (fallback direto no banco)
def _obter_restante_e_status(caminho_banco: str, obrigacao_id: int) -> tuple[float, str]:
    restante = 0.0
    status = "QUITADA"
    try:
        with get_conn(caminho_banco) as _c:
            _row = pd.read_sql(
                """
                SELECT COALESCE(valor_evento,0) AS valor_evento,
                       COALESCE(valor_pago_acumulado,0) AS pago
                  FROM contas_a_pagar_mov
                 WHERE obrigacao_id = ?
                   AND categoria_evento = 'LANCAMENTO'
                 LIMIT 1
                """,
                _c,
                params=(int(obrigacao_id),),
            )
        if not _row.empty:
            _rest = float(_row.iloc[0]["valor_evento"]) - float(_row.iloc[0]["pago"])
            restante = round(_rest if _rest > 0 else 0.0, 2)
            status = "PARCIAL" if restante > 0.005 else "QUITADA"
    except Exception:
        pass
    return restante, status


# ---------------- Dispatcher principal (mantém as mesmas regras do original)
def registrar_saida(caminho_banco: str, data_lanc: date, usuario_nome: str, payload: dict) -> ResultadoSaida:
    """
    Dispatcher que executa a mesma lógica do módulo original, com as mesmas validações.
    """
    ledger = LedgerService(caminho_banco)
    cartoes_repo = CartoesRepository(caminho_banco)

    # Unpack do payload (nomes idênticos aos usados no original/UI)
    valor_saida = float(payload.get("valor_saida") or 0.0)
    forma_pagamento = (payload.get("forma_pagamento") or "").strip()
    cat_nome = (payload.get("cat_nome") or "").strip()
    subcat_nome = (payload.get("subcat_nome") or "").strip()
    is_pagamentos = bool(payload.get("is_pagamentos"))
    tipo_pagamento_sel = (payload.get("tipo_pagamento_sel") or "").strip() if is_pagamentos else None
    destino_pagamento_sel = (payload.get("destino_pagamento_sel") or "").strip() if is_pagamentos else None

    # Fatura
    competencia_fatura_sel = payload.get("competencia_fatura_sel")
    obrigacao_id_fatura = payload.get("obrigacao_id_fatura")
    multa_fatura = float(payload.get("multa_fatura") or 0.0)
    juros_fatura = float(payload.get("juros_fatura") or 0.0)
    desconto_fatura = float(payload.get("desconto_fatura") or 0.0)

    # Boleto
    parcela_boleto_escolhida = payload.get("parcela_boleto_escolhida")  # dict da parcela selecionada
    multa_boleto = float(payload.get("multa_boleto") or 0.0)
    juros_boleto = float(payload.get("juros_boleto") or 0.0)
    desconto_boleto = float(payload.get("desconto_boleto") or 0.0)

    # Empréstimo
    parcela_emp_escolhida = payload.get("parcela_emp_escolhida")        # dict da parcela selecionada
    multa_emp = float(payload.get("multa_emp") or 0.0)
    juros_emp = float(payload.get("juros_emp") or 0.0)
    desconto_emp = float(payload.get("desconto_emp") or 0.0)

    # Demais campos
    parcelas = int(payload.get("parcelas") or 1)
    cartao_escolhido = (payload.get("cartao_escolhido") or "").strip()
    banco_escolhido_in = (payload.get("banco_escolhido") or "").strip()
    origem_dinheiro = (payload.get("origem_dinheiro") or "").strip()
    venc_1 = payload.get("venc_1")
    fornecedor = (payload.get("fornecedor") or "").strip()
    documento = (payload.get("documento") or "").strip()
    descricao_final = (payload.get("descricao_final") or "").strip()

    data_str = str(data_lanc)

    # ================== Validações gerais/finais ==================
    if is_pagamentos and tipo_pagamento_sel == "Boletos":
        valor_digitado = float(valor_saida)
        if valor_digitado <= 0 and (multa_boleto + juros_boleto - desconto_boleto) <= 0:
            raise ValueError("Informe um valor de pagamento > 0 ou ajustes (multa/juros/desconto).")

    if is_pagamentos and tipo_pagamento_sel == "Empréstimos e Financiamentos":
        valor_digitado = float(valor_saida)
        if valor_digitado <= 0 and (multa_emp + juros_emp - desconto_emp) <= 0:
            raise ValueError("Informe um valor de pagamento > 0 ou ajustes (multa/juros/desconto).")

    if not is_pagamentos and valor_saida <= 0:
        raise ValueError("O valor deve ser maior que zero.")

    # Validações específicas dos fluxos
    if forma_pagamento in ["PIX", "DÉBITO"] and not banco_escolhido_in:
        raise ValueError("Selecione ou digite o banco da saída.")
    if forma_pagamento == "DINHEIRO" and not origem_dinheiro:
        raise ValueError("Informe a origem do dinheiro (Caixa/Caixa 2).")

    if is_pagamentos:
        if not tipo_pagamento_sel:
            raise ValueError("Selecione o tipo de pagamento (Fatura, Empréstimos ou Boletos).")
        if tipo_pagamento_sel != "Fatura Cartão de Crédito":
            if not destino_pagamento_sel or not str(destino_pagamento_sel).strip():
                raise ValueError("Selecione o destino correspondente ao tipo escolhido.")

    # ================== FATURA CARTÃO DE CRÉDITO ==================
    if is_pagamentos and tipo_pagamento_sel == "Fatura Cartão de Crédito":
        if not obrigacao_id_fatura:
            raise ValueError("Selecione uma fatura em aberto (cartão, data, valor).")
        # exige origem: dinheiro (Caixa/Caixa 2) ou banco (PIX/DÉBITO)
        if forma_pagamento == "DINHEIRO" and not origem_dinheiro:
            raise ValueError("Informe a origem do dinheiro (Caixa/Caixa 2).")
        if forma_pagamento in ["PIX", "DÉBITO"] and not banco_escolhido_in:
            raise ValueError("Selecione ou digite o banco da saída.")

        # 🔒 BLOQUEIO: não permitir pagar acima do saldo/restante da fatura
        saldo_dropdown = _obter_saldo_fatura_dropdown(caminho_banco, int(obrigacao_id_fatura))
        if saldo_dropdown is None:
            # fallback direto no banco
            saldo_dropdown, _ = _obter_restante_e_status(caminho_banco, int(obrigacao_id_fatura))
        _bloqueia_pagamento_maior(valor_saida, saldo_dropdown or 0.0, "fatura")

        origem = origem_dinheiro if forma_pagamento == "DINHEIRO" else _canonicalizar_banco_safe(caminho_banco, banco_escolhido_in)

        # tentar pagar via ledger (com retorno de restante/status)
        res = ledger.pagar_fatura_cartao(
            data=data_str,
            valor=float(valor_saida),
            forma_pagamento=forma_pagamento,
            origem=origem,
            obrigacao_id=int(obrigacao_id_fatura),
            usuario=usuario_nome,
            categoria="Fatura Cartão de Crédito",
            sub_categoria=subcat_nome,
            descricao=descricao_final,
            multa=float(multa_fatura),
            juros=float(juros_fatura),
            desconto=float(desconto_fatura),
            retornar_info=True,
        )

        # compat: lidar com retorno (3 ou 5 itens)
        if isinstance(res, tuple) and len(res) >= 3:
            id_saida, id_mov, id_cap = res[0], res[1], res[2]
            if len(res) >= 5:
                restante, status = float(res[3]), str(res[4])
            else:
                # fallback: consulta restante/status direto no banco
                restante, status = _obter_restante_e_status(caminho_banco, int(obrigacao_id_fatura))
        else:
            raise RuntimeError("Retorno inesperado do Ledger no pagamento de fatura.")

        # >>> PADRÃO UNIFICADO: Base, Ajustes, Total debitado
        ajustes_fat = float(multa_fatura) + float(juros_fatura) - float(desconto_fatura)
        total_debitado_fat = float(valor_saida) + ajustes_fat

        msg_sucesso_fatura = (
            "✅ Pagamento de fatura registrado! "
            f"Base: R$ {float(valor_saida):.2f} | "
            f"Ajustes (+multa+juros−desconto): R$ {ajustes_fat:.2f} | "
            f"Total debitado: R$ {total_debitado_fat:.2f} | "
            f"Restante: R$ {restante:.2f} | Status: {status} | "
            f"Saída: {id_saida or '—'} | Log: {id_mov or '—'} | Evento CAP: {id_cap or '—'}"
        )

        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if id_saida == -1 or id_mov == -1 or id_cap == -1
                else msg_sucesso_fatura
            ),
        }

    # ================== Branches Especiais (Pagamentos) ==================
    if is_pagamentos and tipo_pagamento_sel == "Boletos":
        if not destino_pagamento_sel or not str(destino_pagamento_sel).strip():
            raise ValueError("Selecione o credor do boleto.")
        if not parcela_boleto_escolhida:
            raise ValueError("Selecione a parcela do boleto para pagar (ou informe o identificador).")

        # 🔒 BLOQUEIO: teto = em_aberto informado no item (ou reconstituído)
        em_aberto = _as_float((parcela_boleto_escolhida or {}).get("em_aberto"))
        if em_aberto is None:
            va = _as_float((parcela_boleto_escolhida or {}).get("valor_evento") or 0.0) or 0.0
            pa = _as_float((parcela_boleto_escolhida or {}).get("pago_acumulado") or 0.0) or 0.0
            em_aberto = max(0.0, va - pa)
        _bloqueia_pagamento_maior(valor_saida, em_aberto, "boleto")

        obrigacao_id = (
            payload.get("obrigacao_id")
            or payload.get("parcela_obrigacao_id")
            or (parcela_boleto_escolhida.get("obrigacao_id") if isinstance(parcela_boleto_escolhida, dict) else None)
            or 0
        )

        origem = origem_dinheiro if forma_pagamento == "DINHEIRO" else _canonicalizar_banco_safe(caminho_banco, banco_escolhido_in)
        id_saida, id_mov, id_cap = ledger.pagar_parcela_boleto(
            data=data_str,
            valor=float(valor_saida),
            forma_pagamento=forma_pagamento,
            origem=origem,
            obrigacao_id=int(obrigacao_id),
            usuario=usuario_nome,
            categoria="Boletos",
            sub_categoria=subcat_nome,
            descricao=descricao_final,
            multa=float(multa_boleto),
            juros=float(juros_boleto),
            desconto=float(desconto_boleto),
        )

        # NOVO: obter restante/status pós-pagamento (fallback direto no banco)
        restante, status = _obter_restante_e_status(caminho_banco, int(obrigacao_id))

        # >>> PADRÃO UNIFICADO: Base, Ajustes, Total debitado
        ajustes_bol = float(multa_boleto) + float(juros_boleto) - float(desconto_boleto)
        total_debitado_bol = float(valor_saida) + ajustes_bol

        msg_sucesso_boleto = (
            "✅ Pagamento de boleto registrado! "
            f"Base: R$ {float(valor_saida):.2f} | "
            f"Ajustes (+multa+juros−desconto): R$ {ajustes_bol:.2f} | "
            f"Total debitado: R$ {total_debitado_bol:.2f} | "
            f"Restante: R$ {restante:.2f} | Status: {status} | "
            f"Saída: {id_saida or '—'} | Log: {id_mov or '—'} | Evento CAP: {id_cap or '—'}"
        )

        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if id_saida == -1 or id_mov == -1 or id_cap == -1
                else msg_sucesso_boleto
            ),
        }

    if is_pagamentos and tipo_pagamento_sel == "Empréstimos e Financiamentos":
        if not destino_pagamento_sel:
            raise ValueError("Selecione o banco/descrição do empréstimo.")
        if not parcela_emp_escolhida:
            raise ValueError("Selecione a parcela do empréstimo (ou informe o identificador).")

        # 🔒 BLOQUEIO: teto = em_aberto informado no item (ou reconstituído)
        em_aberto_emp = _as_float((parcela_emp_escolhida or {}).get("em_aberto"))
        if em_aberto_emp is None:
            va = _as_float((parcela_emp_escolhida or {}).get("valor_evento") or 0.0) or 0.0
            pa = _as_float((parcela_emp_escolhida or {}).get("pago_acumulado") or 0.0) or 0.0
            em_aberto_emp = max(0.0, va - pa)
        _bloqueia_pagamento_maior(valor_saida, em_aberto_emp, "emprestimo")

        obrigacao_id = (
            payload.get("obrigacao_id")
            or payload.get("parcela_obrigacao_id")
            or (parcela_emp_escolhida.get("obrigacao_id") if isinstance(parcela_emp_escolhida, dict) else None)
            or 0
        )

        origem = origem_dinheiro if forma_pagamento == "DINHEIRO" else _canonicalizar_banco_safe(caminho_banco, banco_escolhido_in)
        id_saida, id_mov, id_cap = ledger.pagar_parcela_emprestimo(
            data=data_str,
            valor=float(valor_saida),
            forma_pagamento=forma_pagamento,
            origem=origem,
            obrigacao_id=int(obrigacao_id),
            usuario=usuario_nome,
            categoria="Empréstimos e Financiamentos",
            sub_categoria=subcat_nome,
            descricao=descricao_final,
            multa=float(multa_emp),
            juros=float(juros_emp),
            desconto=float(desconto_emp),
        )

        # NOVO: obter restante/status pós-pagamento (fallback direto no banco)
        restante, status = _obter_restante_e_status(caminho_banco, int(obrigacao_id))

        # >>> PADRÃO UNIFICADO: Base, Ajustes, Total debitado
        ajustes_emp = float(multa_emp) + float(juros_emp) - float(desconto_emp)
        total_debitado_emp = float(valor_saida) + ajustes_emp

        msg_sucesso_emp = (
            "✅ Parcela de Empréstimo paga! "
            f"Base: R$ {float(valor_saida):.2f} | "
            f"Ajustes (+multa+juros−desconto): R$ {ajustes_emp:.2f} | "
            f"Total debitado: R$ {total_debitado_emp:.2f} | "
            f"Restante: R$ {restante:.2f} | Status: {status} | "
            f"Saída: {id_saida or '—'} | Log: {id_mov or '—'} | Evento CAP: {id_cap or '—'}"
        )

        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if id_saida == -1 or id_mov == -1 or id_cap == -1
                else msg_sucesso_emp
            ),
        }

    # ================== Fluxos Padrão ==================
    categoria = cat_nome
    sub_categoria = subcat_nome

    if forma_pagamento == "DINHEIRO":
        id_saida, id_mov = ledger.registrar_saida_dinheiro(
            data=data_str,
            valor=float(valor_saida),
            origem_dinheiro=origem_dinheiro,
            categoria=categoria,
            sub_categoria=sub_categoria,
            descricao=descricao_final,
            usuario=usuario_nome,
        )
        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if id_saida == -1
                else f"✅ Saída em dinheiro registrada! Valor: {valor_saida:.2f} | ID saída: {id_saida} | Log: {id_mov}"
            ),
        }

    if forma_pagamento in ["PIX", "DÉBITO"]:
        banco_nome = _canonicalizar_banco_safe(caminho_banco, banco_escolhido_in)
        id_saida, id_mov = ledger.registrar_saida_bancaria(
            data=data_str,
            valor=float(valor_saida),
            banco_nome=banco_nome,
            forma=forma_pagamento,
            categoria=categoria,
            sub_categoria=sub_categoria,
            descricao=descricao_final,
            usuario=usuario_nome,
        )
        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if id_saida == -1
                else f"✅ Saída bancária ({forma_pagamento}) registrada! Valor: {valor_saida:.2f} | ID saída: {id_saida} | Log: {id_mov}"
            ),
        }

    if forma_pagamento == "CRÉDITO":
        fc_vc = cartoes_repo.obter_por_nome(cartao_escolhido)
        if not fc_vc:
            raise ValueError("Cartão não encontrado. Cadastre em 📇 Cartão de Crédito.")
        vencimento, fechamento = fc_vc  # ordem preservada

        ids_fatura, id_mov = ledger.registrar_saida_credito(
            data_compra=data_str,
            valor=float(valor_saida),
            parcelas=int(parcelas),
            cartao_nome=cartao_escolhido,
            categoria=categoria,
            sub_categoria=sub_categoria,
            descricao=descricao_final,   # descrição detalhada para fatura_cartao_itens
            usuario=usuario_nome,
            fechamento=int(fechamento),
            vencimento=int(vencimento),
        )
        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if not ids_fatura
                else f"✅ Despesa em CRÉDITO programada! Valor: {valor_saida:.2f} | Parcelas criadas: {len(ids_fatura)} | Log: {id_mov}"
            ),
        }

    if forma_pagamento == "BOLETO":
        ids_cap, id_mov = ledger.registrar_saida_boleto(
            data_compra=data_str,
            valor=float(valor_saida),
            parcelas=int(parcelas),
            vencimento_primeira=str(venc_1),
            categoria=categoria,
            sub_categoria=sub_categoria,
            descricao=descricao_final,
            usuario=usuario_nome,
            fornecedor=(fornecedor or None),
            documento=(documento or None),
        )
        return {
            "ok": True,
            "msg": (
                "ℹ️ Transação já registrada (idempotência)."
                if not ids_cap
                else f"✅ Boleto programado! Valor: {valor_saida:.2f} | Parcelas criadas: {len(ids_cap)} | Log: {id_mov}"
            ),
        }

    # Se chegou aqui, forma desconhecida
    raise ValueError("Forma de pagamento inválida ou não suportada.")


# ---------------- Canonicalização de banco (igual ao original, tolerante a falha)
def _canonicalizar_banco_safe(caminho_banco: str, banco_in: str) -> str:
    try:
        return canonicalizar_banco(caminho_banco, (banco_in or "").strip()) or (banco_in or "").strip()
    except Exception:
        return (banco_in or "").strip()


# --- LISTAGEM: Boletos em aberto (privada p/ providers) ----------------------
def _listar_boletos_em_aberto(caminho_banco: str) -> list[dict]:
    return listar_boletos_em_aberto(caminho_banco)


# --- LISTAGEM: Boletos em aberto (pública/reutilizável) ----------------------
def listar_boletos_em_aberto(caminho_banco: str) -> list[dict]:
    """
    Lista parcelas de BOLETO em aberto, mostrando o VALOR EM ABERTO.
    em_aberto = valor_evento - valor_pago_acumulado
    Saída: [{label, obrigacao_id, parcela_id, credor, vencimento, valor_evento, pago_acumulado, em_aberto}]
    Requer: _resolver_colunas_evento_e_pago(conn) já definido no módulo.
    """
    eps = 0.005

    with get_conn(caminho_banco) as conn:
        col_evento, col_pago = _resolver_colunas_evento_e_pago(conn)
        sel_pago = f", COALESCE({col_pago}, 0.0) AS valor_pago_acum" if col_pago else ", 0.0 AS valor_pago_acum"

        df = pd.read_sql(
            f"""
            SELECT
                id                           AS parcela_id,
                COALESCE(obrigacao_id, 0)    AS obrigacao_id,
                TRIM(COALESCE(credor, ''))   AS credor,
                COALESCE(parcela_num, 1)     AS parcela_num,
                COALESCE(parcelas_total, 1)  AS parcelas_total,
                DATE(vencimento)             AS vencimento,
                COALESCE({col_evento}, 0.0)  AS valor_evento
                {sel_pago},
                UPPER(TRIM(COALESCE(tipo_obrigacao,''))) AS u_tipo
            FROM contas_a_pagar_mov
            WHERE categoria_evento = 'LANCAMENTO'
              AND UPPER(COALESCE(status, 'EM ABERTO')) IN ('EM ABERTO', 'PARCIAL')
            ORDER BY DATE(vencimento) ASC, credor ASC, parcela_num ASC
            """,
            conn,
        )

    if df is None or df.empty:
        return []

    # aceita BOLETO ou variações (ex: 'BOLETO XYZ')
    df = df[(df["u_tipo"] == "BOLETO") | (df["u_tipo"].str.startswith("BOLETO"))]
    if df.empty:
        return []

    # em_aberto = max(valor_evento - valor_pago_acum, 0)
    df["em_aberto"] = (df["valor_evento"] - df["valor_pago_acum"]).clip(lower=0.0)
    df = df[df["em_aberto"] > eps]  # evita listar valores residuais por arredondamento

    def _fmt_row(r):
        credor = (r["credor"] or "").strip() or "(sem credor)"
        par    = int(r["parcela_num"] or 1)
        tot    = int(r["parcelas_total"] or par)
        venc   = str(r["vencimento"] or "")
        try:
            venc_pt = pd.to_datetime(venc).strftime("%d/%m/%Y") if venc else "—"
        except Exception:
            venc_pt = "—"
        em_aberto = float(r["em_aberto"] or 0.0)
        rotulo = f"{credor} • Parc {par}/{tot} • Venc {venc_pt} • R$ {em_aberto:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return {
            "label": rotulo,
            "obrigacao_id": int(r["obrigacao_id"] or 0),
            "parcela_id": int(r["parcela_id"]),
            "credor": credor,
            "vencimento": venc,
            "valor_evento": float(r["valor_evento"] or 0.0),
            "pago_acumulado": float(r["valor_pago_acum"] or 0.0),
            "em_aberto": em_aberto,
            "parcela_num": par,
            "parcelas_total": tot,
        }

    return [_fmt_row(r) for _, r in df.iterrows()]


__all__ = [
    "carregar_listas_para_form",
    "registrar_saida",
    "listar_boletos_em_aberto",
]
