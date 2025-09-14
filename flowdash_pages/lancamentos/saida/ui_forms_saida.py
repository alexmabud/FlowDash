# ===================== UI Forms: Saída =====================
"""
Componentes de UI para Saída. Apenas interface – sem regra/SQL.
Mantém a mesma experiência do módulo original (campos e fluxos).
"""

from __future__ import annotations

import streamlit as st
import pandas as pd
from datetime import date
from typing import Optional, List, Callable

FORMAS = ["DINHEIRO", "PIX", "DÉBITO", "CRÉDITO", "BOLETO"]
ORIGENS_DINHEIRO = ["Caixa", "Caixa 2"]

def render_form_saida(
    data_lanc: date,
    invalidate_cb,
    nomes_bancos: list[str],
    nomes_cartoes: list[str],
    categorias_df,  # DataFrame com colunas (id, nome)
    listar_subcategorias_fn,  # fn(cat_id)->DataFrame
    listar_destinos_fatura_em_aberto_fn: Callable[[], list],  # fn()->list[dict]
    carregar_opcoes_pagamentos_fn,  # legacy (mantido p/ compat)
    listar_boletos_em_aberto_fn: Optional[Callable[[], list]] = None,
    listar_empfin_em_aberto_fn: Optional[Callable[[], list]] = None,
):
    st.markdown("#### 📤 Lançar Saída")
    st.caption(f"Data do lançamento: **{data_lanc}**")

    # ===================== CAMPOS GERAIS =====================
    valor_saida = st.number_input(
        "Valor da Saída",
        min_value=0.0, step=0.01, format="%.2f",
        key="valor_saida", on_change=invalidate_cb
    )
    forma_pagamento = st.selectbox(
        "Forma de Pagamento", FORMAS,
        key="forma_pagamento_saida", on_change=invalidate_cb
    )

    # ===================== CATEGORIA / SUBCATEGORIA / PAGAMENTOS =====================
    if categorias_df is not None and not categorias_df.empty:
        cat_nome = st.selectbox(
            "Categoria", categorias_df["nome"].tolist(),
            key="categoria_saida", on_change=invalidate_cb
        )
        cat_id = int(categorias_df[categorias_df["nome"] == cat_nome].iloc[0]["id"])
    else:
        st.info("Dica: cadastre categorias em **Cadastro → 📂 Cadastro de Saídas**.")
        cat_nome = st.text_input("Categoria (digite)", key="categoria_saida_text")
        cat_id = None

    is_pagamentos = (cat_nome or "").strip().lower() == "pagamentos"

    # Campos usados no processamento
    subcat_nome = None
    tipo_pagamento_sel: Optional[str] = None
    destino_pagamento_sel: Optional[str] = None

    # >>> FATURA <<<
    competencia_fatura_sel: Optional[str] = None
    obrigacao_id_fatura: Optional[int] = None
    multa_fatura = juros_fatura = desconto_fatura = 0.0

    # >>> BOLETO <<<
    parcela_boleto_escolhida: Optional[dict] = None
    multa_boleto = juros_boleto = desconto_boleto = 0.0
    obrigacao_id_boleto: Optional[int] = None

    # >>> EMPRÉSTIMO <<<
    parcela_emp_escolhida: Optional[dict] = None
    multa_emp = juros_emp = desconto_emp = 0.0
    obrigacao_id_emp: Optional[int] = None

    if is_pagamentos:
        tipo_pagamento_sel = st.selectbox(
            "Tipo de Pagamento",
            ["Fatura Cartão de Crédito", "Empréstimos e Financiamentos", "Boletos"],
            key="tipo_pagamento_pagamentos", on_change=invalidate_cb
        )

        # ---------- FATURA CARTÃO DE CRÉDITO ----------
        if tipo_pagamento_sel == "Fatura Cartão de Crédito":
            # Filtra para garantir que só apareçam faturas com saldo > 0
            f_raw = listar_destinos_fatura_em_aberto_fn() or []
            faturas = [f for f in f_raw if float(f.get("saldo") or 0.0) > 0.00001]

            if not faturas:
                st.warning("Não há faturas de cartão em aberto.")
            else:
                # Usa o label pronto vindo do repositório (já inclui o saldo faltante)
                opcoes = [f["label"] for f in faturas]
                escolha = st.selectbox(
                    "Fatura em aberto",
                    opcoes, key="destino_fatura_comp", on_change=invalidate_cb
                )
                if escolha:
                    f_sel = next(f for f in faturas if f["label"] == escolha)
                    destino_pagamento_sel = f_sel.get("cartao", "")
                    competencia_fatura_sel = f_sel.get("competencia", "")
                    obrigacao_id_fatura = int(f_sel.get("obrigacao_id"))

                    # Saldo faltante (não usa valor total)
                    saldo_restante = float(f_sel.get("saldo") or 0.0)

                    st.caption(
                        f"Selecionado: {destino_pagamento_sel} — {competencia_fatura_sel} • obrigação #{obrigacao_id_fatura}"
                    )

                    # Mostra o saldo restante (somente leitura) para referência
                    st.number_input(
                        "Saldo restante da fatura (R$)",
                        value=saldo_restante,
                        step=0.01,
                        format="%.2f",
                        disabled=True,
                        key="saldo_restante_fatura_ro",
                        help="Para fatura, utilize o campo 'Valor da Saída' acima como principal."
                    )

                    # (REMOVIDO) Campo 'Valor a pagar (principal)' — para fatura usamos 'Valor da Saída'
                    # Mantemos os ajustes normalmente:
                    colf1, colf2, colf3 = st.columns(3)
                    with colf1:
                        multa_fatura = st.number_input("Multa (+)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="multa_fatura")
                    with colf2:
                        juros_fatura = st.number_input("Juros (+)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="juros_fatura")
                    with colf3:
                        desconto_fatura = st.number_input("Desconto (−)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="desconto_fatura")

                    total_saida_fatura = float(valor_saida) + float(multa_fatura) + float(juros_fatura) - float(desconto_fatura)
                    st.caption(f"Total da saída (caixa/banco): R$ {total_saida_fatura:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

        # ---------- EMPRÉSTIMOS E FINANCIAMENTOS ----------
        elif tipo_pagamento_sel == "Empréstimos e Financiamentos":
            if listar_empfin_em_aberto_fn is None:
                st.error("Faltando provider: `listar_empfin_em_aberto_fn()` não foi informado.")
            else:
                itens = listar_empfin_em_aberto_fn() or []
                if not itens:
                    st.warning("Não há parcelas de empréstimos/financiamentos em aberto.")
                else:
                    opcoes = [i["label"] for i in itens]
                    escolha = st.selectbox("Selecione a parcela em aberto", opcoes, key="emp_parcela_em_aberto", on_change=invalidate_cb)
                    if escolha:
                        it = next(i for i in itens if i["label"] == escolha)
                        destino_pagamento_sel = it.get("credor") or it.get("banco") or it.get("descricao") or ""
                        obrigacao_id_emp = int(it.get("obrigacao_id"))
                        parcela_emp_escolhida = it
                        st.caption(
                            f"Selecionado: {destino_pagamento_sel} • obrigação #{obrigacao_id_emp}"
                            + (f" • parcela #{it.get('parcela_id')}" if it.get("parcela_id") else "")
                        )

                        st.number_input(
                            "Valor do pagamento (pode ser parcial)",
                            value=float(valor_saida),
                            step=0.01,
                            format="%.2f",
                            disabled=True,
                            key="valor_pagamento_emp_ro",
                            help="Este valor vem de 'Valor da Saída'. Para alterar, edite o campo acima."
                        )
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            multa_emp = st.number_input("Multa (+)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="multa_emp")
                        with c2:
                            juros_emp = st.number_input("Juros (+)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="juros_emp")
                        with c3:
                            desconto_emp = st.number_input("Desconto (−)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="desconto_emp")

                        total_saida_emp = float(valor_saida) + float(multa_emp) + float(juros_emp) - float(desconto_emp)
                        st.caption(f"Total da saída (caixa/banco): R$ {total_saida_emp:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

        # ---------- BOLETOS ----------
        elif tipo_pagamento_sel == "Boletos":
            if listar_boletos_em_aberto_fn is None:
                st.error("Faltando provider: `listar_boletos_em_aberto_fn()` não foi informado.")
            else:
                boletos = listar_boletos_em_aberto_fn() or []
                if not boletos:
                    st.warning("Não há boletos em aberto.")
                else:
                    opcoes = [b["label"] for b in boletos]
                    escolha = st.selectbox("Selecione o boleto/parcela em aberto", opcoes, key="boleto_em_aberto", on_change=invalidate_cb)
                    if escolha:
                        b = next(i for i in boletos if i["label"] == escolha)
                        destino_pagamento_sel = b.get("credor") or b.get("descricao") or ""
                        obrigacao_id_boleto = int(b.get("obrigacao_id"))
                        parcela_boleto_escolhida = b
                        st.caption(
                            f"Selecionado: {destino_pagamento_sel} • obrigação #{obrigacao_id_boleto}"
                            + (f" • parcela #{b.get('parcela_id')}" if b.get("parcela_id") else "")
                        )

                        st.number_input(
                            "Valor do pagamento (pode ser parcial)",
                            value=float(valor_saida),
                            step=0.01,
                            format="%.2f",
                            disabled=True,
                            key="valor_pagamento_boleto_ro",
                            help="Este valor vem de 'Valor da Saída'. Para alterar, edite o campo acima."
                        )
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            multa_boleto = st.number_input("Multa (+)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="multa_boleto")
                        with col2:
                            juros_boleto = st.number_input("Juros (+)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="juros_boleto")
                        with col3:
                            desconto_boleto = st.number_input("Desconto (−)", min_value=0.0, step=1.0, format="%.2f", value=0.0, key="desconto_boleto")

                        total_saida_calc = float(valor_saida) + float(juros_boleto) + float(multa_boleto) - float(desconto_boleto)
                        st.caption(f"Total da saída (caixa/banco): R$ {total_saida_calc:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

    else:
        # Subcategoria comum (fora de Pagamentos)
        if cat_id:
            df_sub = listar_subcategorias_fn(cat_id)
            if df_sub is not None and not df_sub.empty:
                subcat_nome = st.selectbox("Subcategoria", df_sub["nome"].tolist(), key="subcategoria_saida", on_change=invalidate_cb)
            else:
                subcat_nome = st.text_input("Subcategoria (digite)", key="subcategoria_saida_text")
        else:
            subcat_nome = st.text_input("Subcategoria (digite)", key="subcategoria_saida_text")

    # ===================== CAMPOS CONDICIONAIS À FORMA =====================
    esconder_descricao = bool(
        is_pagamentos and (tipo_pagamento_sel in ["Fatura Cartão de Crédito", "Empréstimos e Financiamentos", "Boletos"])
    )

    parcelas = 1
    cartao_escolhido = ""
    banco_escolhido = ""
    origem_dinheiro = ""
    venc_1: Optional[date] = None
    credor_boleto = ""
    documento = ""  # compatibilidade

    descricao_digitada = ""

    if forma_pagamento == "CRÉDITO":
        parcelas = st.selectbox("Parcelas", list(range(1, 13)), key="parcelas_saida", on_change=invalidate_cb)
        if nomes_cartoes:
            cartao_escolhido = st.selectbox("Cartão de Crédito", nomes_cartoes, key="saida_cartao_credito", on_change=invalidate_cb)
        else:
            st.warning("⚠️ Nenhum cartão de crédito cadastrado.")
        if not esconder_descricao:
            descricao_digitada = st.text_input("Descrição (opcional)", key="descricao_saida_credito")

    elif forma_pagamento == "DINHEIRO":
        origem_dinheiro = st.selectbox("Origem do Dinheiro", ORIGENS_DINHEIRO, key="origem_dinheiro", on_change=invalidate_cb)
        if not esconder_descricao:
            descricao_digitada = st.text_input("Descrição (opcional)", key="descricao_saida_dinheiro")

    elif forma_pagamento in ["PIX", "DÉBITO"]:
        if nomes_bancos:
            banco_escolhido = st.selectbox("Banco da Saída", nomes_bancos, key="saida_banco_saida", on_change=invalidate_cb)
        else:
            banco_escolhido = st.text_input("Banco da Saída (digite)", key="saida_banco_saida_text", on_change=invalidate_cb)
        if not esconder_descricao:
            descricao_digitada = st.text_input("Descrição (opcional)", key="descricao_saida_banco")

    elif forma_pagamento == "BOLETO":
        # Este bloco é para PROGRAMAR boletos (fora de Pagamentos)
        parcelas = st.selectbox("Parcelas", list(range(1, 37)), index=0, key="parcelas_boleto", on_change=invalidate_cb)
        venc_1 = st.date_input("Vencimento da 1ª parcela", value=date.today(), key="venc1_boleto")
        credor_boleto = st.text_input("Credor (Fornecedor)", key="credor_boleto")
        if not esconder_descricao:
            descricao_digitada = st.text_input("Descrição (opcional)", key="descricao_saida_boleto")

    # ===================== DESCRIÇÃO (para CONTAS A PAGAR) =====================
    meta_tag = ""
    if is_pagamentos:
        tipo_txt = (tipo_pagamento_sel or "").strip()
        dest_txt = (destino_pagamento_sel or "").strip()
        # Novo padrão solicitado: "PAGAMENTO <tipo> <destino>"
        if tipo_txt and dest_txt:
            meta_tag = f"PAGAMENTO {tipo_txt} {dest_txt}"
        elif tipo_txt:
            meta_tag = f"PAGAMENTO {tipo_txt}"
        else:
            meta_tag = "PAGAMENTO"

    descricao_final = " ".join([(descricao_digitada or "").strip(), meta_tag]).strip() if not esconder_descricao else meta_tag

    # ===================== RESUMO =====================
    data_saida_str = data_lanc.strftime("%d/%m/%Y")
    linhas_md = [
        "**Confirme os dados da saída**",
        f"- **Data:** {data_saida_str}",
        f"- **Valor:** R$ {valor_saida:.2f}",
        f"- **Forma de pagamento:** {forma_pagamento}",
        f"- **Categoria:** {cat_nome or '—'}",
        (f"- **Subcategoria:** {subcat_nome or '—'}") if not is_pagamentos else (f"- **Tipo Pagamento:** {tipo_pagamento_sel or '—'}"),
        (f"- **Destino:** {destino_pagamento_sel or '—'}") if is_pagamentos else "",
        f"- **Descrição:** {descricao_final or 'N/A'}",
    ]
    if forma_pagamento == "CRÉDITO":
        linhas_md += [f"- **Parcelas:** {parcelas}x", f"- **Cartão de Crédito (credor):** {cartao_escolhido or '—'}"]
    elif forma_pagamento == "DINHEIRO":
        linhas_md += [f"- **Origem do Dinheiro:** {origem_dinheiro or '—'}"]
    elif forma_pagamento in ["PIX", "DÉBITO"]:
        linhas_md += [f"- **Banco da Saída:** {(banco_escolhido or '').strip() or '—'}"]
    elif forma_pagamento == "BOLETO":
        linhas_md += [
            f"- **Parcelas:** {parcelas}x",
            f"- **Vencimento 1ª Parcela:** {venc_1.strftime('%d/%m/%Y') if venc_1 else '—'}",
            f"- **Credor:** {credor_boleto or '—'}",
        ]

    st.info("\n".join([l for l in linhas_md if l != ""]))

    confirmado = st.checkbox("Está tudo certo com os dados acima?", key="confirmar_saida")

    # Mensagem fixa sob o checkbox (não some, não muda)
    st.info("Confirme os dados para habilitar o botão de salvar.")

    # --------- mapear 'credor' por forma ----------
    credor_val = ""
    if forma_pagamento == "CRÉDITO":
        credor_val = (cartao_escolhido or "").strip()
    elif forma_pagamento == "BOLETO":
        credor_val = (credor_boleto or "").strip()

    return {
        # Importante: para Fatura, usamos o "Valor da Saída" como principal
        "valor_saida": float(valor_saida or 0.0),
        "forma_pagamento": forma_pagamento,
        "cat_nome": (cat_nome or "").strip(),
        "cat_id": cat_id,
        "subcat_nome": (subcat_nome or "").strip() if subcat_nome else None,

        # Pagamentos
        "is_pagamentos": bool(is_pagamentos),
        "tipo_pagamento_sel": (tipo_pagamento_sel or "").strip() if is_pagamentos else None,
        "destino_pagamento_sel": (destino_pagamento_sel or "").strip() if is_pagamentos else None,

        # Fatura
        "competencia_fatura_sel": obrigacao_id_fatura and competencia_fatura_sel or competencia_fatura_sel,
        "obrigacao_id_fatura": obrigacao_id_fatura,
        "multa_fatura": float(multa_fatura),
        "juros_fatura": float(juros_fatura),
        "desconto_fatura": float(desconto_fatura),

        # Boletos (AGORA: selecionados da lista de abertos)
        "obrigacao_id": obrigacao_id_boleto,
        "parcela_boleto_escolhida": parcela_boleto_escolhida,
        "multa_boleto": float(multa_boleto),
        "juros_boleto": float(juros_boleto),
        "desconto_boleto": float(desconto_boleto),

        # Empréstimos (AGORA: selecionados da lista de abertos)
        "parcela_emp_escolhida": parcela_emp_escolhida,
        "multa_emp": float(multa_emp),
        "juros_emp": float(juros_emp),
        "desconto_emp": float(desconto_emp),
        "parcela_obrigacao_id": obrigacao_id_emp,

        # Comuns/forma
        "parcelas": int(parcelas or 1),
        "cartao_escolhido": (cartao_escolhido or "").strip(),
        "banco_escolhido": (banco_escolhido or "").strip(),
        "origem_dinheiro": (origem_dinheiro or "").strip(),
        "venc_1": venc_1,

        # Credor e Descrição (para CONTAS A PAGAR)
        "credor": credor_val,
        "descricao_final": descricao_final,

        # Compat antigos
        "documento": "",
        "fornecedor": credor_boleto,  # mantém por compat, mas o valor real está em "credor"

        "confirmado": bool(confirmado),
    }
