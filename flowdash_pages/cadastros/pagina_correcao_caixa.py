import re
import sqlite3
import streamlit as st
import pandas as pd
from datetime import date
from utils.utils import formatar_valor
from .cadastro_classes import CorrecaoCaixaRepository
from repository.movimentacoes_repository import MovimentacoesRepository
from shared.ids import uid_correcao_caixa
from shared.db import get_conn


# ------------------------------------------------------------------------------------
# Lança em movimentacoes_bancarias com referência ao ajuste (com idempotência)
def inserir_mov_bancaria_correcao(
    caminho_banco: str,
    data_: str,
    banco: str,
    valor: float,
    ref_id: int,
    obs: str = ""
) -> int | None:
    """
    Cria um movimento em movimentacoes_bancarias vinculado ao ajuste de caixa,
    preenchendo referencia_tabela, referencia_id e trans_uid (hash).
    Usa registrar_entrada/registrar_saida do MovimentacoesRepository (idempotente).
    """
    data_s = str(data_).strip()
    banco_s = (banco or "").strip()
    obs_s = (obs or "Ajuste manual de caixa").strip()
    ref_tab = "correcao_caixa"
    ref_id_i = int(ref_id) if ref_id else None

    if not banco_s or valor is None or float(valor) == 0.0:
        return None

    # UID padronizado para idempotência
    trans_uid = uid_correcao_caixa(data_s, banco_s, float(valor), obs_s, ref_id_i)

    mov_repo = MovimentacoesRepository(caminho_banco)
    if float(valor) > 0:
        return mov_repo.registrar_entrada(
            data=data_s,
            banco=banco_s,
            valor=float(valor),
            origem="correcao_caixa",
            observacao=obs_s,
            referencia_tabela=ref_tab,
            referencia_id=ref_id_i,
            trans_uid=trans_uid
        )
    else:
        return mov_repo.registrar_saida(
            data=data_s,
            banco=banco_s,
            valor=abs(float(valor)),
            origem="correcao_caixa",
            observacao=obs_s,
            referencia_tabela=ref_tab,
            referencia_id=ref_id_i,
            trans_uid=trans_uid
        )


# ------------------------------------------------------------------------------------
# Helpers de DB para snapshots (saldos_caixas/saldos_bancos)
def _sanitize_col_name(col: str) -> str:
    """Permite letras, números, espaços e _. Demais caracteres são removidos (anti-injeção)."""
    col = (col or "").strip()
    col = re.sub(r"[^0-9A-Za-z _]", "", col)
    return col


def _col_exists(conn: sqlite3.Connection, tabela: str, col: str) -> bool:
    info = conn.execute(f'PRAGMA table_info("{tabela}")').fetchall()
    cols = {row[1] for row in info}
    return col in cols


def _get_table_cols(conn: sqlite3.Connection, tabela: str) -> set[str]:
    info = conn.execute(f'PRAGMA table_info("{tabela}")').fetchall()
    return {row[1] for row in info}


def _init_row_saldos_caixas_if_missing(conn: sqlite3.Connection, data_str: str) -> None:
    """
    Garante 1 linha em saldos_caixas na data. Se não houver, cria a linha
    carregando o snapshot do dia anterior:
      - caixa        ← prev.caixa_total (ou 0)
      - caixa_vendas ← 0.0
      - caixa_total  ← caixa + caixa_vendas
      - caixa_2      ← prev.caixa2_total (ou 0)
      - caixa2_dia   ← 0.0
      - caixa2_total ← caixa_2 + caixa2_dia
    Apenas popula colunas que existirem na tabela.
    """
    cur = conn.execute("SELECT 1 FROM saldos_caixas WHERE date(data)=date(?) LIMIT 1", (data_str,))
    if cur.fetchone() is not None:
        return  # já existe

    # Busca linha anterior (max data < data_str)
    prev = conn.execute(
        """
        SELECT *
        FROM saldos_caixas
        WHERE date(data) < date(?)
        ORDER BY date(data) DESC
        LIMIT 1
        """,
        (data_str,),
    ).fetchone()

    cols = _get_table_cols(conn, "saldos_caixas")

    prev_caixa_total = 0.0
    prev_caixa2_total = 0.0
    if prev is not None:
        # Mapeia nome->índice
        names = [d[1] for d in conn.execute('PRAGMA table_info("saldos_caixas")').fetchall()]
        row = dict(zip(names, prev))
        prev_caixa_total = float(row.get("caixa_total") or 0.0)
        prev_caixa2_total = float(row.get("caixa2_total") or 0.0)

    # Monta os valores iniciais respeitando as colunas existentes
    values = {"data": data_str}
    if "caixa" in cols:
        values["caixa"] = prev_caixa_total
    if "caixa_vendas" in cols:
        values["caixa_vendas"] = 0.0
    if "caixa_total" in cols:
        values["caixa_total"] = float(values.get("caixa", 0.0)) + float(values.get("caixa_vendas", 0.0))
    if "caixa_2" in cols:
        values["caixa_2"] = prev_caixa2_total
    if "caixa2_dia" in cols:
        values["caixa2_dia"] = 0.0
    if "caixa2_total" in cols:
        values["caixa2_total"] = float(values.get("caixa_2", 0.0)) + float(values.get("caixa2_dia", 0.0))

    # INSERT dinâmico só com as colunas que existem
    col_names = ", ".join(f'"{k}"' for k in values.keys())
    placeholders = ", ".join(["?"] * len(values))
    conn.execute(f'INSERT INTO saldos_caixas ({col_names}) VALUES ({placeholders})', tuple(values.values()))


def aplicar_delta_caixa(caminho_banco: str, data_str: str, coluna: str, delta: float) -> None:
    """
    Aplica delta em saldos_caixas.<coluna> da data.
    - Se a linha NÃO existir, cria a linha carregando os totais do dia anterior (ver _init_row_saldos_caixas_if_missing).
    - Recalcula caixa_total/caixa2_total após o delta, se as colunas existirem.
    """
    coluna = _sanitize_col_name(coluna)
    if coluna not in ("caixa", "caixa_2"):
        raise ValueError("Coluna inválida para saldos_caixas. Use 'caixa' ou 'caixa_2'.")

    with get_conn(caminho_banco) as conn:
        # Inicializa linha do dia com os saldos do dia anterior
        _init_row_saldos_caixas_if_missing(conn, data_str)

        # Aplica delta na coluna-alvo
        conn.execute(
            f'UPDATE saldos_caixas SET "{coluna}" = COALESCE("{coluna}", 0) + ? '
            "WHERE date(data)=date(?)",
            (float(delta), data_str),
        )

        # Recalcula totais derivados, se existirem
        if _col_exists(conn, "saldos_caixas", "caixa_total") and _col_exists(conn, "saldos_caixas", "caixa_vendas"):
            conn.execute(
                'UPDATE saldos_caixas '
                'SET "caixa_total" = COALESCE("caixa",0) + COALESCE("caixa_vendas",0) '
                'WHERE date(data)=date(?)',
                (data_str,),
            )

        if _col_exists(conn, "saldos_caixas", "caixa2_total") and _col_exists(conn, "saldos_caixas", "caixa2_dia"):
            conn.execute(
                'UPDATE saldos_caixas '
                'SET "caixa2_total" = COALESCE("caixa_2",0) + COALESCE("caixa2_dia",0) '
                'WHERE date(data)=date(?)',
                (data_str,),
            )


def aplicar_delta_banco(caminho_banco: str, data_str: str, nome_banco_coluna: str, delta: float) -> None:
    """
    Soma/subtrai `delta` na coluna do banco em saldos_bancos da data.
    Usa o texto do select como nome de coluna (deve existir em saldos_bancos).
    """
    col = _sanitize_col_name(nome_banco_coluna)
    if not col:
        raise ValueError("Nome da coluna do banco inválido.")

    with get_conn(caminho_banco) as conn:
        # Se a linha do dia não existir, cria pelo menos a data (sem carry-over aqui)
        cur = conn.execute("SELECT 1 FROM saldos_bancos WHERE date(data)=date(?) LIMIT 1", (data_str,))
        if cur.fetchone() is None:
            conn.execute("INSERT INTO saldos_bancos (data) VALUES (date(?))", (data_str,))

        if not _col_exists(conn, "saldos_bancos", col):
            raise ValueError(f'A coluna do banco "{col}" não existe em saldos_bancos.')

        conn.execute(
            f'UPDATE saldos_bancos SET "{col}" = COALESCE("{col}", 0) + ? '
            "WHERE date(data)=date(?)",
            (float(delta), data_str),
        )


# ------------------------------------------------------------------------------------
# Opções de conta/banco para correção (Caixa, Caixa 2 + bancos cadastrados)
def carregar_opcoes_banco(caminho_banco: str) -> list[str]:
    opcoes = ["Caixa", "Caixa 2"]
    try:
        with get_conn(caminho_banco) as conn:
            df = pd.read_sql("SELECT nome FROM bancos_cadastrados ORDER BY nome", conn)
        if not df.empty:
            opcoes.extend(df["nome"].tolist())
    except Exception:
        pass
    return opcoes


# ------------------------------------------------------------------------------------
def pagina_correcao_caixa(caminho_banco: str):
    st.subheader("🧮 Correção Manual de Caixa")
    repo = CorrecaoCaixaRepository(caminho_banco)

    # Mensagem pós-rerun
    if st.session_state.get("correcao_msg_ok"):
        st.success(st.session_state.pop("correcao_msg_ok"))

    # Formulário
    data_corrigir = st.date_input("Data do ajuste", value=date.today())
    bancos = carregar_opcoes_banco(caminho_banco)
    destino_banco = st.selectbox("Conta/Banco do ajuste", bancos)

    col1, col2 = st.columns(2)
    with col1:
        # valor pode ser positivo (entrada) ou negativo (saída)
        valor_ajuste = st.number_input(
            "Valor do ajuste (use negativo para saída)",
            step=10.0, format="%.2f"
        )
    with col2:
        lancar_mov = st.checkbox("Lançar também em movimentações bancárias", value=True)

    observacao = st.text_input("Observação (opcional)", value="Ajuste manual de caixa")

    if st.button("✔️ Registrar Ajuste", use_container_width=True):
        try:
            if not destino_banco:
                st.warning("Selecione uma conta/banco.")
                return
            if valor_ajuste == 0:
                st.warning("Informe um valor diferente de zero.")
                return

            data_str = str(data_corrigir)

            # 1) grava ajuste e captura o ID
            ajuste_id = repo.salvar_ajuste(
                data_=data_str,
                valor=float(valor_ajuste),
                observacao=observacao or ""
            )

            # 2) aplica delta conforme o destino selecionado
            dest_norm = (destino_banco or "").strip().lower()
            try:
                if dest_norm == "caixa":
                    aplicar_delta_caixa(caminho_banco, data_str, "caixa", float(valor_ajuste))
                elif dest_norm in ("caixa 2", "caixa2"):
                    aplicar_delta_caixa(caminho_banco, data_str, "caixa_2", float(valor_ajuste))
                else:
                    # Banco: usa o nome exibido como nome da coluna em saldos_bancos
                    aplicar_delta_banco(caminho_banco, data_str, destino_banco, float(valor_ajuste))
            except Exception as e:
                # Não bloqueia o fluxo (ajuste já foi salvo); mostra erro claro da aplicação do delta
                st.error(f"Falha ao aplicar delta no snapshot: {e}")

            # 3) opcional: espelhar em movimentações com referência e trans_uid
            mov_id = None
            if lancar_mov:
                mov_id = inserir_mov_bancaria_correcao(
                    caminho_banco=caminho_banco,
                    data_=data_str,
                    banco=destino_banco,
                    valor=float(valor_ajuste),
                    ref_id=ajuste_id,
                    obs=observacao
                )

            tipo_txt = "entrada" if valor_ajuste > 0 else "saída"
            msg = (
                f"✅ Ajuste registrado: **{tipo_txt.upper()}** de {formatar_valor(abs(valor_ajuste))} "
                f"em **{destino_banco}** (ID ajuste: {ajuste_id}"
                + (f", mov: {mov_id}" if mov_id else "") + ")."
            )
            st.session_state["correcao_msg_ok"] = msg
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao registrar ajuste: {e}")

    st.markdown("---")

    # Resumo do dia escolhido
    st.markdown("### 📅 Resumo do dia selecionado")
    try:
        df_aj = repo.listar_ajustes()
        if isinstance(df_aj, pd.DataFrame) and not df_aj.empty:
            col_data = "data" if "data" in df_aj.columns else None
            col_valor = "valor" if "valor" in df_aj.columns else None

            if col_data:
                df_aj[col_data] = pd.to_datetime(df_aj[col_data], errors="coerce")
                df_dia = df_aj[df_aj[col_data].dt.date == data_corrigir]
            else:
                df_dia = pd.DataFrame()

            if not df_dia.empty and col_valor:
                total_pos = df_dia[df_dia[col_valor] > 0][col_valor].sum()
                total_neg = df_dia[df_dia[col_valor] < 0][col_valor].sum()
                st.info(
                    f"**Entradas:** {formatar_valor(total_pos)} • "
                    f"**Saídas:** {formatar_valor(abs(total_neg))} • "
                    f"**Saldo do dia:** {formatar_valor((total_pos + total_neg))}"
                )
            else:
                st.caption("Sem ajustes para esta data.")
        else:
            st.caption("Sem ajustes cadastrados.")
    except Exception as e:
        st.error(f"Erro ao verificar correções do dia: {e}")

    # Histórico geral
    st.markdown("### 🗂️ Histórico de ajustes")
    try:
        df_ajustes = repo.listar_ajustes()
        if isinstance(df_ajustes, pd.DataFrame) and not df_ajustes.empty:
            if "data" in df_ajustes.columns:
                df_ajustes["data"] = pd.to_datetime(df_ajustes["data"]).dt.strftime("%d/%m/%Y")
            if "valor" in df_ajustes.columns:
                df_ajustes["valor"] = df_ajustes["valor"].apply(formatar_valor)

            ren = {}
            if "data" in df_ajustes.columns:
                ren["data"] = "Data"
            if "valor" in df_ajustes.columns:
                ren["valor"] = "Valor (R$)"
            if "observacao" in df_ajustes.columns:
                ren["observacao"] = "Observação"

            df_show = df_ajustes.rename(columns=ren)
            cols = [c for c in ["Data", "Valor (R$)", "Observação"] if c in df_show.columns]
            st.dataframe(df_show[cols], use_container_width=True, hide_index=True)
        else:
            st.info("Nenhum ajuste registrado ainda.")
    except Exception as e:
        st.error(f"Erro ao carregar ajustes: {e}")
