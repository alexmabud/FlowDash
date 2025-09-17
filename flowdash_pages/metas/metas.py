# flowdash_pages/metas/metas.py
from __future__ import annotations

from datetime import date, timedelta
from typing import Tuple, Optional, List
import re
import os
import sqlite3

import pandas as pd
import streamlit as st
import plotly.graph_objects as go

try:
    from utils.utils import formatar_moeda as _fmt
except Exception:
    def _fmt(v):
        try:
            return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except Exception:
            return str(v)

__all__ = ["page_metas", "render_metas_auto", "render", "render_metas"]

# ============================= Helpers =============================
_PT_WEEK = {0: "segunda", 1: "terca", 2: "quarta", 3: "quinta", 4: "sexta", 5: "sabado", 6: "domingo"}

def _coluna_dia(d: date) -> str: return _PT_WEEK[d.weekday()]
def _inicio_semana(d: date) -> date: return d - timedelta(days=d.weekday())
def _calcular_percentual(valor: float, meta: float) -> float:
    if not meta or meta <= 0: return 0.0
    return round((float(valor) / float(meta)) * 100.0, 1)

def _hex_to_rgb(h: str) -> Tuple[int, int, int]:
    h = h.strip().lstrip("#");  h = "".join(c*2 for c in h) if len(h)==3 else h
    return int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
def _tint_rgba(hex_color: str, alpha: float = 0.25) -> str:
    r,g,b = _hex_to_rgb(hex_color); a = max(0.0, min(1.0, float(alpha))); return f"rgba({r},{g},{b},{a})"
def _slug_key(s: str) -> str:
    s = str(s or "").strip().lower();  s = re.sub(r"[^a-z0-9]+", "_", s);  return s or "anon"

# ======================= Gauge (zonas + label R$) =======================
def _gauge_percentual_zonas(titulo: str, percentual: float, bronze_pct: float, prata_pct: float,
                            axis_max: float = 120.0, bar_color_rgba: str = "rgba(0,200,83,0.75)",
                            valor_label: Optional[str] = None) -> go.Figure:
    bronze = max(0.0, min(100.0, float(bronze_pct)))
    prata  = max(bronze, min(100.0, float(prata_pct)))
    max_axis = max(100.0, float(axis_max))
    value = float(max(0.0, min(max_axis, percentual)))

    steps = [
        {"range": [0, bronze],      "color": "#E53935"},  # vermelho
        {"range": [bronze, prata],  "color": "#CD7F32"},  # bronze
        {"range": [prata, 100],     "color": "#C0C0C0"},  # prata
        {"range": [100, max_axis],  "color": "#FFD700"},  # ouro
    ]

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        number={"suffix": "%"},
        title={"text": titulo, "font": {"size": 18}},
        gauge={
            "shape": "angular",
            "axis": {"range": [0, max_axis]},
            "bgcolor": "rgba(0,0,0,0)",
            "bar": {"color": bar_color_rgba},
            "steps": steps,
            "borderwidth": 0,
        },
    ))
    if valor_label:
        fig.add_annotation(
            x=0.5, y=0.06, xref="paper", yref="paper",
            text=f"<span style='font-size:12px;color:#B0BEC5'>{valor_label}</span>",
            showarrow=False, align="center"
        )
    fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=230)
    return fig

# ======================= Cards de metas (HTML) =======================
def _card_periodo_html(titulo: str, ouro: float, prata: float, bronze: float, acumulado: float) -> str:
    def _linha(nivel, meta):
        falta = max(float(meta) - float(acumulado), 0.0)
        falta_txt = f"<span style='color:#00C853'>✅ {_fmt(0)}</span>" if falta <= 0 else _fmt(falta)
        return (
            f"<tr>"
            f"<td style='padding:8px 6px;color:#ECEFF1;font-weight:600;'>{nivel}</td>"
            f"<td style='padding:8px 6px;color:#B0BEC5;text-align:right;'>{_fmt(meta)}</td>"
            f"<td style='padding:8px 6px;color:#B0BEC5;text-align:right;'>{falta_txt}</td>"
            f"</tr>"
        )
    html = f"""
    <div style='border:1px solid #333; border-radius:12px; padding:12px; background-color:#121212;'>
      <div style='font-weight:700; color:#B0BEC5; margin-bottom:8px;'>{titulo}</div>
      <table style='width:100%; border-collapse:collapse;'>
        <thead>
          <tr>
            <th style='text-align:left; padding:6px; color:#90A4AE; font-weight:600;'>Nível</th>
            <th style='text-align:right; padding:6px; color:#90A4AE; font-weight:600;'>Meta</th>
            <th style='text-align:right; padding:6px; color:#90A4AE; font-weight:600;'>Falta</th>
          </tr>
        </thead>
        <tbody>
          {_linha("🥇 Ouro", ouro)}
          {_linha("🥈 Prata", prata)}
          {_linha("🥉 Bronze", bronze)}
        </tbody>
      </table>
    </div>
    """
    return html

# ======================= Normalização & Auto-load =======================
def _norm_df_entrada(df_entrada: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df_entrada, pd.DataFrame):
        return pd.DataFrame(columns=["Usuario", "UsuarioUpper", "Data", "Valor"])
    df = df_entrada.copy()
    if "Data" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["Data"]):
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
    df["Valor"] = pd.to_numeric(df.get("Valor", 0.0), errors="coerce").fillna(0.0)
    if "Usuario" in df.columns:
        df["Usuario"] = df["Usuario"].astype(str).fillna("LOJA")
        df["UsuarioUpper"] = df["Usuario"].str.upper()
    else:
        df["Usuario"] = "LOJA"; df["UsuarioUpper"] = "LOJA"
    return df

def _descobrir_perfil_usuario() -> Tuple[str, str]:
    perfil = (st.session_state.get("perfil_logado") or st.session_state.get("perfil") or st.session_state.get("role") or "Administrador")
    usuario = (st.session_state.get("usuario_logado") or st.session_state.get("usuario") or st.session_state.get("nome_usuario") or "")
    if isinstance(usuario, dict): usuario = usuario.get("nome", "")
    return str(perfil), str(usuario)

# ---- DB fallbacks (idêntico aos já enviados; mantido aqui para robustez) ----
_USER_COLS = ["Usuario","usuario","vendedor","responsavel","user","nome_usuario"]
_DATE_COLS = ["Data","data","data_venda","data_lanc","data_emissao","created_at","data_evento","data_pagamento"]
_VALU_COLS = ["Valor","valor","valor_total","valor_liquido","valor_bruto","Valor_Mercadoria","valor_evento","valor_pago","valor_a_pagar"]

def _discover_db_path() -> Optional[str]:
    cand = st.session_state.get("caminho_banco")
    if isinstance(cand,str) and os.path.exists(cand): return cand
    for p in (os.path.join("data","flowdash_data.db"), os.path.join("data","entrada.db"),
              os.path.join("data","dashboard_rc.db"), "dashboard_rc.db",
              os.path.join("data","flowdash_template.db")):
        if os.path.exists(p): return p
    return None

def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND LOWER(name)=LOWER(?) LIMIT 1;", (name,))
    return cur.fetchone() is not None

def _pick_cols(conn: sqlite3.Connection, table: str) -> Optional[Tuple[Optional[str], str, str]]:
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info('{table}')")]
    lower = {c.lower(): c for c in cols}
    def _first(cands: List[str]) -> Optional[str]:
        for c in cands:
            if c.lower() in lower: return lower[c.lower()]
        return None
    u = _first(_USER_COLS); d = _first(_DATE_COLS); v = _first(_VALU_COLS)
    if not d or not v: return None
    return (u, d, v)

def _load_df_entrada_from_db(db_path: str) -> pd.DataFrame:
    try: conn = sqlite3.connect(db_path)
    except Exception: return pd.DataFrame(columns=["Usuario","Data","Valor"])
    try:
        for tb in ["entradas","entrada","lancamentos_entrada","vendas","venda"]:
            if _table_exists(conn,tb):
                user_col, date_col, valu_col = _pick_cols(conn,tb) or (None,None,None)
                if not date_col or not valu_col: continue
                if user_col:
                    sql = f'SELECT "{user_col}" AS Usuario, "{date_col}" AS Data, "{valu_col}" AS Valor FROM "{tb}";'
                else:
                    sql = f'SELECT "LOJA" AS Usuario, "{date_col}" AS Data, "{valu_col}" AS Valor FROM "{tb}";'
                df = pd.read_sql(sql, conn)
                df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
                df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)
                df["Usuario"] = df["Usuario"].astype(str).fillna("LOJA")
                return df
        return pd.DataFrame(columns=["Usuario","Data","Valor"])
    finally: conn.close()

def _load_df_metas_from_db(db_path: str) -> pd.DataFrame:
    try: conn = sqlite3.connect(db_path)
    except Exception:
        return pd.DataFrame(columns=["vendedor","mensal","semanal","segunda","terca","quarta","quinta","sexta","sabado","domingo","meta_ouro","meta_prata","meta_bronze"])
    try:
        if not _table_exists(conn,"metas"):
            return pd.DataFrame(columns=["vendedor","mensal","semanal","segunda","terca","quarta","quinta","sexta","sabado","domingo","meta_ouro","meta_prata","meta_bronze"])
        raw = pd.read_sql("SELECT * FROM metas;", conn)
        if raw.empty:
            return pd.DataFrame(columns=["vendedor","mensal","semanal","segunda","terca","quarta","quinta","sexta","sabado","domingo","meta_ouro","meta_prata","meta_bronze"])
        df = raw.copy()
        def _num(col, default=0.0): 
            return pd.to_numeric(df[col], errors="coerce").fillna(default) if col in df.columns else pd.Series([default]*len(df))
        mensal   = _num("meta_mensal",0.0)
        perc_sem = _num("perc_semanal",25.0)
        p_seg,p_ter,p_qua,p_qui,p_sex,p_sab,p_dom = (_num("perc_segunda",0.0), _num("perc_terca",0.0), _num("perc_quarta",0.0),
                                                     _num("perc_quinta",0.0), _num("perc_sexta",0.0), _num("perc_sabado",0.0), _num("perc_domingo",0.0))
        p_bronze = _num("perc_bronze",75.0); p_prata = _num("perc_prata",87.5)
        if "vendedor" in df.columns:
            vendedor = df["vendedor"].astype(str).fillna("LOJA")
        else:
            vendedor = pd.Series(["LOJA"]*len(df))
            if "id_usuario" in df.columns and _table_exists(conn,"usuarios"):
                u = pd.read_sql("SELECT id, nome FROM usuarios;", conn)
                vendedor = df["id_usuario"].map(dict(zip(u["id"],u["nome"]))).fillna("LOJA").astype(str)
        semanal_abs = mensal * (perc_sem/100.0)
        segunda,terca,quarta,quinta,sexta,sabado,domingo = [semanal_abs*(p/100.0) for p in (p_seg,p_ter,p_qua,p_qui,p_sex,p_sab,p_dom)]
        meta_bronze = mensal*(p_bronze/100.0); meta_prata = mensal*(p_prata/100.0); meta_ouro = mensal*1.0
        out = pd.DataFrame({
            "vendedor": vendedor.astype(str),
            "mensal": mensal, "semanal": semanal_abs,
            "segunda": segunda, "terca": terca, "quarta": quarta, "quinta": quinta, "sexta": sexta, "sabado": sabado, "domingo": domingo,
            "meta_ouro": meta_ouro, "meta_prata": meta_prata, "meta_bronze": meta_bronze,
        })
        for c in ["mensal","semanal","segunda","terca","quarta","quinta","sexta","sabado","domingo","meta_ouro","meta_prata","meta_bronze"]:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)
        out["vendedor"] = out["vendedor"].astype(str).fillna("LOJA")
        return out
    finally: conn.close()

def _auto_carregar_dfs() -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    df_e, df_m = st.session_state.get("df_entrada"), st.session_state.get("df_metas")
    if isinstance(df_e,pd.DataFrame) and isinstance(df_m,pd.DataFrame): return df_e, df_m
    try:
        from flowdash_pages.dataframes import dataframes as _dfmod  # type: ignore
        if hasattr(_dfmod,"carregar_df_entrada") and hasattr(_dfmod,"carregar_df_metas"):
            df_e, df_m = _dfmod.carregar_df_entrada(), _dfmod.carregar_df_metas()
            if isinstance(df_e,pd.DataFrame) and isinstance(df_m,pd.DataFrame):
                st.session_state["df_entrada"], st.session_state["df_metas"] = df_e, df_m
                return df_e, df_m
    except Exception: pass
    db = _discover_db_path()
    if db:
        df_e, df_m = _load_df_entrada_from_db(db), _load_df_metas_from_db(db)
        st.session_state["df_entrada"], st.session_state["df_metas"] = df_e, df_m
        return df_e, df_m
    return None, None

# ------ metas por vendedor/loja ------
def _extrair_metas_completo(df_metas: pd.DataFrame, vendedor_upper: str, coluna_dia: str) -> Tuple[float, float, float, float, float, float]:
    metas_v = pd.DataFrame()
    if isinstance(df_metas,pd.DataFrame) and not df_metas.empty:
        col_v = df_metas.get("vendedor")
        if col_v is not None: metas_v = df_metas[col_v.astype(str).str.upper() == vendedor_upper]
    def _get(col: str, default: float=0.0) -> float:
        if not metas_v.empty and col in metas_v.columns:
            try: return float(metas_v[col].max())
            except Exception: return default
        return default
    return (
        _get(coluna_dia,0.0), _get("semanal",0.0), _get("mensal",0.0),
        _get("meta_ouro",16000.0), _get("meta_prata",14000.0), _get("meta_bronze",12000.0)
    )

# =============================== Página ===============================
def page_metas(df_entrada: Optional[pd.DataFrame], df_metas: Optional[pd.DataFrame], perfil_logado: str, usuario_logado: str):
    if not isinstance(df_entrada,pd.DataFrame) or not isinstance(df_metas,pd.DataFrame):
        df_e2, df_m2 = _auto_carregar_dfs(); perfil2, usuario2 = _descobrir_perfil_usuario()
        if isinstance(df_e2,pd.DataFrame) and isinstance(df_m2,pd.DataFrame): return page_metas(df_e2, df_m2, perfil2, usuario2)
        st.error("Não encontrei os DataFrames de entrada/metas automaticamente."); return

    st.markdown("### 🎯 Metas — Velocímetros (Dia / Semana / Mês)")
    df_e = _norm_df_entrada(df_entrada)

    available_dates = df_e["Data"].dt.date.dropna()
    if available_dates.empty: st.info("Sem dados de vendas para exibir."); return

    # Seletor de data
    min_d, max_d = available_dates.min(), max(available_dates.max(), date.today())
    default_ref = date.today() if (available_dates == date.today()).any() else available_dates.max()
    ref_day = st.date_input("📅 Data de referência", value=default_ref, min_value=min_d, max_value=max_d, format="YYYY/MM/DD", key="metas_ref_date")
    st.markdown(f"**📆 Metas do dia — {ref_day:%Y-%m-%d}**")

    inicio_sem, inicio_mes = _inicio_semana(ref_day), ref_day.replace(day=1)
    coluna_dia, mes_key = _coluna_dia(ref_day), f"{inicio_mes:%Y%m}"

    # ------------- helpers de período -------------
    def _ratios(meta_ouro: float, meta_prata: float, meta_bronze: float) -> Tuple[float,float]:
        if meta_ouro and meta_ouro > 0:
            return (100.0 * (meta_prata/meta_ouro), 100.0 * (meta_bronze/meta_ouro))
        # fallback
        return (87.5, 75.0)

    def _cards_periodo(ouro_m: float, prata_m: float, bronze_m: float,
                       meta_dia: float, meta_sem: float,
                       val_d: float, val_s: float, val_m: float):
        # ratios vindos do mês (preserva seu desenho da prata/bronze)
        prata_pct, bronze_pct = _ratios(ouro_m, prata_m, bronze_m)
        # metas escalares para dia/semana
        prata_d, bronze_d = meta_dia * (prata_pct/100.0), meta_dia * (bronze_pct/100.0)
        prata_s, bronze_s = meta_sem * (prata_pct/100.0), meta_sem * (bronze_pct/100.0)

        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown(_card_periodo_html("📅 Dia", ouro=meta_dia, prata=prata_d, bronze=bronze_d, acumulado=val_d), unsafe_allow_html=True)
        with c2:
            st.markdown(_card_periodo_html("🗓️ Semana", ouro=meta_sem, prata=prata_s, bronze=bronze_s, acumulado=val_s), unsafe_allow_html=True)
        with c3:
            st.markdown(_card_periodo_html("📆 Mês", ouro=ouro_m, prata=prata_m, bronze=bronze_m, acumulado=val_m), unsafe_allow_html=True)

    # ---------------- LOJA ----------------
    mask_dia = (df_e["Data"].dt.date == ref_day)
    mask_sem = (df_e["Data"].dt.date >= inicio_sem) & (df_e["Data"].dt.date <= ref_day)
    mask_mes = (df_e["Data"].dt.date >= inicio_mes) & (df_e["Data"].dt.date <= ref_day)
    valor_dia_loja = df_e.loc[mask_dia, "Valor"].sum()
    valor_sem_loja = df_e.loc[mask_sem, "Valor"].sum()
    valor_mes_loja = df_e.loc[mask_mes, "Valor"].sum()

    m_dia, m_sem, m_mes, ouro_l, prata_l, bronz_l = _extrair_metas_completo(df_metas, "LOJA", coluna_dia)
    perc_dia_loja = _calcular_percentual(valor_dia_loja, m_dia)
    perc_sem_loja = _calcular_percentual(valor_sem_loja, m_sem)
    perc_mes_loja = _calcular_percentual(valor_mes_loja, m_mes)
    bronze_pct_l = 75.0 if ouro_l <= 0 else round(100.0 * (bronz_l / ouro_l), 1)
    prata_pct_l  = 87.5 if ouro_l <= 0 else round(100.0 * (prata_l / ouro_l), 1)

    st.markdown(f"<h5 style='margin: 5px 0;'>🏪 LOJA</h5>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    c1.plotly_chart(_gauge_percentual_zonas("Meta do Dia", perc_dia_loja, bronze_pct_l, prata_pct_l, valor_label=_fmt(valor_dia_loja)),
                    use_container_width=True, key=f"gauge_loja_dia_{mes_key}_{ref_day}")
    c2.plotly_chart(_gauge_percentual_zonas("Meta da Semana", perc_sem_loja, bronze_pct_l, prata_pct_l, valor_label=_fmt(valor_sem_loja)),
                    use_container_width=True, key=f"gauge_loja_sem_{mes_key}_{ref_day}")
    c3.plotly_chart(_gauge_percentual_zonas("Meta do Mês", perc_mes_loja, bronze_pct_l, prata_pct_l, valor_label=_fmt(valor_mes_loja)),
                    use_container_width=True, key=f"gauge_loja_mes_{mes_key}_{ref_day}")

    _cards_periodo(ouro_l, prata_l, bronz_l, m_dia, m_sem, valor_dia_loja, valor_sem_loja, valor_mes_loja)

    # ------------- VENDEDORES -------------
    st.markdown("#### 👥 Vendedores")
    vendedores = sorted([u for u in df_e["Usuario"].dropna().unique() if str(u).strip() and str(u).upper() != "LOJA"])
    if str(perfil_logado).strip().lower() == "vendedor":
        vendedores = [v for v in vendedores if v.upper() == str(usuario_logado).upper()]
    if not vendedores:
        st.info("Nenhum vendedor encontrado para exibir."); return

    for vendedor in vendedores:
        nome_upper = str(vendedor).upper()
        if nome_upper == "NONE": continue
        df_u = df_e[df_e["Usuario"].astype(str).str.upper() == nome_upper]

        mask_d = (df_u["Data"].dt.date == ref_day)
        mask_s = (df_u["Data"].dt.date >= inicio_sem) & (df_u["Data"].dt.date <= ref_day)
        mask_m = (df_u["Data"].dt.date >= inicio_mes) & (df_u["Data"].dt.date <= ref_day)
        valor_dia = df_u.loc[mask_d, "Valor"].sum()
        valor_sem = df_u.loc[mask_s, "Valor"].sum()
        valor_mes = df_u.loc[mask_m, "Valor"].sum()

        m_dia, m_sem, m_mes, ouro, prata, bronz = _extrair_metas_completo(df_metas, nome_upper, coluna_dia)
        perc_dia = _calcular_percentual(valor_dia, m_dia)
        perc_sem = _calcular_percentual(valor_sem, m_sem)
        perc_mes = _calcular_percentual(valor_mes, m_mes)
        bronze_pct = 75.0 if ouro <= 0 else round(100.0 * (bronz / ouro), 1)
        prata_pct  = 87.5 if ouro <= 0 else round(100.0 * (prata / ouro), 1)

        slug = _slug_key(vendedor)
        st.markdown(f"<h5 style='margin: 5px 0 -25px;'>👤 {vendedor}</h5>", unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        c1.plotly_chart(_gauge_percentual_zonas("Meta do Dia", perc_dia, bronze_pct, prata_pct, valor_label=_fmt(valor_dia)),
                        use_container_width=True, key=f"gauge_{slug}_dia_{mes_key}_{ref_day}")
        c2.plotly_chart(_gauge_percentual_zonas("Meta da Semana", perc_sem, bronze_pct, prata_pct, valor_label=_fmt(valor_sem)),
                        use_container_width=True, key=f"gauge_{slug}_sem_{mes_key}_{ref_day}")
        c3.plotly_chart(_gauge_percentual_zonas("Meta do Mês", perc_mes, bronze_pct, prata_pct, valor_label=_fmt(valor_mes)),
                        use_container_width=True, key=f"gauge_{slug}_mes_{mes_key}_{ref_day}")

        _cards_periodo(ouro, prata, bronz, m_dia, m_sem, valor_dia, valor_sem, valor_mes)

# ============================ Entrypoints ============================
def render_metas_auto():
    df_e, df_m = _auto_carregar_dfs(); perfil, usuario = _descobrir_perfil_usuario()
    if not isinstance(df_e,pd.DataFrame) or not isinstance(df_m,pd.DataFrame):
        st.error("Não encontrei os DataFrames de entrada/metas automaticamente."); return
    page_metas(df_e, df_m, perfil, usuario)

def render(*_args, **_kwargs): render_metas_auto()
def render_metas(*_args, **_kwargs): render_metas_auto()
