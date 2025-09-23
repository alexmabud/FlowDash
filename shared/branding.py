# shared/branding.py
# -*- coding: utf-8 -*-
"""
FlowDash — Branding Utilities (centralização garantida)
"""

from __future__ import annotations
import base64, os
from functools import lru_cache
from typing import Optional
import streamlit as st

# Candidatos de logo (mantém "assets" e variações comuns por segurança)
_DEFAULT_LOGO_CANDIDATES = [
    "assets/flowdash1.PNG","assets/flowdash2.PNG",
    "assets/flowdash1.png","assets/flowdash2.png",
    "assents/flowdash1.PNG","assents/flowdash2.PNG",   # tolerância a erro de digitação
    "assents/flowdash1.png","assents/flowdash2.png",
    "assets/logo_flowdash.png","assets/FlowDash_logo.png",
    "assets/logo.png","assets/flowdash.png","assets/FlowDash.png",
]

def _file_exists(p: str) -> bool:
    try:
        return os.path.exists(p) and os.path.isfile(p)
    except Exception:
        return False

@lru_cache(maxsize=1)
def resolve_logo_path(custom_path: Optional[str] = None) -> Optional[str]:
    if custom_path and _file_exists(custom_path):
        return custom_path
    for p in _DEFAULT_LOGO_CANDIDATES:
        if _file_exists(p):
            return p
    return None

@lru_cache(maxsize=1)
def _logo_as_base64(custom_path: Optional[str] = None) -> Optional[str]:
    path = resolve_logo_path(custom_path)
    if not path:
        return None
    try:
        with open(path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")
        ext = os.path.splitext(path)[1].lower()
        mime = "image/png"
        if ext in (".jpg", ".jpeg"): mime = "image/jpeg"
        elif ext == ".svg": mime = "image/svg+xml"
        return f"data:{mime};base64,{b64}"
    except Exception:
        return None

def _inject_css_once():
    key = "_branding_css_injected_v2"
    if st.session_state.get(key):
        return
    st.session_state[key] = True

    css = """
    <style>
    .block-container{padding-top:6px!important;}

    /* Header do app (centralização do conteúdo é inline no HTML) */
    .fdx-header{
        position: sticky; top: 0; z-index: 999;
        backdrop-filter: blur(6px); -webkit-backdrop-filter: blur(6px);
        background: rgba(255,255,255,0.75);
        border-bottom: 1px solid rgba(0,0,0,0.08);
        padding: 0.30rem 0.60rem; margin: -6px 0 0.30rem 0;
    }
    [data-theme="dark"] .fdx-header{
        background: rgba(13,17,23,0.75);
        border-bottom-color: rgba(255,255,255,0.08);
    }
    .fdx-title{font-weight:700; font-size:1.06rem; line-height:1.2; margin:0;}
    .fdx-subtitle{font-weight:400; font-size:0.86rem; opacity:.85; margin:0;}

    /* Sidebar: padding superior; centralização é via columns */
    section[data-testid="stSidebar"] .block-container{padding-top:6px!important;}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

def sidebar_brand(
    caption: Optional[str] = None,
    custom_path: Optional[str] = None,
    compact: bool = True,
    height_px: Optional[int] = 40,
):
    """
    Exibe a logo no sidebar (centralizada via layout de colunas).
    """
    _inject_css_once()
    path = resolve_logo_path(custom_path)

    cols = st.sidebar.columns([1, 4, 1])
    with cols[1]:
        if path:
            data_uri = _logo_as_base64(custom_path)
            if data_uri:
                h = f"height:{int(height_px)}px;" if (height_px and height_px > 0) else ""
                st.markdown(
                    f'<img src="{data_uri}" alt="logo" '
                    f'style="display:block;{h}width:auto;border-radius:6px;margin:0 auto;" />',
                    unsafe_allow_html=True,
                )
            else:
                st.image(path, use_container_width=False)
        else:
            # fallback sem imagem
            st.markdown('<h3 style="text-align:center;margin:0;">FlowDash</h3>', unsafe_allow_html=True)

        if caption:
            st.caption(caption)

def page_header(
    title: str = "FlowDash",
    subtitle: Optional[str] = None,
    custom_path: Optional[str] = None,
    show: bool = True,
    logo_height_px: int = 30,
    show_title: bool = False,           # <-- por padrão NÃO mostra o texto
):
    """Header fixo com centralização garantida via inline-style."""
    if not show:
        return
    _inject_css_once()
    data_uri = _logo_as_base64(custom_path)
    img_html = ""
    if data_uri:
        img_html = (
            f'<img src="{data_uri}" alt="logo" '
            f'style="display:block;height:{int(logo_height_px)}px;width:auto;border-radius:6px;margin:0 auto;" />'
        )
    title_html = f'<div class="fdx-title">{title}</div>' if show_title else ""
    subtitle_html = f'<div class="fdx-subtitle">{subtitle}</div>' if (subtitle and show_title) else ""

    html = f"""
    <div class="fdx-header"
         style="display:flex!important;justify-content:center!important;align-items:center!important;text-align:center!important;">
      <div style="display:flex!important;flex-direction:column!important;align-items:center!important;justify-content:center!important;gap:6px!important;max-width:100%!important;">
        {img_html}
        {title_html}
        {subtitle_html}
      </div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

def login_brand(
    title: str = "FlowDash",
    custom_path: Optional[str] = None,
    caption: Optional[str] = None,
    height_px: int = 150,               # tamanho padrão mais discreto
    show_title: bool = False,           # <-- por padrão NÃO mostra o texto
):
    """
    Banner para o topo da tela de login (centralizado e com altura fixa).
    """
    _inject_css_once()
    path = resolve_logo_path(custom_path)
    cols = st.columns([1, 4, 1])
    with cols[1]:
        if path:
            data_uri = _logo_as_base64(custom_path)
            if data_uri:
                st.markdown(
                    f'<img src="{data_uri}" alt="logo" '
                    f'style="display:block;height:{int(height_px)}px;width:auto;border-radius:6px;margin:0 auto;" />',
                    unsafe_allow_html=True,
                )
            else:
                st.image(path, width=height_px)
        if show_title:
            st.markdown(f"### {title}")
            if caption:
                st.caption(caption)
