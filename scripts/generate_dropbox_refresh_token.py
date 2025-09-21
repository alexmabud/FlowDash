# scripts/generate_dropbox_refresh_token.py
# -*- coding: utf-8 -*-
"""
Gera um refresh_token do Dropbox via OAuth (fluxo sem redirecionamento).

Uso:
    python scripts/generate_dropbox_refresh_token.py

Pré-requisito:
    - Crie um app em https://www.dropbox.com/developers/apps
    - Copie o APP KEY e APP SECRET do app (aba Settings).
    - Na aba Permissions, marque:
        files.content.read, files.content.write, account_info.read
"""
import os
import sys
from getpass import getpass

try:
    from dropbox import DropboxOAuth2FlowNoRedirect, Dropbox
except Exception:
    print(">> Faltando dependência 'dropbox'. Rode: pip install dropbox==11.36.2")
    sys.exit(1)

def ask(msg: str, default: str = "") -> str:
    v = input(msg).strip()
    return v or default

def ask_bool(msg: str, default_yes: bool = True) -> bool:
    default = "S" if default_yes else "n"
    v = input(f"{msg} [{'S/n' if default_yes else 's/N'}] ").strip().lower()
    if not v:
        return default_yes
    return v in ("s", "sim", "y", "yes")

def main():
    print("=== Dropbox OAuth — gerar refresh_token ===")
    app_key = os.getenv("DROPBOX_APP_KEY") or ask("APP KEY: ")
    app_secret = os.getenv("DROPBOX_APP_SECRET") or getpass("APP SECRET (oculto): ")

    if not app_key or not app_secret:
        print("APP KEY e APP SECRET são obrigatórios.")
        sys.exit(1)

    # Pergunta só para imprimir o file_path correto
    full_dropbox = ask_bool("Seu app é Full Dropbox?", default_yes=True)
    file_path = "/FlowDash/data/flowdash_data.db" if full_dropbox else "/data/flowdash_data.db"

    scopes = ["files.content.read", "files.content.write", "account_info.read"]
    flow = DropboxOAuth2FlowNoRedirect(
        consumer_key=app_key,
        consumer_secret=app_secret,
        token_access_type="offline",   # pede refresh_token
        scope=scopes,
        use_pkce=False,                # com secret, pode ser False
    )

    authorize_url = flow.start()
    print("\n1) Abra esta URL no navegador, faça login e permita o app:")
    print(authorize_url)
    print("\n2) Ao final, o Dropbox mostrará um CÓDIGO.")
    auth_code = ask("Cole aqui o CÓDIGO: ")

    try:
        result = flow.finish(auth_code)
    except Exception as e:
        print(f"\nERRO ao finalizar OAuth: {e}")
        print("Verifique se os scopes foram salvos na aba Permissions do app.")
        sys.exit(2)

    # Valida as credenciais criando um cliente com refresh_token
    try:
        dbx = Dropbox(
            oauth2_refresh_token=result.refresh_token,
            app_key=app_key,
            app_secret=app_secret,
            timeout=30,
        )
        acct = dbx.users_get_current_account()
        print(f"\nOK! Autenticado como: {acct.name.display_name}")
    except Exception as e:
        print(f"\nAviso: não foi possível validar com users_get_current_account: {e}")
        print("Isso pode ser rede/firewall; o refresh_token ainda foi emitido.")

    # Mostra o bloco pronto para colar no secrets.toml
    print("\n=== Cole este bloco no arquivo .streamlit/secrets.toml ===\n")
    print("[dropbox]")
    print(f'app_key = "{app_key}"')
    print(f'app_secret = "{app_secret}"')
    print(f'refresh_token = "{result.refresh_token}"')
    print(f'file_path = "{file_path}"')
    print('disable = "0"')
    print('debug = "0"')
    print('access_token = ""   # deixe vazio para forçar uso do refresh')
    print('force_download = "0"')

if __name__ == "__main__":
    main()
