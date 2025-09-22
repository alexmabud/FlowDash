# 💼 FlowDash

Sistema de Fluxo de Caixa e Dashboard para varejo, em **Python + Streamlit + SQLite**.

---

## 🧠 Objetivo

- Controle de **entradas** e **saídas**
- **Metas** e **comissões** por perfil
- **Fechamento de caixa** diário
- **Dashboard** com indicadores
- **Cadastros**: usuários, taxas, cartões, bancos, metas etc.

---

## 🗂️ Estrutura de Pastas

| Pasta / Arquivo             | Descrição                                                                 |
|-----------------------------|---------------------------------------------------------------------------|
| `main.py`                   | Ponto de entrada principal (app Streamlit).                               |
| `pdv_app.py`                | App PDV/Kiosk (login normal + PIN na venda).                              |
| `auth/`                     | Login, sessão, perfis e verificação de acesso.                            |
| `flowdash_pages/`           | Páginas do app (Dashboard, Lançamentos, Fechamento, Metas, DRE, etc.).    |
| `services/`                 | Regras de negócio e **ledger** (saída/fatura/boleto/crédito/emprestimo…). |
| `repository/`               | Acesso a dados por domínio (bancos, cartões, categorias, CAP…).           |
| `shared/`                   | Infra (SQLite, Dropbox SDK/API, config, IDs).                             |
| `utils/`                    | Helpers (formatação, datas, PIN).                                         |
| `data/flowdash_template.db` | **Template** de banco (sem dados, versionado).                            |
| `data/flowdash_data.db`     | **Banco ativo** local (ignorado no Git).                                  |
| `streamlit/secrets.toml`    | Segredos/variáveis (NÃO versionar).                                       |

---

## ✅ Funcionalidades

- **Login com perfis**: Administrador, Gerente, Vendedor  
- **Lançamentos do dia**: Entradas, Saídas, Transferências, Depósitos, Caixa 2, Mercadorias, Vendas  
- **Cadastro**: Usuários (ativo/inativo), taxas por forma/bandeira/parcelas, cartões, saldos, metas  
- **Fechamento de Caixa**: entradas confirmadas (com taxas), saldos, correções  
- **Dashboard** e **DRE**: em evolução contínua

---

## 🔐 Segurança

- Senhas com **hash SHA-256**
- Controle de acesso por **perfil**
- **Segredos** em `streamlit/secrets.toml` (nunca versionar)

---

## 🛠️ Tecnologias

Python 3.12+, Streamlit, SQLite3, Pandas, Plotly/Matplotlib, Workalendar.

---

## 📝 Banco de Dados

- **Template** (sem dados): `data/flowdash_template.db` (já no repositório)
- **Banco ativo (local)**: `data/flowdash_data.db` (ignorado no Git)

**Credenciais padrão do template (primeiro acesso):**
- Usuário: `admin@local`
- Senha: `admin`

> **Rodar local sem comandos:**  
> Se você já tem um banco com seus dados, **coloque o arquivo na pasta `data/` com o nome exato `flowdash_data.db`**.  
> Se não tiver, **copie/renomeie** o template para esse nome. Pronto — nada de scripts.

---

## 🚀 Como Executar (Local)

1. **Garanta o banco ativo**: tenha `data/flowdash_data.db` (veja a nota acima).
2. **Instale dependências**:

    pip install -r requirements.txt

3. **Inicie o app**:

    streamlit run main.py

Abra o navegador em `http://localhost:8501`.

---

## ☁️ Execução Online com Dropbox (refresh token)

O FlowDash pode buscar/enviar o banco automaticamente no Dropbox usando **refresh token** (SDK).  
Arquivos envolvidos: `shared/dbx_io.py`, `shared/dropbox_client.py`, `shared/dropbox_config.py`.

### 1) Criar um app no Dropbox
- Tipo: **Scoped Access**  
- Permissão: **App folder** (recomendado)  
- Anote **App key** e **App secret**.

### 2) Obter o **refresh token**
Você pode usar o script do repositório:

    python scripts/generate_dropbox_refresh_token.py

Siga o fluxo do navegador (OAuth) e copie o **refresh_token** exibido.

### 3) Configurar `streamlit/secrets.toml`
Crie/edite `streamlit/secrets.toml` (NÃO versionar) com:

    [dropbox]
    # Credenciais do app Dropbox (SDK)
    app_key       = "SEU_APP_KEY"
    app_secret    = "SEU_APP_SECRET"
    refresh_token = "SEU_REFRESH_TOKEN"

    # Caminho do arquivo no Dropbox (dentro da pasta do app)
    file_path     = "/FlowDash/data/flowdash_data.db"

    # Flags úteis
    force_download = "0"   # "1" força baixar sempre que iniciar
    disable = "0"          # "1" desativa Dropbox e usa somente o banco local
    debug = "0"            # "1" para logs extras

> **Como funciona:**  
> - Na inicialização, o app tenta **baixar o banco** do caminho `file_path` para `data/flowdash_data.db`.  
> - Ao salvar, pode **enviar** de volta (conforme a lógica/uso).  
> - Em caso de erro ou `disable="1"`, o app usa **somente o banco local**.

> **Importante:** nunca coloque essas chaves em commits.  
> Para produção, rotacione tokens periodicamente.

---

## 📦 Dependências (principais)

- `streamlit` — interface do app  
- `pandas`, `plotly`, `matplotlib` — dados e gráficos  
- `workalendar` — dias úteis/feriados  
- `dropbox` — SDK para sincronizar o banco (opcional)

Tudo listado em `requirements.txt`.

---

## 👨‍💻 Autor

**Alex Abud**  
**Projeto:** FlowDash — Sistema de Fluxo de Caixa + Dashboard Inteligente
