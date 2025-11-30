# 💼 FlowDash

![Status](https://img.shields.io/badge/Status-v1.0.0--rc1-blue) ![Python](https://img.shields.io/badge/Python-3.12%2B-yellow) ![Streamlit](https://img.shields.io/badge/Streamlit-App-red)

> **Status:** v1.0.0-rc1 (Release Candidate 1).
> **Foco:** Integridade de Caixa, Segurança Operacional, Dashboard Inteligente e Sincronização em Nuvem.

Sistema completo de **Fluxo de Caixa + Dashboard + PDV** para varejo físico, desenvolvido em **Python + Streamlit + SQLite**, com suporte a **Dropbox** para sincronização via refresh token.

O FlowDash foi criado para ser um **auditor financeiro em tempo real**, garantindo:
- Integridade do caixa
- Previsibilidade do faturamento
- Segurança operacional
- Indicadores avançados e DRE automatizada
- Registro confiável de toda a operação diária

---

# 🧠 Objetivo e Diferenciais do Sistema

O FlowDash resolve problemas reais do varejo físico:

- Registro rápido de **Entradas**, **Saídas**, **Vendas**, **Depósitos**, **Sangrias**, **Transferências**, **Caixa 2**, **Mercadorias**
- Controle total de **Caixa** e **Caixa 2**
- Taxas de maquininha aplicadas automaticamente
- Fechamento diário com travas antifraude
- Dashboard Executivo com **KPIs avançados**
- Motor de previsão com **Facebook Prophet**
- DRE automatizada (CMV, EBITDA, Lucro Líquido, Dívida, Ativos)
- Metas da loja e comissões automáticas por nível (Bronze, Prata, Ouro)
- Sincronização com Dropbox via refresh token
- PDV rápido com login via PIN

---

# 🛡️ Integridade Financeira (Travas de Segurança)

O FlowDash possui mecanismos que **não existem em planilhas**:

### 🔒 1. Protocolo de Fechamento Sequencial
Se o usuário tentar lançar algo **com dias anteriores em aberto**, o sistema:
- **Bloqueia qualquer lançamento**, até que os dias pendentes sejam fechados.
- Evita furos de caixa e inconsistências.
- Obriga o operador a consolidar corretamente.

### 🔒 2. Imutabilidade de Caixa Fechado
Datas com fechamento tornam-se:
- **Somente leitura**.
- Não é possível editar nem adicionar lançamentos retroativos.
- Evita fraudes, alterações indevidas e erros históricos.

### 🔒 3. Segurança Interna via `lock_manager.py`
O módulo garante:
- Integridade transacional.
- Bloqueios condicionais.
- Proteção contra duplicidade e inconsistência.
- Auditoria financeira completa.

---

# 🗂️ Estrutura de Pastas (atualizada)

| Pasta / Arquivo                                   | Descrição                                                                 |
|---------------------------------------------------|---------------------------------------------------------------------------|
| `main.py`                                         | App principal (admin): dashboard, lançamentos, metas, fechamento, DRE.    |
| `pdv_app.py`                                      | Aplicação PDV/Kiosk (venda rápida usando PIN).                            |
| `auth/`                                           | Login, perfis, controle de sessão.                                        |
| `flowdash_pages/`                                 | Todas as páginas do app (Dashboard, DRE, Fechamento, Metas, etc.).        |
| ├── `dashboard/dashboard.py`                      | KPIs, gráficos, comparações históricas e previsões.                       |
| ├── `dashboard/prophet_engine.py`                 | Motor de previsão usando Facebook Prophet.                                |
| ├── `fechamento/fechamento.py`                    | Fechamento diário de caixa.                                               |
| ├── `metas/metas.py`                              | Metas da loja e metas por vendedor.                                       |
| ├── `dre/dre.py`                                  | Demonstração de Resultados com cálculos completos.                        |
| ├── `dataframes/dataframes.py`                    | Base unificada de DataFrames (entradas/saídas/mercadorias).               |
| └── `lancamentos/*`                               | Lançamentos completos (entradas, saídas, caixa2, depósito, mercadorias).  |
| `services/`                                       | Regras de negócio e ledger (saídas, fatura, boletos, crédito, empréstimo).|
| `repository/`                                     | Repositórios de dados (bancos, categorias, CAP, empréstimos etc).         |
| `shared/`                                         | Infra geral (SQLite, Dropbox API/SDK, config, IDS).                       |
| `utils/utils.py`                                  | Funções auxiliares: formatação, datas e helpers gerais.                   |
| `scripts/generate_dropbox_refresh_token.py`       | Script para gerar refresh token do Dropbox.                               |
| `tools/`                                          | Utilidades auxiliares do projeto.                                         |
| `data/flowdash_template.db`                       | Template limpo do banco OFICIAL (versionado).                             |
| `data/flowdash_data.db`                           | Banco ativo local (não versionado).                                       |
| `streamlit/secrets.toml`                          | Segredos: chaves Dropbox, flags, configs (NÃO versionar).                 |
| `README.md`                                       | Este arquivo.                                                             |
| `README_ESTRUTURA.md`                             | Detalhamento técnico da estrutura.                                        |

---

# ✅ Funcionalidades Detalhadas

## 🔐 1. Login e Perfis
- Perfis: **Administrador**, **Gerente**, **Vendedor**
- Controle de acesso granular
- Usuários Ativo/Inativo
- Senhas com **SHA-256**

---

## 💰 2. Lançamentos Financeiros
Módulo completo para Entradas, Saídas, Vendas, Mercadorias, Depósitos, Caixa 2, Transferências e Aporte financeiro.

Com:
- Estados independentes
- Validação dinâmica
- Ledger integrado
- IDs únicos via `ids.py`

---

## 📦 3. Mercadorias
- Registro de pedidos, fornecedor, coleção
- Controle de NF
- Previsão de faturamento vs Recebimento
- Integração total com Dashboard e DRE
- Tabela dinâmica com filtros (ano/mês)

---

## 💳 4. Taxas de Maquininha Inteligentes
Cadastradas por Forma, Bandeira e Parcelas.
Aplicação automática em:
- Fechamento de Caixa
- Entradas confirmadas
- Valor líquido calculado com precisão

---

## 🧾 5. Fechamento de Caixa
- Entradas brutas e líquidas
- Cálculo automático de taxas
- Saldos (Banco 1, 2, 3, 4, Caixa, Caixa 2)
- Correções manuais
- Depósitos confirmados
- Auditoria de diferenças
- **Trava antifraude de dias em aberto**

---

## 🎯 6. Metas e Comissões
- Metas Bronze / Prata / Ouro
- Comissões automáticas (1% / 1.5% / 2%)
- Ranking por vendedor
- KPIs e acompanhamento em tempo real

---

# 📊 7. Dashboard Inteligente com IA (Prophet)

O dashboard centraliza os indicadores essenciais do negócio:

### 🔹 Indicadores Disponíveis
- Vendas por dia, mês e ano
- Ticket médio mensal/anual
- Nº de vendas mensal/anual
- Saldo disponível (bancos + caixa + caixa 2)
- Reposição vs CMV
- Lucro Líquido e Operacional
- Balanço mensal (entradas × saídas × resultado)
- Ranking de melhores meses
- Heatmap anual de faturamento
- Crescimento m/m e comparações anuais
- ROE, ROI, ROA

### 🔮 Previsão de Faturamento (Machine Learning)
Motor de previsão baseado em **Facebook Prophet**, implementado em `flowdash_pages/dashboard/prophet_engine.py`.

O modelo usa histórico de vendas, sazonalidade e tendências para entregar:
- Projeção de faturamento futuro
- Estimativa do orçamento mensal
- Tendências de alta/baixa
- Gráfico com intervalo de confiança

---

# 📘 8. DRE – Demonstrativo de Resultados

Implementado em `dre.py`. Calcula automaticamente:
- Receita Bruta e Líquida
- CMV (mercadorias + frete proporcional)
- Lucro Bruto e Margem Bruta
- Margem de Contribuição
- EBITDA e EBIT
- Lucro Líquido
- Ativos Totais e Endividamento

---

## 📝 Banco de Dados

- **Template** (sem dados): `data/flowdash_template.db` (já no repositório)
- **Banco ativo (local)**: `data/flowdash_data.db` (ignorado pelo Git)

**Credenciais padrão do template (primeiro acesso):**
- Usuário: `admin@local`
- Senha: `admin`

> **Rodar local sem comandos:**  
> Se você já tem um banco com seus dados, **coloque o arquivo na pasta `data/` com o nome exato `flowdash_data.db`**.  
> Se não tiver, **copie/renomeie** o template para esse nome. Pronto — nada de scripts.

---

## 🚀 Como Executar (Local)

1. **Garanta o banco ativo**: tenha `data/flowdash_data.db` (veja a nota acima).
2. **Instale as dependências**:

```bash
pip install -r requirements.txt
```

3. **Inicie o app**:

```bash
streamlit run main.py
```

Abra o navegador em `http://localhost:8501`.

---

## ☁️ Execução Online com Dropbox (refresh token)

O FlowDash pode buscar/enviar o banco automaticamente no Dropbox usando refresh token (SDK).
Arquivos envolvidos: `shared/dbx_io.py`, `shared/dropbox_client.py`, `shared/dropbox_config.py`.

### 1) Criar um app no Dropbox
- Tipo: **Scoped Access**
- Permissão: **App folder** (recomendado)
- Anote **App key** e **App secret**.

### 2) Obter o refresh token
Você pode usar o script do repositório:

```bash
python scripts/generate_dropbox_refresh_token.py
```

Siga o fluxo do navegador (OAuth) e copie o **refresh_token** exibido.

### 3) Configurar `streamlit/secrets.toml`
Crie/edite `streamlit/secrets.toml` (NÃO versionar) com:

```toml
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
```

> **Como funciona:**
> - Na inicialização, o app tenta **baixar o banco** do caminho `file_path` para `data/flowdash_data.db`.
> - Ao salvar, pode **enviar** de volta (conforme a lógica/uso).
> - Em caso de erro ou `disable="1"`, o app usa **somente o banco local**.

> **Importante:** nunca coloque essas chaves em commits.
> Para produção, rotacione tokens periodicamente.

---

## 🛠️ Tecnologias

- **Python 3.12+**
- **Streamlit**
- **SQLite3**
- **Pandas**
- **Plotly / Matplotlib**
- **Workalendar**
- **Dropbox SDK** (opcional, para sincronização em nuvem)
- Todas listadas em `requirements.txt`.

---

## 🔐 Segurança

- Senhas com **hash SHA-256**
- Controle de acesso por **perfil de usuário**
- Banco protegido por `.gitignore`
- Segredos isolados em `streamlit/secrets.toml`
- Travas antifraude de fechamento e auditoria de dias em aberto

---

## 👨‍💻 Autor

**Alex Abud**
**Projeto:** FlowDash — Sistema de Fluxo de Caixa + Dashboard Inteligente.