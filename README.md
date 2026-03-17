# FlowDash

![Status](https://img.shields.io/badge/Status-v1.0.0--rc1-blue) ![Python](https://img.shields.io/badge/Python-3.12%2B-yellow) ![Streamlit](https://img.shields.io/badge/Streamlit-App-red)

> **Status:** v1.0.0-rc1 (Release Candidate 1).
> **Foco:** Integridade de Caixa, Segurança Operacional, Dashboard Inteligente e Sincronização em Nuvem.

Sistema completo de **Fluxo de Caixa + Dashboard + PDV** para varejo físico, desenvolvido em **Python + Streamlit + SQLite**, com sincronização automática via **Dropbox SDK** (refresh token).

O FlowDash foi criado para ser um **auditor financeiro em tempo real**, garantindo:
- Integridade do caixa com travas antifraude
- Previsibilidade do faturamento via machine learning
- Segurança operacional e auditoria completa
- Indicadores avançados e DRE automatizada
- Registro confiável de toda a operação diária

---

## Duas Aplicações

| App | Arquivo | Descrição |
|-----|---------|-----------|
| **Admin** | `main.py` | App principal — dashboard, lançamentos, fechamento, DRE, metas, cadastros. |
| **PDV** | `pdv_app.py` | Kiosk de venda rápida — login com email/senha + autenticação por PIN por vendedor. |

```bash
# App Admin
streamlit run main.py

# App PDV (porta separada)
streamlit run pdv_app.py
```

---

## Objetivo e Diferenciais

O FlowDash resolve problemas reais do varejo físico:

- Registro rápido de **Vendas**, **Saídas**, **Depósitos**, **Transferências**, **Caixa 2** e **Mercadorias**
- Controle total de **Caixa** e **Caixa 2**
- Taxas de maquininha aplicadas automaticamente por forma/bandeira/parcelas
- Fechamento diário com travas antifraude
- Dashboard Executivo com **KPIs avançados**
- Motor de previsão com **Facebook Prophet**
- DRE automatizada (CMV, EBITDA, Lucro Líquido, Dívida, Ativos)
- Metas da loja e comissões automáticas por nível (Bronze, Prata, Ouro)
- Sincronização bidirecional com Dropbox (auto pull/push)
- PDV com login via PIN por vendedor

---

## Travas de Segurança (Integridade Financeira)

O FlowDash possui mecanismos que **não existem em planilhas**:

### 1. Protocolo de Fechamento Sequencial
Se o usuário tentar lançar algo **com dias anteriores em aberto**, o sistema:
- **Bloqueia qualquer lançamento** até que os dias pendentes sejam fechados.
- Evita furos de caixa e inconsistências históricas.
- Obriga o operador a consolidar corretamente.

### 2. Imutabilidade de Caixa Fechado
Datas com fechamento tornam-se:
- **Somente leitura** — não é possível editar nem adicionar lançamentos retroativos.
- Evita fraudes, alterações indevidas e erros históricos.

### 3. Segurança Interna via `lock_manager.py`
O módulo garante:
- Integridade transacional.
- Bloqueios condicionais.
- Proteção contra duplicidade e inconsistência.
- Auditoria financeira completa.

---

## Estrutura de Pastas

```
FlowDash/
├── main.py                          # App Admin (Streamlit)
├── pdv_app.py                       # App PDV/Kiosk (Streamlit)
├── banco/
│   └── banco.py                     # Loader genérico com whitelist de tabelas
├── auth/
│   └── auth.py                      # Login, validação, perfis, sessão
├── flowdash_pages/
│   ├── dashboard/
│   │   ├── dashboard.py             # KPIs, gráficos, histórico, previsões
│   │   └── prophet_engine.py        # Motor de previsão (Facebook Prophet)
│   ├── lancamentos/
│   │   ├── pagina/                  # Página unificada de lançamentos
│   │   ├── venda/                   # Lançamento de venda
│   │   ├── saida/                   # Lançamento de saída
│   │   ├── deposito/                # Lançamento de depósito
│   │   ├── transferencia/           # Transferência entre contas
│   │   ├── caixa2/                  # Lançamento Caixa 2
│   │   ├── mercadorias/             # Pedidos de mercadoria / NF
│   │   └── shared_ui.py             # UI compartilhada entre módulos
│   ├── dataframes/
│   │   ├── dataframes.py            # Entradas, Saídas, Mercadorias, Faturas, CAP, Empréstimos
│   │   ├── livro_caixa.py           # Livro Caixa consolidado
│   │   ├── entradas.py              # View de entradas
│   │   ├── saidas.py                # View de saídas
│   │   ├── mercadorias.py           # View de mercadorias
│   │   ├── faturas_cartao.py        # Fatura de cartão de crédito
│   │   ├── contas_a_pagar.py        # Contas a pagar
│   │   ├── emprestimos.py           # Empréstimos / financiamentos
│   │   └── filtros.py               # Helpers de filtros
│   ├── fechamento/
│   │   ├── fechamento.py            # Fechamento diário de caixa
│   │   └── lock_manager.py          # Travas antifraude e bloqueios
│   ├── metas/
│   │   └── metas.py                 # Metas da loja e por vendedor
│   ├── dre/
│   │   └── dre.py                   # Demonstração de Resultados (DRE)
│   ├── cadastros/
│   │   ├── pagina_usuarios.py       # Gestão de usuários
│   │   ├── pagina_metas.py          # Cadastro de metas
│   │   ├── pagina_maquinetas.py     # Taxas de maquinetas
│   │   ├── pagina_cartoes.py        # Cartões de crédito
│   │   ├── pagina_caixa.py          # Configuração de caixa
│   │   ├── pagina_correcao_caixa.py # Correção manual de caixa
│   │   ├── pagina_saldos_bancarios.py # Saldos bancários iniciais
│   │   ├── pagina_emprestimos.py    # Cadastro de empréstimos
│   │   ├── pagina_bancos_cadastrados.py # Bancos cadastrados
│   │   ├── cadastro_categorias.py   # Categorias de saída
│   │   ├── cadastro_classes.py      # Classes de saída
│   │   └── variaveis_dre.py         # Variáveis configuráveis do DRE
│   ├── finance_logic.py             # Lógica financeira compartilhada
│   └── utils_timezone.py            # Utilitários de timezone
├── services/
│   ├── ledger/
│   │   ├── service_ledger.py        # Ledger principal
│   │   ├── service_ledger_saida.py  # Ledger de saídas
│   │   ├── service_ledger_fatura.py # Ledger de faturas
│   │   ├── service_ledger_boleto.py # Ledger de boletos
│   │   ├── service_ledger_credito.py # Ledger de crédito
│   │   ├── service_ledger_emprestimo.py # Ledger de empréstimos
│   │   ├── service_ledger_autobaixa.py  # Baixa automática
│   │   ├── service_ledger_cap_helpers.py # Helpers de CAP
│   │   └── service_ledger_infra.py  # Infraestrutura do ledger
│   ├── taxas.py                     # Cálculo de taxas de maquininha
│   └── vendas.py                    # Regras de negócio de vendas
├── repository/
│   ├── bancos_cadastrados_repository.py
│   ├── cartoes_repository.py
│   ├── categorias_repository.py
│   ├── emprestimos_financiamentos_repository.py
│   ├── movimentacoes_repository.py
│   ├── taxas_maquinas_repository.py
│   └── contas_a_pagar_mov_repository/
│       ├── base.py / queries.py / types.py
│       ├── adjustments.py / events.py / loans.py / payments.py
├── shared/
│   ├── db.py                        # get_conn() — conexão centralizada SQLite
│   ├── dbx_io.py                    # Pull/push do banco via Dropbox SDK
│   ├── dropbox_client.py            # Cliente Dropbox (refresh token)
│   ├── dropbox_config.py            # Carregamento de config Dropbox
│   ├── db_from_dropbox_api.py       # Fallback legado (HTTP access token)
│   ├── branding.py                  # Logos e identidade visual
│   ├── ids.py                       # Geração de IDs únicos
│   └── safe_session.py              # Helpers de session_state seguro
├── utils/
│   ├── utils.py                     # Formatação, datas, helpers gerais
│   ├── pin_utils.py                 # Validação de PIN (PDV)
│   └── column_discovery.py          # Descoberta dinâmica de colunas
├── scripts/
│   ├── generate_dropbox_refresh_token.py  # Gera refresh token OAuth
│   └── sync_template_from_live.py         # Sincroniza template com banco ativo
├── tools/
│   └── install_safe_snapshots.py    # Utilitários de snapshot do banco
├── data/
│   ├── flowdash_template.db         # Banco template limpo (versionado)
│   └── flowdash_data.db             # Banco ativo local (não versionado)
├── assets/                          # Logos e imagens
├── streamlit/
│   └── secrets.toml                 # Credenciais Dropbox (NÃO versionar)
└── requirements.txt
```

---

## Padrão dos Módulos de Lançamento

Cada tipo de lançamento segue uma arquitetura consistente de 4 arquivos:

| Arquivo | Responsabilidade |
|---------|-----------------|
| `page_*.py` | Orquestra a página, chama estado e UI |
| `state_*.py` | Estado do formulário (session_state) |
| `actions_*.py` | Ações de persistência no banco |
| `ui_forms_*.py` | Formulários e componentes visuais |

---

## Funcionalidades

### Login e Perfis
- Perfis: **Administrador**, **Gerente**, **Vendedor**
- Controle de acesso granular por página
- Usuários Ativo/Inativo
- Hash de senhas com SHA-256

### Lançamentos Financeiros
Módulo completo para: Vendas, Saídas, Mercadorias, Depósitos, Caixa 2 e Transferências.
- Estados independentes por módulo
- Validação dinâmica
- Ledger integrado (faturas, boletos, crédito, empréstimos)
- IDs únicos via `shared/ids.py`

### Mercadorias
- Registro de pedidos, fornecedor, coleção
- Controle de NF
- Previsão de faturamento vs Recebimento
- Integração total com Dashboard e DRE
- Tabela dinâmica com filtros (ano/mês)

### Taxas de Maquininha
Cadastradas por Forma, Bandeira e Parcelas.
Aplicadas automaticamente em:
- Fechamento de Caixa
- Entradas confirmadas
- Valor líquido calculado com precisão

### Fechamento de Caixa
- Entradas brutas e líquidas (com taxas)
- Saldos: Banco 1, 2, 3, 4, Caixa, Caixa 2
- Correções manuais
- Depósitos confirmados
- Auditoria de diferenças
- Trava antifraude de dias em aberto

### Metas e Comissões
- Metas Bronze / Prata / Ouro por mês/semana/dia
- Comissões automáticas (1% / 1.5% / 2%)
- Ranking por vendedor
- Gauges visuais de progresso (disponíveis no PDV)

### DataFrames (Visões de Dados)
Acesso tabelado com filtros dinâmicos para:
- Livro Caixa consolidado
- Entradas e Saídas
- Mercadorias
- Fatura de Cartão de Crédito
- Contas a Pagar
- Empréstimos / Financiamentos

---

## Dashboard Inteligente com IA (Prophet)

### Indicadores Disponíveis
- Vendas por dia, mês e ano
- Ticket médio mensal/anual
- Nº de vendas mensal/anual
- Saldo disponível (bancos + caixa + caixa 2)
- Reposição vs CMV
- Lucro Líquido e Operacional
- Balanço mensal (entradas x saídas x resultado)
- Ranking de melhores meses
- Heatmap anual de faturamento
- Crescimento m/m e comparações anuais
- ROE, ROI, ROA

### Previsão de Faturamento (Machine Learning)
Motor de previsão baseado em **Facebook Prophet** em `flowdash_pages/dashboard/prophet_engine.py`.

Usa histórico de vendas, sazonalidade e tendências para:
- Projeção de faturamento futuro
- Estimativa do orçamento mensal
- Tendências de alta/baixa
- Gráfico com intervalo de confiança

---

## DRE — Demonstrativo de Resultados

Calcula automaticamente:
- Receita Bruta e Líquida
- CMV (mercadorias + frete proporcional)
- Lucro Bruto e Margem Bruta
- Margem de Contribuição
- EBITDA e EBIT
- Lucro Líquido
- Ativos Totais e Endividamento

---

## PDV (Ponto de Venda)

O `pdv_app.py` é uma interface Kiosk otimizada para venda no balcão:
- Login com email/senha (usuário administrador ou gerente)
- Seleção de vendedor + autenticação por **PIN de 4 dígitos**
- Formulário de venda rápida
- Gauges de Metas da Loja em tempo real (dia / semana / mês)
- Sincronização Dropbox com throttle de 45s
- Sidebar oculta (modo kiosk)
- Proteção contra força-bruta (máx. 5 tentativas de PIN)

---

## Banco de Dados

**Tabelas principais:**

| Tabela | Descrição |
|--------|-----------|
| `entrada` | Vendas e entradas financeiras |
| `saida` | Saídas e despesas |
| `mercadorias` | Pedidos de mercadoria |
| `fechamento_caixa` | Fechamentos diários |
| `metas` | Metas da loja e por vendedor |
| `usuarios` | Usuários e perfis |
| `taxas_maquinas` | Taxas de maquinetas |
| `cartoes_credito` | Cartões cadastrados |
| `fatura_cartao` | Faturas de cartão |
| `contas_a_pagar` | Contas a pagar |
| `emprestimos_financiamentos` | Empréstimos e financiamentos |
| `saldos_bancos` | Saldos bancários |
| `saldos_caixas` | Saldos de caixa |
| `movimentacoes` | Ledger de movimentações |
| `variaveis_dre` | Variáveis configuráveis do DRE |
| `correcao_caixa` | Correções manuais de caixa |

- **Template** (sem dados): `data/flowdash_template.db` (versionado)
- **Banco ativo (local)**: `data/flowdash_data.db` (ignorado pelo Git)

**Credenciais padrão do template:**
- Usuário: `admin@local`
- Senha: `admin`

> Se você já tem um banco com seus dados, coloque-o em `data/flowdash_data.db`.
> Se não tiver, copie/renomeie o template para esse nome. Nenhum script necessário.

---

## Como Executar (Local)

1. **Garanta o banco ativo** — tenha `data/flowdash_data.db` (veja acima).
2. **Instale as dependências**:

```bash
pip install -r requirements.txt
```

3. **Inicie o app Admin**:

```bash
streamlit run main.py
```

Acesse em `http://localhost:8501`.

4. **(Opcional) App PDV em porta separada:**

```bash
streamlit run pdv_app.py --server.port 8502
```

---

## Sincronização com Dropbox (Refresh Token)

O FlowDash sincroniza o banco automaticamente com o Dropbox usando SDK com refresh token.

Estratégia de sync:
- **Auto pull** com throttle de 60s (main) / 45s (PDV) — baixa se remoto for mais novo
- **Auto push** imediato — envia se mtime local mudou
- **Fallback:** modo local se Dropbox desabilitado ou sem credenciais

### 1) Criar app no Dropbox
- Tipo: **Scoped Access**
- Permissão: **App folder** (recomendado)
- Anote **App key** e **App secret**.

### 2) Obter o refresh token

```bash
python scripts/generate_dropbox_refresh_token.py
```

Siga o fluxo OAuth e copie o `refresh_token` exibido.

### 3) Configurar `streamlit/secrets.toml`

```toml
[dropbox]
app_key       = "SEU_APP_KEY"
app_secret    = "SEU_APP_SECRET"
refresh_token = "SEU_REFRESH_TOKEN"
file_path     = "/FlowDash/data/flowdash_data.db"
force_download = "0"   # "1" força download sempre ao iniciar
disable = "0"          # "1" desativa Dropbox (modo offline)
debug = "0"            # "1" ativa logs de diagnóstico
```

> Nunca versione `secrets.toml`. Para produção, rotacione tokens periodicamente.

---

## Tecnologias

| Lib | Uso |
|-----|-----|
| **Python 3.12+** | Linguagem principal |
| **Streamlit >= 1.36** | Interface web |
| **SQLite3** | Banco de dados local |
| **Pandas >= 2.2** | Manipulação de dados |
| **Prophet >= 1.1.5** | Previsão de faturamento (ML) |
| **NumPy < 2.0.0** | Compatibilidade com Prophet |
| **Plotly >= 5.22** | Gráficos interativos |
| **Matplotlib >= 3.8** | Gráficos auxiliares |
| **Dropbox SDK** | Sincronização em nuvem |
| **bcrypt >= 4.0.0** | Hash de senhas |
| **workalendar >= 17.0** | Calendário de dias úteis |
| **python-bcb >= 0.2.0** | Dados do Banco Central |
| **sidrapy == 0.1.4** | Dados IBGE/SIDRA |
| **requests >= 2.32** | HTTP (legado Dropbox) |

---

## Segurança

- Hash de senhas (SHA-256 + bcrypt)
- Controle de acesso por perfil de usuário
- Banco de dados protegido por `.gitignore`
- Segredos isolados em `streamlit/secrets.toml`
- Whitelist de tabelas no loader genérico (`banco/banco.py`)
- Travas antifraude de fechamento e auditoria de dias em aberto
- Limite de tentativas de PIN no PDV (máx. 5)

---

## Autor

**Alex Abud** — FlowDash: Sistema de Fluxo de Caixa + Dashboard Inteligente para Varejo Físico.
