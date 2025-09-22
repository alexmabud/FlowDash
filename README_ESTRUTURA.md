
# 📁 Estrutura de Pastas — FlowDash

Este arquivo descreve a estrutura organizacional do projeto **FlowDash**, com explicações sobre o propósito de cada pasta e arquivo, conforme a estrutura modular real utilizada.

---

## 🌳 Estrutura Atual

```
FlowDash/
├── assets/
│   └── Fluxograma FlowDash.png
├── auth/
│   ├── auth.py
│   └── __init__.py
├── banco/
│   ├── banco.py
│   └── __init__.py
├── data/
│   ├── flowdash_data.db
│   └── flowdash_template.db
├── flowdash_pages/
│   ├── __init__.py
│   ├── cadastros/
│   │   ├── cadastro_categorias.py
│   │   ├── cadastro_classes.py
│   │   ├── pagina_bancos_cadastrados.py
│   │   ├── pagina_caixa.py
│   │   ├── pagina_cartoes.py
│   │   ├── pagina_correcao_caixa.py
│   │   ├── pagina_emprestimos.py
│   │   ├── pagina_maquinetas.py
│   │   ├── pagina_metas.py
│   │   ├── pagina_saldos_bancarios.py
│   │   └── __init__.py
│   ├── dashboard/
│   │   ├── dashboard.py
│   │   └── __init__.py
│   ├── dataframes/
│   │   ├── dataframes.py
│   │   ├── entradas.py
│   │   ├── filtros.py
│   │   ├── saidas.py
│   │   └── __init__.py
│   ├── dre/
│   │   ├── dre.py
│   │   └── __init__.py
│   ├── fechamento/
│   │   ├── fechamento.py
│   │   └── __init__.py
│   └── lancamentos/
│       ├── shared_ui.py
│       ├── __init__.py
│       ├── caixa2/
│       │   ├── actions_caixa2.py
│       │   ├── page_caixa2.py
│       │   ├── state_caixa2.py
│       │   └── ui_forms_caixa2.py
│       ├── deposito/
│       │   ├── actions_deposito.py
│       │   ├── page_deposito.py
│       │   ├── state_deposito.py
│       │   └── ui_forms_deposito.py
│       ├── mercadorias/
│       │   ├── actions_mercadorias.py
│       │   ├── page_mercadorias.py
│       │   ├── state_mercadorias.py
│       │   └── ui_forms_mercadorias.py
│       ├── pagina/
│       │   ├── actions_pagina.py
│       │   ├── page_lancamentos.py
│       │   ├── state_pagina.py
│       │   └── ui_cards_pagina.py
│       ├── saida/
│       │   ├── actions_saida.py
│       │   ├── page_saida.py
│       │   ├── state_saida.py
│       │   └── ui_forms_saida.py
│       └── transferencia/
│           ├── actions_transferencia.py
│           ├── page_transferencia.py
│           ├── state_transferencia.py
│           └── ui_forms_transferencia.py
├── repository/
│   ├── bancos_cadastrados_repository.py
│   ├── cartoes_repository.py
│   ├── categorias_repository.py
│   ├── emprestimos_financiamentos_repository.py
│   ├── movimentacoes_repository.py
│   ├── taxas_maquinas_repository.py
│   └── contas_a_pagar_mov_repository/
│       ├── adjustments.py
│       ├── base.py
│       ├── events.py
│       ├── loans.py
│       ├── payments.py
│       ├── queries.py
│       └── types.py
├── scripts/
│   ├── generate_dropbox_refresh_token.py
│   └── sync_template_from_live.py
├── services/
│   ├── taxas.py
│   ├── vendas.py
│   └── ledger/
│       ├── service_ledger.py
│       ├── service_ledger_autobaixa.py
│       ├── service_ledger_boleto.py
│       ├── service_ledger_cap_helpers.py
│       ├── service_ledger_credito.py
│       ├── service_ledger_emprestimo.py
│       ├── service_ledger_fatura.py
│       ├── service_ledger_infra.py
│       └── service_ledger_saida.py
├── shared/
│   ├── db.py
│   ├── db_from_dropbox_api.py
│   ├── dbx_io.py
│   ├── dropbox_client.py
│   ├── dropbox_config.py
│   ├── ids.py
│   └── __init__.py
├── streamlit/
│   └── secrets.toml
├── utils/
│   ├── pin_utils.py
│   ├── utils.py
│   └── __init__.py
├── .gitattributes
├── .gitignore
├── README.md
├── README_ESTRUTURA.md
├── main.py
├── manage_inits.py
├── nova_tabela.py
├── pdv_app.py
├── requirements.txt
└── __init__.py
```

---

## 🗂️ Detalhamento das Pastas e Arquivos

| Caminho                                         | Descrição                                                                 |
|-------------------------------------------------|---------------------------------------------------------------------------|
| `main.py`                                       | Ponto de entrada principal. Define a estrutura de navegação do app.       |
| `pdv_app.py`                                    | App PDV/Kiosk (login normal + PIN na venda) para operação rápida.         |
| `assets/Fluxograma FlowDash.png`                | Diagrama do fluxo da aplicação (referência visual).                       |
| `auth/auth.py`                                  | Lógica de login, controle de sessão, perfis e acesso por usuário.         |
| `banco/banco.py`                                | Utilitários legados de banco (camada oficial em `shared/db.py`).          |
| `shared/db.py`                                  | Conexão com SQLite + helpers de leitura/escrita usados pelo app.          |
| `shared/db_from_dropbox_api.py`                 | Download do banco via Dropbox API (HTTP) com `access_token`.              |
| `shared/dbx_io.py`                              | Integração Dropbox SDK com refresh token (download/upload confiável).     |
| `shared/dropbox_client.py`                      | Cliente unificado que orquestra API/SDK do Dropbox.                       |
| `shared/dropbox_config.py`                      | Leitura de `secrets.toml`/env e flags (DEBUG/OFFLINE).                    |
| `shared/ids.py`                                 | Geradores/validadores de IDs/UIDs de transações e registros.              |
| `flowdash_pages/dashboard/dashboard.py`         | KPIs e gráficos do painel.                                                |
| `flowdash_pages/fechamento/fechamento.py`       | Fechamento de caixa: saldos e entradas confirmadas.                       |
| `flowdash_pages/metas/metas.py`                 | Metas LOJA e por vendedor (Bronze/Prata/Ouro).                            |
| `flowdash_pages/dre/dre.py`                     | Estrutura da DRE (demonstração de resultados).                            |
| `flowdash_pages/dataframes/dataframes.py`       | Base de DataFrames e utilidades.                                          |
| `flowdash_pages/dataframes/entradas.py`         | Agregações e consultas de Entradas.                                       |
| `flowdash_pages/dataframes/saidas.py`           | Agregações e consultas de Saídas.                                         |
| `flowdash_pages/dataframes/filtros.py`          | Filtros e transformações auxiliares.                                      |
| `flowdash_pages/lancamentos/shared_ui.py`       | Componentes visuais compartilhados dos Lançamentos.                       |
| `flowdash_pages/lancamentos/pagina/*`           | Página “Lançamentos” (estado, ações e cartões).                           |
| `flowdash_pages/lancamentos/caixa2/*`           | Fluxo Caixa 2 (actions, state, forms, página).                            |
| `flowdash_pages/lancamentos/deposito/*`         | Depósitos (actions, state, forms, página).                                |
| `flowdash_pages/lancamentos/transferencia/*`    | Transferências (actions, state, forms, página).                           |
| `flowdash_pages/lancamentos/mercadorias/*`      | Mercadorias (pedido/NF, custos, frete, previsões).                        |
| `flowdash_pages/lancamentos/saida/*`            | Saídas (contas, pagamentos, categorização).                               |
| `flowdash_pages/lancamentos/venda/*`            | Vendas (forms, state e ações de venda).                                   |
| `repository/*.py`                               | Repositórios por domínio (bancos, cartões, categorias, mov., taxas).      |
| `repository/contas_a_pagar_mov_repository/*`    | CAP especializado (base, events, loans, payments, queries, types).        |
| `services/taxas.py`                             | Regras de taxas (bandeira/forma/parcelas).                                |
| `services/vendas.py`                            | Regras de vendas.                                                         |
| `services/ledger/*`                             | Ledger por fluxo (saída, fatura, boleto, crédito, empréstimo, etc.).      |
| `utils/utils.py`                                | Funções auxiliares: formatação, datas, helpers gerais.                    |
| `utils/pin_utils.py`                            | Utilitários de PIN para o PDV.                                            |
| `streamlit/secrets.toml`                        | Credenciais/config do Streamlit (NÃO versionar).                          |
| `data/flowdash_template.db`                     | Template de banco (versionado).                                           |
| `data/flowdash_data.db`                         | Banco “vivo” local (ignorado pelo Git).                                   |
| `README.md`                                     | Apresentação geral do projeto, funcionalidades, instalação.               |
| `README_ESTRUTURA.md`                           | Detalhamento técnico da estrutura de arquivos e pastas (este arquivo).    |

---

## 💡 Observações

- A estrutura foi planejada para:
  - Organização modular por responsabilidade
  - Facilidade de manutenção e expansão
  - Reutilização de partes do sistema em outros contextos
  - Migração futura para frameworks como Django, Flask ou interfaces desktop

- Todas as funções e lógicas estão agrupadas por tema:
  banco, autenticação, interface, utilidades e regras de negócio.

- Banco de dados:
  - `data/flowdash_data.db` é o banco “vivo” (ignorado no Git).
  - `data/flowdash_template.db` é o template versionado.
  - Registrar mudanças de schema (changelog curto) quando alterar tabelas/índices.

- Credenciais e segurança:
  - Nunca versionar tokens/segredos.
  - Usar `streamlit/secrets.toml` ou variáveis de ambiente.
  - Flags úteis: `FLOWDASH_DEBUG=1`, `DROPBOX_DISABLE=1`.

- Sincronização/backup (Dropbox):
  - Preferir SDK com refresh token (`shared/dbx_io.py`).
  - Manter 1 backup diário e 3 versões anteriores.

- Padrões de código:
  - Python 3.12+, commits curtos (`feat:`, `fix:`, `docs:`).
  - snake_case (funções/variáveis), PascalCase (classes).

---

**Autor:** Alex Abud  
**Projeto:** FlowDash – Sistema de Fluxo de Caixa + Dashboard Inteligente
