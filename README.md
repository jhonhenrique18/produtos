# CRM de Vendas - Atacado

Sistema completo de análise de clientes e produtos para vendas no atacado.

## Funcionalidades

### 📊 Análise de Clientes
- Segmentação automática (VIP, Fiel, Em Risco, Inativo, etc.)
- Análise completa individual de cada cliente
- Histórico detalhado de TODOS os produtos comprados
- Identificação de oportunidades de cross-sell
- Script de abordagem personalizado
- Previsão de próxima compra

### 📦 Análise de Produtos  
- Lista completa dos 591 produtos
- Análise detalhada de cada produto
- Clientes que compraram cada produto
- Produtos complementares (comprados juntos)
- Análise de sazonalidade
- Classificação ABC

### 🎯 Ações de Follow-up
- Lista de clientes em risco
- Clientes para reativação
- Oportunidades de cross-sell identificadas
- Scripts de abordagem prontos

## Instalação Local

```bash
# Clonar o repositório
git clone [seu-repo]

# Instalar dependências
pip install -r requirements.txt

# Rodar aplicação
streamlit run app.py
```

## Deploy no Railway

1. Faça push do código para o GitHub
2. Conecte o repositório no Railway
3. O deploy será automático

## Estrutura do Banco

- **SQLite** para portabilidade e simplicidade
- Tabelas principais:
  - `vendas` - Dados originais
  - `clientes_metricas` - Métricas agregadas de clientes
  - `produtos_metricas` - Métricas agregadas de produtos
  - `cliente_produtos` - Relação cliente x produtos

## Atualização de Dados

1. Acesse a aba "⚙️ Atualizar Dados"
2. Faça upload do novo CSV
3. Confirme a importação

## Tecnologias

- Python 3.11+
- Streamlit
- SQLite
- Pandas
- Plotly