# 🚀 Guia de Deploy - CRM Vendas Atacado

## 📋 Pré-requisitos
- Conta no GitHub
- Conta no Railway (railway.app)

## 🔧 Preparação Local

### 1. Limpar arquivos desnecessários
Remova os seguintes arquivos antes do deploy:
- `database.db` (antigo)
- `database_novo.db` (temporário)
- `database_old.db` (backup)
- `test_produtos.py`
- `reimport_data.py`
- `analise_produtos.py` (versão antiga)
- Arquivos `*.pyc` e pasta `__pycache__`

### 2. Manter apenas:
✅ Arquivos essenciais:
- `app.py` - Aplicação principal
- `db_manager.py` - Gerenciador de banco
- `analise_clientes.py` - Análise de clientes
- `analise_produtos_v2.py` - Análise de produtos
- `startup.py` - Script de inicialização
- `database_final.db` - Banco de dados correto
- `requirements.txt` - Dependências
- `railway.json` - Configuração Railway
- `Procfile` - Configuração de deploy
- `.gitignore` - Ignorar arquivos
- `.streamlit/config.toml` - Configuração Streamlit

## 📦 Deploy no Railway

### Passo 1: Preparar repositório GitHub

```bash
# Inicializar git se necessário
git init

# Adicionar arquivos
git add .

# Commit inicial
git commit -m "Deploy CRM Vendas Atacado"

# Adicionar repositório remoto
git remote add origin https://github.com/SEU_USUARIO/crm-vendas-atacado.git

# Push para GitHub
git push -u origin main
```

### Passo 2: Deploy no Railway

1. Acesse [railway.app](https://railway.app)
2. Clique em "New Project"
3. Selecione "Deploy from GitHub repo"
4. Conecte sua conta GitHub
5. Selecione o repositório `crm-vendas-atacado`
6. Railway detectará automaticamente as configurações

### Passo 3: Configurar variáveis (opcional)

No painel do Railway, adicione se necessário:
- Nenhuma variável específica é necessária
- O sistema detecta automaticamente a porta

### Passo 4: Aguardar deploy

- Railway construirá e iniciará automaticamente
- Aguarde a mensagem "Deployment live"
- Clique no domínio gerado para acessar

## 🔍 Verificação

Após o deploy, verifique:
1. ✅ Dashboard Principal carrega
2. ✅ Análise de Clientes funciona
3. ✅ Análise de Produtos mostra todos os 591 produtos
4. ✅ Follow-up e Relatórios funcionam
5. ✅ Upload de novos dados funciona

## 🛠️ Solução de Problemas

### Erro: "No module named..."
- Verifique se `requirements.txt` está completo
- Railway usa Python 3.11 por padrão

### Erro: "Database not found"
- O `startup.py` criará automaticamente o banco
- Certifique-se que `database_final.db` está no repositório

### Erro: "Port binding"
- Railway configura a porta automaticamente via $PORT
- Não defina porta fixa no código

## 📊 Dados Iniciais

O sistema já vem com:
- **270 clientes** cadastrados
- **591 produtos** com análises
- **6.158 vendas** processadas
- Período: Set/2024 a Ago/2025

## 🔄 Atualizar Dados

Para atualizar dados em produção:
1. Acesse a aba "⚙️ Atualizar Dados"
2. Faça upload do novo CSV
3. Confirme a importação

## 📱 Acesso Mobile

O sistema é responsivo e funciona em:
- 📱 Smartphones
- 📱 Tablets
- 💻 Desktops

## 🔐 Segurança

Recomendações:
- Configure autenticação se necessário
- Use HTTPS (Railway fornece automaticamente)
- Faça backups regulares do banco

## 📧 Suporte

Em caso de dúvidas:
- Verifique os logs no Railway
- Console do Railway mostra erros em tempo real

## ✅ Checklist Final

Antes do deploy, confirme:
- [ ] Banco de dados `database_final.db` presente
- [ ] Arquivos desnecessários removidos
- [ ] `requirements.txt` atualizado
- [ ] `.gitignore` configurado
- [ ] Código testado localmente

## 🎉 Pronto!

Seu CRM está pronto para deploy. Após seguir estes passos, você terá um sistema completo de análise de vendas online e acessível de qualquer lugar!

---
*Versão 1.0 - Sistema CRM Vendas Atacado*