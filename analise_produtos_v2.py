"""
Sistema de análise detalhada de produtos - Versão corrigida
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from db_manager_v2 import DatabaseManager

class AnalisadorProdutos:
    def __init__(self, db_manager):
        self.db = db_manager
    
    def get_todos_produtos_analise(self):
        """Retorna análise de todos os produtos com tratamento de erros"""
        try:
            conn = self.db.connect()
            
            # Verificar qual tabela usar
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='produtos_metricas_v2'")
            use_v2 = cursor.fetchone()[0] > 0
            
            if use_v2:
                # Usar tabela v2 com códigos
                query = """
                SELECT 
                    cod_produto,
                    produto,
                    quantidade_vendida,
                    valor_total,
                    qtd_vendas as total_vendas,
                    clientes_unicos,
                    ticket_medio,
                    taxa_recompra,
                    margem_media,
                    primeira_venda,
                    ultima_venda,
                    dias_desde_ultima
                FROM produtos_metricas_v2
                ORDER BY valor_total DESC
                """
            else:
                # Query original
                query = """
                SELECT 
                    produto,
                    SUM(quantidade) as quantidade_vendida,
                    SUM(total) as valor_total,
                    COUNT(*) as total_vendas,
                    COUNT(DISTINCT parceiro) as clientes_unicos,
                    AVG(total) as ticket_medio,
                    MIN(data) as primeira_venda,
                    MAX(data) as ultima_venda
                FROM vendas
                WHERE produto IS NOT NULL AND produto != ''
                GROUP BY produto
                ORDER BY valor_total DESC
                """
            
            produtos_df = pd.read_sql(query, conn)
            
            if produtos_df.empty:
                return pd.DataFrame()
            
            # Adicionar dias desde última venda
            produtos_df['ultima_venda'] = pd.to_datetime(produtos_df['ultima_venda'])
            produtos_df['dias_desde_ultima'] = (datetime.now() - produtos_df['ultima_venda']).dt.days
            
            # Categorizar produtos
            produtos_df['categoria'] = produtos_df['produto'].apply(self.categorizar_produto)
            
            # Classificação ABC
            produtos_df = produtos_df.sort_values('valor_total', ascending=False)
            produtos_df['valor_acumulado'] = produtos_df['valor_total'].cumsum()
            produtos_df['pct_acumulado'] = produtos_df['valor_acumulado'] / produtos_df['valor_total'].sum() * 100
            
            produtos_df['classificacao_abc'] = produtos_df['pct_acumulado'].apply(
                lambda x: 'A' if x <= 70 else ('B' if x <= 90 else 'C')
            )
            
            # Se não estamos usando v2, calcular taxa_recompra e margem_media
            if not use_v2:
                # Calcular taxa de recompra
                taxa_recompra = []
                for produto in produtos_df['produto']:
                    query_recompra = """
                    SELECT COUNT(DISTINCT parceiro) as total_clientes,
                           SUM(CASE WHEN compras > 1 THEN 1 ELSE 0 END) as clientes_recorrentes
                    FROM (
                        SELECT parceiro, COUNT(*) as compras
                        FROM vendas
                        WHERE produto = ?
                        GROUP BY parceiro
                    ) t
                    """
                    result = pd.read_sql(query_recompra, conn, params=[produto])
                    if result['total_clientes'][0] > 0:
                        taxa = (result['clientes_recorrentes'][0] / result['total_clientes'][0]) * 100
                    else:
                        taxa = 0
                    taxa_recompra.append(taxa)
                
                produtos_df['taxa_recompra'] = taxa_recompra
                
                # Calcular margem média
                margem_query = """
                SELECT 
                    produto,
                    AVG(CASE 
                        WHEN preco_base > 0 AND preco_final > 0 
                        THEN ((preco_final - preco_base) / preco_base * 100)
                        ELSE 0 
                    END) as margem_media
                FROM vendas
                WHERE produto IS NOT NULL
                GROUP BY produto
                """
                margem_df = pd.read_sql(margem_query, conn)
                
                # Merge com margem
                produtos_df = produtos_df.merge(margem_df, on='produto', how='left')
                produtos_df['margem_media'] = produtos_df['margem_media'].fillna(0)
            else:
                # V2 já tem essas colunas
                if 'taxa_recompra' not in produtos_df.columns:
                    produtos_df['taxa_recompra'] = 0
                if 'margem_media' not in produtos_df.columns:
                    produtos_df['margem_media'] = 0
            
            # Score de performance
            if len(produtos_df) > 0:
                produtos_df['score_performance'] = (
                    (produtos_df['valor_total'] / produtos_df['valor_total'].max() * 40) +
                    (produtos_df['clientes_unicos'] / produtos_df['clientes_unicos'].max() * 30) +
                    (produtos_df['taxa_recompra'] / 100 * 30)
                ).round(1)
            else:
                produtos_df['score_performance'] = 0
            
            return produtos_df.sort_values('valor_total', ascending=False)
            
        except Exception as e:
            print(f"Erro ao analisar produtos: {str(e)}")
            # Retornar DataFrame vazio com colunas esperadas
            return pd.DataFrame(columns=[
                'produto', 'quantidade_vendida', 'valor_total', 'total_vendas',
                'clientes_unicos', 'ticket_medio', 'primeira_venda', 'ultima_venda',
                'dias_desde_ultima', 'categoria', 'classificacao_abc', 'taxa_recompra',
                'margem_media', 'score_performance'
            ])
    
    def categorizar_produto(self, nome_produto):
        """Categoriza produto baseado no nome"""
        if pd.isna(nome_produto):
            return 'Outros'
        
        nome_upper = str(nome_produto).upper()
        
        categorias = {
            'Especiarias': ['CANELA', 'CRAVO', 'PIMENTA', 'GENGIBRE', 'CURCUMA', 'PAPRICA', 'ALHO', 'CEBOLA'],
            'Frutas Secas': ['UVA PASSA', 'DAMASCO', 'GOJI', 'CRANBERRY', 'AMEIXA'],
            'Oleaginosas': ['AMENDOA', 'CASTANHA', 'NOZES', 'AMENDOIM', 'PISTACHE'],
            'Farinhas': ['FARINHA'],
            'Chás': ['CHA', 'HIBISCO', 'CAMOMILA', 'ERVA DOCE'],
            'Óleos': ['OLEO', 'AZEITE'],
            'Suplementos': ['WHEY', 'PROTEIN', 'COLAGENO', 'VITAMINA'],
            'Grãos/Sementes': ['CHIA', 'LINHACA', 'QUINOA', 'AVEIA', 'GIRASSOL'],
            'Cacau': ['CACAU', 'CHOCOLATE']
        }
        
        for categoria, palavras in categorias.items():
            for palavra in palavras:
                if palavra in nome_upper:
                    return categoria
        
        return 'Outros'
    
    def get_analise_completa_produto(self, produto_id):
        """Retorna análise completa de um produto específico (código ou nome)"""
        try:
            conn = self.db.connect()
            cursor = conn.cursor()
            
            # Verificar qual tabela usar
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='produtos_metricas_v2'")
            use_v2 = cursor.fetchone()[0] > 0
            
            if use_v2:
                # Usar tabela v2
                metricas = pd.read_sql(
                    "SELECT * FROM produtos_metricas_v2 WHERE cod_produto = ?",
                    conn, params=[produto_id]
                )
                produto_nome = metricas['produto'].iloc[0] if not metricas.empty else produto_id
                produto = produto_nome
                cod_produto = produto_id
            else:
                # Métricas básicas
                metricas_query = """
                SELECT
                    produto,
                    SUM(quantidade) as quantidade_vendida,
                    SUM(total) as valor_total,
                    COUNT(*) as qtd_vendas,
                    COUNT(DISTINCT parceiro) as clientes_unicos,
                    AVG(total) as ticket_medio,
                    MIN(data) as primeira_venda,
                    MAX(data) as ultima_venda
                FROM vendas
                WHERE produto = ?
                GROUP BY produto
                """
                metricas = pd.read_sql(metricas_query, conn, params=[produto_id])
                produto_nome = produto_id
                produto = produto_id
                cod_produto = None

            if metricas.empty:
                return None
            
            # Adicionar dias desde última venda
            metricas['ultima_venda'] = pd.to_datetime(metricas['ultima_venda'])
            metricas['dias_desde_ultima'] = (datetime.now() - metricas['ultima_venda']).dt.days
            
            # Taxa de recompra
            recompra_query = """
            SELECT 
                COUNT(DISTINCT parceiro) as total_clientes,
                SUM(CASE WHEN compras > 1 THEN 1 ELSE 0 END) as clientes_recorrentes
            FROM (
                SELECT parceiro, COUNT(*) as compras
                FROM vendas
                WHERE produto = ?
                GROUP BY parceiro
            ) t
            """
            recompra = pd.read_sql(recompra_query, conn, params=[produto])
            
            if recompra['total_clientes'][0] > 0:
                taxa_recompra = (recompra['clientes_recorrentes'][0] / recompra['total_clientes'][0]) * 100
            else:
                taxa_recompra = 0
            
            metricas['taxa_recompra'] = taxa_recompra
            
            # Clientes que compraram
            clientes_query = """
            SELECT 
                parceiro,
                SUM(quantidade) as qtd_total,
                SUM(total) as valor_total,
                COUNT(*) as frequencia,
                MIN(data) as primeira_compra,
                MAX(data) as ultima_compra
            FROM vendas
            WHERE produto = ?
            GROUP BY parceiro
            ORDER BY valor_total DESC
            """
            clientes = pd.read_sql(clientes_query, conn, params=[produto])
            
            # Evolução temporal
            evolucao_query = """
            SELECT 
                strftime('%Y-%m', data) as mes,
                SUM(quantidade) as qtd_vendida,
                SUM(total) as valor_total,
                COUNT(DISTINCT parceiro) as clientes_unicos,
                AVG(preco_final) as preco_medio
            FROM vendas
            WHERE produto = ?
            AND data IS NOT NULL
            GROUP BY mes
            ORDER BY mes
            """
            evolucao = pd.read_sql(evolucao_query, conn, params=[produto])
            
            # Produtos complementares
            complementares = self.get_produtos_complementares(produto)
            
            # Análise de margem
            margem_query = """
            SELECT 
                AVG(CASE 
                    WHEN preco_base > 0 THEN ((preco_final - preco_base) / preco_base * 100)
                    ELSE 0 
                END) as margem_media,
                MIN(preco_final) as preco_minimo,
                MAX(preco_final) as preco_maximo,
                AVG(preco_final) as preco_medio
            FROM vendas
            WHERE produto = ?
            """
            margem = pd.read_sql(margem_query, conn, params=[produto])
            
            # Sazonalidade
            sazonalidade = self.analisar_sazonalidade(produto)
            
            return {
                'metricas': metricas.to_dict('records')[0] if not metricas.empty else {},
                'clientes': clientes.to_dict('records') if not clientes.empty else [],
                'evolucao': evolucao.to_dict('records') if not evolucao.empty else [],
                'complementares': complementares,
                'margem': margem.to_dict('records')[0] if not margem.empty else {},
                'sazonalidade': sazonalidade
            }
            
        except Exception as e:
            print(f"Erro ao analisar produto {produto_id}: {str(e)}")
            return None
    
    def get_produtos_complementares(self, produto):
        """Identifica produtos frequentemente comprados juntos"""
        try:
            conn = self.db.connect()
            
            # Encontrar vendas que incluem o produto
            vendas_query = """
            SELECT DISTINCT n_venda
            FROM vendas
            WHERE produto = ?
            """
            vendas_df = pd.read_sql(vendas_query, conn, params=[produto])
            
            if vendas_df.empty:
                return []
            
            vendas_list = vendas_df['n_venda'].tolist()
            
            if not vendas_list:
                return []
            
            # Criar placeholder para query
            placeholders = ','.join(['?' for _ in vendas_list])
            
            # Produtos comprados nas mesmas vendas
            complementares_query = f"""
            SELECT 
                produto,
                COUNT(DISTINCT n_venda) as freq_conjunta,
                SUM(total) as valor_conjunto
            FROM vendas
            WHERE n_venda IN ({placeholders})
            AND produto != ?
            GROUP BY produto
            ORDER BY freq_conjunta DESC
            LIMIT 10
            """
            
            params = vendas_list + [produto]
            complementares = pd.read_sql(complementares_query, conn, params=params)
            
            if not complementares.empty:
                complementares['confianca'] = (complementares['freq_conjunta'] / len(vendas_list) * 100).round(1)
                return complementares[['produto', 'freq_conjunta', 'confianca', 'valor_conjunto']].to_dict('records')
            
            return []
            
        except Exception as e:
            print(f"Erro ao buscar produtos complementares: {str(e)}")
            return []
    
    def analisar_sazonalidade(self, produto):
        """Analisa padrões sazonais do produto"""
        try:
            conn = self.db.connect()
            
            query = """
            SELECT 
                strftime('%m', data) as mes_num,
                SUM(quantidade) as qtd,
                SUM(total) as valor
            FROM vendas
            WHERE produto = ?
            AND data IS NOT NULL
            GROUP BY mes_num
            ORDER BY mes_num
            """
            
            vendas_mensais = pd.read_sql(query, conn, params=[produto])
            
            if vendas_mensais.empty:
                return []
            
            # Mapear nome dos meses
            meses = {
                '01': 'Janeiro', '02': 'Fevereiro', '03': 'Março',
                '04': 'Abril', '05': 'Maio', '06': 'Junho',
                '07': 'Julho', '08': 'Agosto', '09': 'Setembro',
                '10': 'Outubro', '11': 'Novembro', '12': 'Dezembro'
            }
            
            vendas_mensais['mes_nome'] = vendas_mensais['mes_num'].map(meses)
            
            # Identificar meses de pico
            media = vendas_mensais['valor'].mean()
            vendas_mensais['tipo'] = vendas_mensais['valor'].apply(
                lambda x: 'Pico' if x > media * 1.3 else ('Baixa' if x < media * 0.7 else 'Normal')
            )
            
            return vendas_mensais.to_dict('records')
            
        except Exception as e:
            print(f"Erro ao analisar sazonalidade: {str(e)}")
            return []
    
    def analisar_mix_produtos(self):
        """Analisa o mix de produtos e sugere otimizações"""
        try:
            conn = self.db.connect()
            
            # Buscar todos os produtos
            produtos_df = self.get_todos_produtos_analise()
            
            if produtos_df.empty:
                return pd.DataFrame()
            
            # Agrupar por categoria
            analise_categorias = produtos_df.groupby('categoria').agg({
                'valor_total': 'sum',
                'clientes_unicos': 'mean',
                'taxa_recompra': 'mean',
                'produto': 'count'
            }).round(2)
            
            analise_categorias.columns = ['valor_total', 'media_clientes', 'taxa_recompra_media', 'qtd_produtos']
            analise_categorias['pct_faturamento'] = (
                analise_categorias['valor_total'] / analise_categorias['valor_total'].sum() * 100
            ).round(1)
            
            # Potencial de crescimento
            analise_categorias['potencial'] = (
                analise_categorias['media_clientes'] * analise_categorias['taxa_recompra_media'] / 100
            ).round(1)
            
            return analise_categorias.sort_values('valor_total', ascending=False)
            
        except Exception as e:
            print(f"Erro ao analisar mix de produtos: {str(e)}")
            return pd.DataFrame()
    
    def get_produtos_para_acao(self):
        """Identifica produtos que precisam de ação"""
        try:
            conn = self.db.connect()
            
            # Produtos sem venda recente
            sem_venda_query = """
            SELECT 
                produto,
                SUM(total) as valor_total,
                COUNT(DISTINCT parceiro) as clientes_unicos,
                julianday('now') - julianday(MAX(data)) as dias_desde_ultima
            FROM vendas
            WHERE produto IS NOT NULL
            GROUP BY produto
            HAVING dias_desde_ultima > 30
            ORDER BY valor_total DESC
            LIMIT 20
            """
            sem_venda_recente = pd.read_sql(sem_venda_query, conn)
            
            # Produtos com baixa taxa de recompra (calcular inline)
            baixa_recompra = []
            produtos_df = self.get_todos_produtos_analise()
            if not produtos_df.empty:
                baixa = produtos_df[
                    (produtos_df['taxa_recompra'] < 20) & 
                    (produtos_df['clientes_unicos'] > 5)
                ].head(20)
                baixa_recompra = baixa[['produto', 'valor_total', 'taxa_recompra', 'clientes_unicos']].to_dict('records')
            
            # Produtos com margem baixa
            margem_query = """
            SELECT 
                produto,
                SUM(total) as valor_total,
                AVG(CASE 
                    WHEN preco_base > 0 THEN ((preco_final - preco_base) / preco_base * 100)
                    ELSE 0 
                END) as margem_media
            FROM vendas
            WHERE produto IS NOT NULL
            GROUP BY produto
            HAVING margem_media < 10 AND margem_media > 0
            ORDER BY valor_total DESC
            LIMIT 20
            """
            margem_baixa = pd.read_sql(margem_query, conn)
            
            return {
                'sem_venda_recente': sem_venda_recente.to_dict('records') if not sem_venda_recente.empty else [],
                'baixa_recompra': baixa_recompra,
                'margem_baixa': margem_baixa.to_dict('records') if not margem_baixa.empty else []
            }
            
        except Exception as e:
            print(f"Erro ao buscar produtos para ação: {str(e)}")
            return {
                'sem_venda_recente': [],
                'baixa_recompra': [],
                'margem_baixa': []
            }
    
    def get_relatorio_executivo_produtos(self):
        """Gera relatório executivo sobre produtos"""
        try:
            conn = self.db.connect()
            
            # KPIs principais
            kpis_query = """
            SELECT 
                COUNT(DISTINCT produto) as total_produtos,
                SUM(total) as faturamento_total
            FROM vendas
            WHERE produto IS NOT NULL
            """
            kpis = pd.read_sql(kpis_query, conn)
            
            # Adicionar taxa de recompra e margem média
            produtos_df = self.get_todos_produtos_analise()
            if not produtos_df.empty:
                kpis['taxa_recompra_media'] = produtos_df['taxa_recompra'].mean()
                kpis['margem_media_geral'] = produtos_df['margem_media'].mean()
            else:
                kpis['taxa_recompra_media'] = 0
                kpis['margem_media_geral'] = 0
            
            # Top produtos
            top_query = """
            SELECT produto, SUM(total) as valor_total
            FROM vendas
            WHERE produto IS NOT NULL
            GROUP BY produto
            ORDER BY valor_total DESC
            LIMIT 5
            """
            top_produtos = pd.read_sql(top_query, conn)
            
            # Produtos problemáticos
            problematicos_query = """
            SELECT 
                produto,
                julianday('now') - julianday(MAX(data)) as dias_desde_ultima,
                SUM(total) as valor_total
            FROM vendas
            WHERE produto IS NOT NULL
            GROUP BY produto
            HAVING dias_desde_ultima > 60 AND valor_total > 1000
            ORDER BY valor_total DESC
            LIMIT 5
            """
            problematicos = pd.read_sql(problematicos_query, conn)
            
            # Mix de categorias
            mix_categorias = self.analisar_mix_produtos()
            
            return {
                'kpis': kpis.to_dict('records')[0] if not kpis.empty else {},
                'top_produtos': top_produtos.to_dict('records') if not top_produtos.empty else [],
                'produtos_problematicos': problematicos.to_dict('records') if not problematicos.empty else [],
                'mix_categorias': mix_categorias.to_dict() if not mix_categorias.empty else {}
            }
            
        except Exception as e:
            print(f"Erro ao gerar relatório executivo: {str(e)}")
            return {
                'kpis': {},
                'top_produtos': [],
                'produtos_problematicos': [],
                'mix_categorias': {}
            }