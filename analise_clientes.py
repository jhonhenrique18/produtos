"""
Sistema de análise avançada de clientes
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from db_manager_v2 import DatabaseManager

class AnalisadorClientes:
    def __init__(self, db_manager):
        self.db = db_manager
    
    def get_analise_completa_cliente(self, cliente_id):
        """Retorna análise completa de um cliente específico (por código ou nome)"""
        
        # Verificar se é código ou nome
        conn = self.db.connect()
        cursor = conn.cursor()
        
        # Tentar primeiro como código
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='clientes_metricas_v2'")
        use_v2 = cursor.fetchone()[0] > 0
        
        if use_v2:
            # Usar tabela v2 com códigos
            cliente_info = self.db.get_cliente_data_v2(cliente_id)
            if cliente_info.empty:
                return None
            
            cod_parceiro = cliente_info['cod_parceiro'].iloc[0]
            parceiro = cliente_info['parceiro'].iloc[0]
            
            # Produtos comprados
            produtos = self.db.get_produtos_cliente_v2(cod_parceiro)
            
            # Histórico de compras
            historico = pd.read_sql('''
                SELECT data, n_venda, cod_produto, produto, quantidade, total
                FROM vendas
                WHERE cod_parceiro = ?
                ORDER BY data DESC
            ''', conn, params=[cod_parceiro])
        else:
            # Usar tabela antiga
            cliente_info = self.db.get_cliente_data(cliente_id)
            if cliente_info.empty:
                return None
            
            parceiro = cliente_id
            
            # Produtos comprados
            produtos = self.db.get_produtos_cliente(parceiro)
            
            # Histórico de compras
            historico = pd.read_sql('''
                SELECT data, n_venda, produto, quantidade, total
                FROM vendas
                WHERE parceiro = ?
                ORDER BY data DESC
            ''', conn, params=[parceiro])
        
        # Determinar identificador para outras funções
        if use_v2:
            identificador = cod_parceiro
        else:
            identificador = parceiro
        
        # Análise de categorias
        categorias = self.analisar_categorias_cliente(identificador, use_v2)
        
        # Produtos não comprados (oportunidades)
        produtos_nao_comprados = self.get_produtos_nao_comprados(identificador, use_v2)
        
        # Análise de frequência
        frequencia = self.analisar_frequencia_compra(identificador, use_v2)
        
        # Recomendações
        recomendacoes = self.gerar_recomendacoes(identificador, use_v2)
        
        return {
            'info_basica': cliente_info.to_dict('records')[0],
            'produtos_comprados': produtos.to_dict('records'),
            'historico': historico.to_dict('records'),
            'categorias': categorias,
            'produtos_nao_comprados': produtos_nao_comprados,
            'frequencia': frequencia,
            'recomendacoes': recomendacoes
        }
    
    def analisar_categorias_cliente(self, cliente_id, use_v2=False):
        """Analisa as categorias de produtos que o cliente compra"""
        conn = self.db.connect()
        
        # Definir categorias
        categorias_map = {
            'Especiarias': ['CANELA', 'CRAVO', 'PIMENTA', 'GENGIBRE', 'CURCUMA', 'PAPRICA', 'ALHO', 'CEBOLA', 'OREGANO'],
            'Frutas Secas': ['UVA PASSA', 'DAMASCO', 'GOJI', 'CRANBERRY', 'AMEIXA', 'TAMARA'],
            'Oleaginosas': ['AMENDOA', 'CASTANHA', 'NOZES', 'AMENDOIM', 'PISTACHE', 'MACADAMIA'],
            'Farinhas': ['FARINHA'],
            'Chás e Ervas': ['CHA', 'HIBISCO', 'CAMOMILA', 'ERVA DOCE', 'HORTELA', 'BOLDO'],
            'Óleos e Manteigas': ['OLEO', 'MANTEIGA', 'GHEE'],
            'Suplementos': ['WHEY', 'PROTEIN', 'COLAGENO', 'VITAMINA', 'OMEGA'],
            'Grãos e Sementes': ['CHIA', 'LINHACA', 'QUINOA', 'AVEIA', 'GIRASSOL', 'ABOBORA'],
            'Açúcares e Adoçantes': ['ACUCAR', 'MEL', 'XILITOL', 'ERITRITOL', 'STEVIA'],
            'Cacau e Chocolate': ['CACAU', 'CHOCOLATE', 'NIBS']
        }
        
        # Buscar produtos do cliente
        if use_v2:
            query = '''
                SELECT produto, SUM(total) as valor_total, SUM(quantidade) as qtd_total
                FROM vendas
                WHERE cod_parceiro = ?
                GROUP BY produto
            '''
        else:
            query = '''
                SELECT produto, SUM(total) as valor_total, SUM(quantidade) as qtd_total
                FROM vendas
                WHERE parceiro = ?
                GROUP BY produto
            '''
        df_produtos = pd.read_sql(query, conn, params=[cliente_id])
        
        # Classificar produtos em categorias
        resultado = {}
        for categoria, palavras in categorias_map.items():
            mask = df_produtos['produto'].str.contains('|'.join(palavras), case=False, na=False)
            if mask.any():
                resultado[categoria] = {
                    'valor_total': df_produtos[mask]['valor_total'].sum(),
                    'qtd_produtos': mask.sum(),
                    'produtos': df_produtos[mask]['produto'].tolist()
                }
        
        # Adicionar "Outros" para produtos não categorizados
        categorizados = set()
        for cat_data in resultado.values():
            categorizados.update(cat_data['produtos'])
        
        nao_categorizados = df_produtos[~df_produtos['produto'].isin(categorizados)]
        if not nao_categorizados.empty:
            resultado['Outros'] = {
                'valor_total': nao_categorizados['valor_total'].sum(),
                'qtd_produtos': len(nao_categorizados),
                'produtos': nao_categorizados['produto'].tolist()
            }
        
        return resultado
    
    def get_produtos_nao_comprados(self, cliente_id, use_v2=False):
        """Retorna produtos que o cliente nunca comprou"""
        conn = self.db.connect()
        
        # Produtos que o cliente já comprou
        if use_v2:
            produtos_comprados = pd.read_sql('''
                SELECT DISTINCT produto 
                FROM vendas 
                WHERE cod_parceiro = ?
            ''', conn, params=[cliente_id])['produto'].tolist()
        else:
            produtos_comprados = pd.read_sql('''
                SELECT DISTINCT produto 
                FROM vendas 
                WHERE parceiro = ?
            ''', conn, params=[cliente_id])['produto'].tolist()
        
        # Todos os produtos disponíveis com suas métricas
        if use_v2:
            tabela_produtos = 'produtos_metricas_v2'
            col_produto = 'cod_produto, produto'
        else:
            tabela_produtos = 'produtos_metricas'
            col_produto = 'produto'
        
        if produtos_comprados:
            todos_produtos = pd.read_sql(f'''
                SELECT 
                    {col_produto},
                    valor_total,
                    clientes_unicos,
                    taxa_recompra,
                    dias_desde_ultima
                FROM {tabela_produtos}
                WHERE produto NOT IN ({','.join(['?'] * len(produtos_comprados))})
                ORDER BY valor_total DESC
            ''', conn, params=produtos_comprados)
        else:
            todos_produtos = pd.read_sql(f'''
                SELECT 
                    {col_produto},
                    valor_total,
                    clientes_unicos,
                    taxa_recompra,
                    dias_desde_ultima
                FROM {tabela_produtos}
                ORDER BY valor_total DESC
            ''', conn)
        
        # Adicionar score de recomendação
        todos_produtos['score_recomendacao'] = (
            todos_produtos['clientes_unicos'] * 0.3 +
            todos_produtos['taxa_recompra'] * 0.4 +
            (100 - todos_produtos['dias_desde_ultima'].fillna(100)) * 0.3
        )
        
        return todos_produtos.sort_values('score_recomendacao', ascending=False).to_dict('records')
    
    def analisar_frequencia_compra(self, cliente_id, use_v2=False):
        """Analisa padrão de frequência de compra do cliente"""
        conn = self.db.connect()
        
        # Buscar datas de compra
        if use_v2:
            datas = pd.read_sql('''
                SELECT DISTINCT data
                FROM vendas
                WHERE cod_parceiro = ?
                ORDER BY data
            ''', conn, params=[cliente_id])
        else:
            datas = pd.read_sql('''
                SELECT DISTINCT data
                FROM vendas
                WHERE parceiro = ?
                ORDER BY data
            ''', conn, params=[cliente_id])
        
        if len(datas) < 2:
            return {
                'frequencia_media_dias': None,
                'desvio_padrao_dias': None,
                'previsao_proxima_compra': None,
                'status_frequencia': 'Cliente Novo'
            }
        
        # Calcular intervalos entre compras
        datas['data'] = pd.to_datetime(datas['data'])
        intervalos = datas['data'].diff().dt.days.dropna()
        
        freq_media = intervalos.mean()
        desvio = intervalos.std()
        ultima_compra = datas['data'].max()
        dias_desde_ultima = (datetime.now() - ultima_compra).days
        
        # Prever próxima compra
        previsao = ultima_compra + timedelta(days=freq_media)
        
        # Determinar status
        if dias_desde_ultima > freq_media + desvio:
            status = 'Atrasado - Precisa contato'
        elif dias_desde_ultima > freq_media:
            status = 'Chegando a hora de comprar'
        else:
            status = 'Dentro do padrão'
        
        return {
            'frequencia_media_dias': round(freq_media, 1),
            'desvio_padrao_dias': round(desvio, 1) if not pd.isna(desvio) else 0,
            'previsao_proxima_compra': previsao.strftime('%Y-%m-%d'),
            'dias_desde_ultima': dias_desde_ultima,
            'status_frequencia': status
        }
    
    def gerar_recomendacoes(self, cliente_id, use_v2=False):
        """Gera recomendações de ação para o cliente"""
        conn = self.db.connect()

        # Informações do cliente
        if use_v2:
            cliente = self.db.get_cliente_data_v2(cliente_id)
            if cliente.empty:
                return []
            parceiro = cliente['parceiro'].iloc[0]
        else:
            cliente = self.db.get_cliente_data(cliente_id)
            if cliente.empty:
                return []
            parceiro = cliente_id

        cliente = cliente.iloc[0]
        frequencia = self.analisar_frequencia_compra(cliente_id, use_v2)
        
        recomendacoes = []
        
        # 1. Baseado no segmento
        if cliente['segmento'] == 'Em Risco':
            recomendacoes.append({
                'tipo': 'Reativação',
                'urgencia': 'Alta',
                'acao': 'Contato imediato com desconto especial',
                'motivo': f"Cliente não compra há {cliente['dias_desde_ultima']} dias"
            })
        
        elif cliente['segmento'] == 'VIP':
            recomendacoes.append({
                'tipo': 'Fidelização',
                'urgencia': 'Média',
                'acao': 'Oferecer benefícios exclusivos VIP',
                'motivo': 'Cliente de alto valor - manter relacionamento'
            })
        
        # 2. Baseado na frequência
        if frequencia['status_frequencia'] == 'Atrasado - Precisa contato':
            recomendacoes.append({
                'tipo': 'Follow-up',
                'urgencia': 'Alta',
                'acao': 'Ligar para entender motivo da ausência',
                'motivo': f"Ultrapassou frequência média de compra em {frequencia['dias_desde_ultima'] - frequencia['frequencia_media_dias']:.0f} dias"
            })
        
        # 3. Cross-sell baseado em produtos similares
        # Buscar produtos frequentemente comprados juntos
        if use_v2:
            produtos_cliente = pd.read_sql('''
                SELECT DISTINCT produto FROM vendas WHERE cod_parceiro = ?
            ''', conn, params=[cliente_id])['produto'].tolist()
        else:
            produtos_cliente = pd.read_sql('''
                SELECT DISTINCT produto FROM vendas WHERE parceiro = ?
            ''', conn, params=[cliente_id])['produto'].tolist()
        
        if produtos_cliente:
            # Encontrar clientes similares
            if use_v2:
                clientes_similares = pd.read_sql('''
                    SELECT cod_parceiro, parceiro, COUNT(DISTINCT produto) as produtos_comum
                    FROM vendas
                    WHERE produto IN ({})
                    AND cod_parceiro != ?
                    GROUP BY cod_parceiro, parceiro
                    HAVING produtos_comum >= 3
                    ORDER BY produtos_comum DESC
                    LIMIT 10
                '''.format(','.join(['?'] * len(produtos_cliente))), 
                conn, params=produtos_cliente + [cliente_id])
            else:
                clientes_similares = pd.read_sql('''
                    SELECT parceiro, COUNT(DISTINCT produto) as produtos_comum
                    FROM vendas
                    WHERE produto IN ({})
                    AND parceiro != ?
                    GROUP BY parceiro
                    HAVING produtos_comum >= 3
                    ORDER BY produtos_comum DESC
                    LIMIT 10
                '''.format(','.join(['?'] * len(produtos_cliente))), 
                conn, params=produtos_cliente + [cliente_id])
            
            if not clientes_similares.empty:
                # Ver o que eles compram que nosso cliente não compra
                similares_list = clientes_similares['parceiro'].tolist()
                produtos_sugestao = pd.read_sql('''
                    SELECT produto, COUNT(DISTINCT parceiro) as freq
                    FROM vendas
                    WHERE parceiro IN ({})
                    AND produto NOT IN ({})
                    GROUP BY produto
                    ORDER BY freq DESC
                    LIMIT 5
                '''.format(
                    ','.join(['?'] * len(similares_list)),
                    ','.join(['?'] * len(produtos_cliente))
                ), conn, params=similares_list + produtos_cliente)
                
                if not produtos_sugestao.empty:
                    top_sugestoes = produtos_sugestao['produto'].head(3).tolist()
                    recomendacoes.append({
                        'tipo': 'Cross-sell',
                        'urgencia': 'Média',
                        'acao': f"Oferecer: {', '.join(top_sugestoes[:2])}",
                        'motivo': 'Produtos populares entre clientes similares'
                    })
        
        # 4. Recompra de produtos
        produtos_recompra = pd.read_sql('''
            SELECT 
                produto,
                MAX(data) as ultima_compra,
                AVG(quantidade) as qtd_media,
                COUNT(*) as vezes_comprado
            FROM vendas
            WHERE parceiro = ?
            GROUP BY produto
            HAVING vezes_comprado > 1
        ''', conn, params=[parceiro])
        
        if not produtos_recompra.empty:
            produtos_recompra['ultima_compra'] = pd.to_datetime(produtos_recompra['ultima_compra'])
            produtos_recompra['dias_desde'] = (datetime.now() - produtos_recompra['ultima_compra']).dt.days
            
            # Produtos que já passou da hora de recomprar
            produtos_atrasados = produtos_recompra[produtos_recompra['dias_desde'] > 60]['produto'].head(3).tolist()
            
            if produtos_atrasados:
                recomendacoes.append({
                    'tipo': 'Recompra',
                    'urgencia': 'Alta',
                    'acao': f"Lembrar recompra: {', '.join(produtos_atrasados[:2])}",
                    'motivo': 'Produtos recorrentes não comprados recentemente'
                })
        
        return recomendacoes
    
    def get_clientes_para_acao(self, tipo_acao=None):
        """Retorna lista de clientes que precisam de ação"""
        conn = self.db.connect()
        
        # Clientes em risco
        em_risco = pd.read_sql('''
            SELECT 
                parceiro,
                total_compras,
                qtd_compras,
                dias_desde_ultima,
                segmento
            FROM clientes_metricas
            WHERE segmento IN ('Em Risco', 'Inativo')
            ORDER BY total_compras DESC
        ''', conn)
        
        # Clientes para reativação
        para_reativar = pd.read_sql('''
            SELECT 
                parceiro,
                total_compras,
                dias_desde_ultima
            FROM clientes_metricas
            WHERE dias_desde_ultima > 60
            AND qtd_compras > 1
            ORDER BY total_compras DESC
        ''', conn)
        
        # Oportunidades de cross-sell (clientes que compram poucas categorias)
        cross_sell = pd.read_sql('''
            SELECT 
                c.parceiro,
                c.total_compras,
                c.total_produtos_unicos,
                c.qtd_compras
            FROM clientes_metricas c
            WHERE c.total_produtos_unicos < 5
            AND c.qtd_compras > 2
            AND c.dias_desde_ultima < 60
            ORDER BY c.total_compras DESC
        ''', conn)
        
        return {
            'em_risco': em_risco.to_dict('records'),
            'para_reativar': para_reativar.to_dict('records'),
            'cross_sell': cross_sell.to_dict('records')
        }
    
    def gerar_script_abordagem(self, cliente_id):
        """Gera script personalizado de abordagem para o cliente"""
        # Verificar se usar v2
        conn = self.db.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='clientes_metricas_v2'")
        use_v2 = cursor.fetchone()[0] > 0
        
        if use_v2:
            cliente = self.db.get_cliente_data_v2(cliente_id)
            if not cliente.empty:
                cliente = cliente.iloc[0]
                produtos = self.db.get_produtos_cliente_v2(cliente_id)
            else:
                return "Cliente não encontrado"
        else:
            cliente = self.db.get_cliente_data(cliente_id)
            if not cliente.empty:
                cliente = cliente.iloc[0]
                produtos = self.db.get_produtos_cliente(cliente_id)
            else:
                return "Cliente não encontrado"
        
        frequencia = self.analisar_frequencia_compra(cliente_id, use_v2)
        
        # Produtos mais comprados - converter valor_total para float
        if not produtos.empty:
            produtos['valor_total'] = pd.to_numeric(produtos['valor_total'], errors='coerce').fillna(0)
            top_produtos = produtos.nlargest(3, 'valor_total')['produto'].tolist()
        else:
            top_produtos = []
        
        script = f"""
SCRIPT DE ABORDAGEM - {cliente.get('parceiro', cliente_id)}
{'='*50}

INFORMAÇÕES DO CLIENTE:
- Segmento: {cliente['segmento']}
- Total de compras: R$ {cliente['total_compras']:,.2f}
- Última compra: há {cliente['dias_desde_ultima']} dias
- Frequência média: a cada {frequencia.get('frequencia_media_dias', 'N/A')} dias

ABERTURA SUGERIDA:
"Olá! Aqui é [NOME] da [EMPRESA]. 
Percebi que faz {cliente['dias_desde_ultima']} dias desde sua última compra.
Como cliente {cliente['segmento']}, gostaria de oferecer condições especiais."

PRODUTOS PARA MENCIONAR:
"""
        
        for i, prod in enumerate(top_produtos, 1):
            script += f"\n{i}. {prod}"
        
        if cliente['segmento'] == 'Em Risco':
            script += """

OFERTA ESPECIAL:
"Para reativar nossa parceria, preparei um desconto exclusivo de 15% 
em todos os produtos que você costuma comprar."
"""
        elif cliente['segmento'] == 'VIP':
            script += """

BENEFÍCIO VIP:
"Como nosso cliente VIP, você tem direito a:
- Desconto progressivo de até 20%
- Frete grátis em pedidos acima de R$ 500
- Prioridade no atendimento"
"""
        
        script += """

FECHAMENTO:
"Posso preparar seu pedido com essas condições especiais?"

OBSERVAÇÕES:
- Cliente responde melhor em [definir melhor horário baseado no histórico]
- Preferência por [análise de produtos mais comprados]
"""
        
        return script