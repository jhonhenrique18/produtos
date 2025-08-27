"""
Gerenciador do banco de dados SQLite - Versão com códigos
"""
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from pathlib import Path

class DatabaseManager:
    def __init__(self, db_path='database.db'):
        self.db_path = db_path
        self.conn = None
        self.init_database()
    
    def connect(self):
        """Conecta ao banco de dados"""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        return self.conn
    
    def init_database(self):
        """Inicializa o banco com as tabelas necessárias"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Tabela principal de vendas (já existe, não vamos alterar)
        
        # Tabela de métricas agregadas de clientes - AGORA COM CÓDIGO
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS clientes_metricas_v2 (
            cod_parceiro TEXT PRIMARY KEY,
            parceiro TEXT,
            total_compras REAL,
            qtd_compras INTEGER,
            ticket_medio REAL,
            primeira_compra DATE,
            ultima_compra DATE,
            dias_desde_ultima INTEGER,
            frequencia_media_dias REAL,
            total_produtos_unicos INTEGER,
            categoria_principal TEXT,
            segmento TEXT,
            score_cliente REAL,
            status TEXT,
            tendencia TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Tabela de produtos por cliente - COM CÓDIGOS
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS cliente_produtos_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cod_parceiro TEXT,
            parceiro TEXT,
            cod_produto TEXT,
            produto TEXT,
            quantidade_total REAL,
            valor_total REAL,
            qtd_compras INTEGER,
            primeira_compra DATE,
            ultima_compra DATE,
            dias_desde_ultima INTEGER,
            frequencia_compra_dias REAL,
            FOREIGN KEY (cod_parceiro) REFERENCES clientes_metricas_v2(cod_parceiro)
        )
        ''')
        
        # Tabela de métricas de produtos - COM CÓDIGOS
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS produtos_metricas_v2 (
            cod_produto TEXT PRIMARY KEY,
            produto TEXT,
            quantidade_vendida REAL,
            valor_total REAL,
            qtd_vendas INTEGER,
            clientes_unicos INTEGER,
            ticket_medio REAL,
            margem_media REAL,
            taxa_recompra REAL,
            primeira_venda DATE,
            ultima_venda DATE,
            dias_desde_ultima INTEGER,
            categoria TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Índices para performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_vendas_cod_parceiro ON vendas(cod_parceiro)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_vendas_cod_produto ON vendas(cod_produto)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cliente_produtos_v2 ON cliente_produtos_v2(cod_parceiro, cod_produto)')
        
        conn.commit()
    
    def update_metrics(self):
        """Atualiza todas as tabelas de métricas usando códigos"""
        print("Atualizando métricas com códigos...")
        conn = self.connect()
        
        # Atualizar métricas de clientes
        self._update_cliente_metrics_v2(conn)
        
        # Atualizar produtos por cliente
        self._update_cliente_produtos_v2(conn)
        
        # Atualizar métricas de produtos
        self._update_produto_metrics_v2(conn)
        
        conn.commit()
        print("OK: Métricas atualizadas com códigos!")
    
    def _update_cliente_metrics_v2(self, conn):
        """Atualiza métricas agregadas de clientes usando código"""
        cursor = conn.cursor()
        
        # Limpar tabela
        cursor.execute('DELETE FROM clientes_metricas_v2')
        
        # Query para calcular métricas - AGORA AGRUPANDO POR CÓDIGO
        query = '''
        INSERT INTO clientes_metricas_v2 (
            cod_parceiro, parceiro, total_compras, qtd_compras, ticket_medio,
            primeira_compra, ultima_compra, dias_desde_ultima,
            total_produtos_unicos
        )
        SELECT 
            cod_parceiro,
            MAX(parceiro) as parceiro,  -- Pega o nome mais recente
            SUM(total) as total_compras,
            COUNT(DISTINCT n_venda) as qtd_compras,
            AVG(total) as ticket_medio,
            MIN(data) as primeira_compra,
            MAX(data) as ultima_compra,
            CAST(julianday('now') - julianday(MAX(data)) as INTEGER) as dias_desde_ultima,
            COUNT(DISTINCT cod_produto) as total_produtos_unicos
        FROM vendas
        WHERE cod_parceiro IS NOT NULL AND cod_parceiro != ''
        GROUP BY cod_parceiro
        '''
        
        cursor.execute(query)
        
        # Atualizar segmentos
        self._update_segmentos_v2(conn)
    
    def _update_segmentos_v2(self, conn):
        """Classifica clientes em segmentos"""
        df = pd.read_sql('''
            SELECT cod_parceiro, parceiro, total_compras, qtd_compras, dias_desde_ultima
            FROM clientes_metricas_v2
        ''', conn)
        
        def classificar_cliente(row):
            if row['qtd_compras'] >= 10 and row['dias_desde_ultima'] <= 30:
                return 'VIP'
            elif row['qtd_compras'] >= 5 and row['dias_desde_ultima'] <= 60:
                return 'Fiel'
            elif row['qtd_compras'] >= 3 and row['dias_desde_ultima'] <= 90:
                return 'Regular'
            elif row['dias_desde_ultima'] > 90:
                return 'Inativo'
            elif row['qtd_compras'] == 1 and row['dias_desde_ultima'] <= 30:
                return 'Novo'
            elif row['qtd_compras'] == 1:
                return 'One-Shot'
            elif row['dias_desde_ultima'] > 60:
                return 'Em Risco'
            else:
                return 'Em Crescimento'
        
        df['segmento'] = df.apply(classificar_cliente, axis=1)
        
        # Atualizar banco
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute('''
                UPDATE clientes_metricas_v2 
                SET segmento = ? 
                WHERE cod_parceiro = ?
            ''', (row['segmento'], row['cod_parceiro']))
    
    def _update_cliente_produtos_v2(self, conn):
        """Atualiza produtos comprados por cada cliente usando códigos"""
        cursor = conn.cursor()
        
        # Limpar tabela
        cursor.execute('DELETE FROM cliente_produtos_v2')
        
        # Query para agregar produtos por cliente - COM CÓDIGOS
        query = '''
        INSERT INTO cliente_produtos_v2 (
            cod_parceiro, parceiro, cod_produto, produto, 
            quantidade_total, valor_total,
            qtd_compras, primeira_compra, ultima_compra, dias_desde_ultima
        )
        SELECT 
            cod_parceiro,
            MAX(parceiro) as parceiro,
            cod_produto,
            MAX(produto) as produto,
            SUM(quantidade) as quantidade_total,
            SUM(total) as valor_total,
            COUNT(*) as qtd_compras,
            MIN(data) as primeira_compra,
            MAX(data) as ultima_compra,
            CAST(julianday('now') - julianday(MAX(data)) as INTEGER) as dias_desde_ultima
        FROM vendas
        WHERE cod_parceiro IS NOT NULL AND cod_parceiro != ''
            AND cod_produto IS NOT NULL AND cod_produto != ''
        GROUP BY cod_parceiro, cod_produto
        '''
        
        cursor.execute(query)
    
    def _update_produto_metrics_v2(self, conn):
        """Atualiza métricas de produtos usando códigos"""
        cursor = conn.cursor()
        
        # Limpar tabela
        cursor.execute('DELETE FROM produtos_metricas_v2')
        
        # Query para calcular métricas - COM CÓDIGOS
        query = '''
        INSERT INTO produtos_metricas_v2 (
            cod_produto, produto, quantidade_vendida, valor_total, qtd_vendas,
            clientes_unicos, ticket_medio, margem_media,
            primeira_venda, ultima_venda, dias_desde_ultima
        )
        SELECT 
            cod_produto,
            MAX(produto) as produto,
            SUM(quantidade) as quantidade_vendida,
            SUM(total) as valor_total,
            COUNT(*) as qtd_vendas,
            COUNT(DISTINCT cod_parceiro) as clientes_unicos,
            AVG(total) as ticket_medio,
            AVG(CASE 
                WHEN preco_base > 0 THEN ((preco_final - preco_base) / preco_base * 100)
                ELSE 0 
            END) as margem_media,
            MIN(data) as primeira_venda,
            MAX(data) as ultima_venda,
            CAST(julianday('now') - julianday(MAX(data)) as INTEGER) as dias_desde_ultima
        FROM vendas
        WHERE cod_produto IS NOT NULL AND cod_produto != ''
        GROUP BY cod_produto
        '''
        
        cursor.execute(query)
        
        # Calcular taxa de recompra
        df_recompra = pd.read_sql('''
            SELECT cod_produto, cod_parceiro, COUNT(*) as compras
            FROM vendas
            GROUP BY cod_produto, cod_parceiro
        ''', conn)
        
        taxa_recompra = df_recompra.groupby('cod_produto')['compras'].apply(
            lambda x: (x > 1).mean() * 100
        ).reset_index()
        taxa_recompra.columns = ['cod_produto', 'taxa_recompra']
        
        # Atualizar taxas de recompra
        for _, row in taxa_recompra.iterrows():
            cursor.execute('''
                UPDATE produtos_metricas_v2 
                SET taxa_recompra = ? 
                WHERE cod_produto = ?
            ''', (row['taxa_recompra'], row['cod_produto']))
    
    def get_cliente_data_v2(self, cod_parceiro=None):
        """Retorna dados de clientes com código"""
        conn = self.connect()
        
        # Primeiro, garantir que as tabelas v2 estejam atualizadas
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM clientes_metricas_v2")
        if cursor.fetchone()[0] == 0:
            self.update_metrics()
        
        if cod_parceiro:
            query = '''
                SELECT * FROM clientes_metricas_v2
                WHERE cod_parceiro = ?
            '''
            return pd.read_sql(query, conn, params=[cod_parceiro])
        else:
            return pd.read_sql('SELECT * FROM clientes_metricas_v2 ORDER BY total_compras DESC', conn)
    
    def get_produtos_cliente_v2(self, cod_parceiro):
        """Retorna produtos comprados por um cliente com códigos"""
        conn = self.connect()
        query = '''
            SELECT * FROM cliente_produtos_v2
            WHERE cod_parceiro = ?
            ORDER BY valor_total DESC
        '''
        return pd.read_sql(query, conn, params=[cod_parceiro])
    
    def get_produto_data_v2(self):
        """Retorna dados de produtos com códigos"""
        conn = self.connect()
        
        # Garantir que as tabelas estejam atualizadas
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM produtos_metricas_v2")
        if cursor.fetchone()[0] == 0:
            self.update_metrics()
            
        return pd.read_sql('SELECT * FROM produtos_metricas_v2 ORDER BY valor_total DESC', conn)
    
    def get_vendas_raw(self):
        """Retorna dados brutos de vendas"""
        conn = self.connect()
        return pd.read_sql('SELECT * FROM vendas', conn)
    
    def close(self):
        """Fecha conexão com banco"""
        if self.conn:
            self.conn.close()
            self.conn = None

# Compatibilidade com código antigo
DatabaseManager.get_cliente_data = DatabaseManager.get_cliente_data_v2
DatabaseManager.get_produtos_cliente = DatabaseManager.get_produtos_cliente_v2
DatabaseManager.get_produto_data = DatabaseManager.get_produto_data_v2