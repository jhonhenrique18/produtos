"""
Gerenciador do banco de dados SQLite
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
        
        # Tabela principal de vendas
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS vendas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            n_venda TEXT,
            data DATE,
            cod_produto TEXT,
            produto TEXT,
            quantidade REAL,
            preco_unitario REAL,
            valor_bruto REAL,
            unidade_medida TEXT,
            qtd_un_medida REAL,
            valor REAL,
            desconto REAL,
            acrescimo REAL,
            total REAL,
            cod_vendedor TEXT,
            nome_vendedor TEXT,
            ref_fabrica TEXT,
            cod_parceiro TEXT,
            parceiro TEXT,
            preco_final REAL,
            preco_base REAL,
            obs TEXT,
            marca TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Tabela de métricas agregadas de clientes
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS clientes_metricas (
            parceiro TEXT PRIMARY KEY,
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
        
        # Tabela de produtos por cliente
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS cliente_produtos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parceiro TEXT,
            produto TEXT,
            quantidade_total REAL,
            valor_total REAL,
            qtd_compras INTEGER,
            primeira_compra DATE,
            ultima_compra DATE,
            dias_desde_ultima INTEGER,
            frequencia_compra_dias REAL,
            FOREIGN KEY (parceiro) REFERENCES clientes_metricas(parceiro)
        )
        ''')
        
        # Tabela de métricas de produtos
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS produtos_metricas (
            produto TEXT PRIMARY KEY,
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
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_vendas_parceiro ON vendas(parceiro)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_vendas_produto ON vendas(produto)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_vendas_data ON vendas(data)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cliente_produtos ON cliente_produtos(parceiro, produto)')
        
        conn.commit()
    
    def import_csv(self, csv_path):
        """Importa dados do CSV para o banco"""
        print("Importando dados do CSV...")
        
        # Ler CSV
        df = pd.read_csv(csv_path, encoding='latin-1', sep=';', decimal=',')
        
        # Limpar colunas
        df.columns = df.columns.str.strip()
        
        # Função para limpar valores monetários
        def clean_money(val):
            if pd.isna(val):
                return 0
            if isinstance(val, str):
                val = val.replace('R$', '').replace('.', '').replace(',', '.').strip()
            try:
                return float(val)
            except:
                return 0
        
        # Aplicar limpeza
        money_cols = ['Valor Bruto', 'Valor', 'Total', 'Desconto', 'Acréscimo', 
                     'Preço Unitario', 'Preço Final', 'Preço Base']
        for col in money_cols:
            if col in df.columns:
                df[col] = df[col].apply(clean_money)
        
        # Converter data
        df['Data'] = pd.to_datetime(df['Data'], format='%d/%m/%Y', errors='coerce')
        
        # Renomear colunas - tratando caracteres especiais
        # Primeiro, limpar os nomes das colunas existentes
        df.columns = [col.replace('�', '').strip() for col in df.columns]
        
        # Mapear colunas possíveis (com e sem caracteres especiais)
        column_mapping = {}
        for col in df.columns:
            col_clean = col.lower()
            if 'venda' in col_clean and 'n' in col_clean:
                column_mapping[col] = 'n_venda'
            elif col == 'Data':
                column_mapping[col] = 'data'
            elif 'produto' in col_clean and 'classifica' in col_clean:
                column_mapping[col] = 'produto'
            elif 'class' in col_clean and 'produto' not in col_clean:
                column_mapping[col] = 'cod_produto'
            elif col == 'Quantidade':
                column_mapping[col] = 'quantidade'
            elif 'unitario' in col_clean:
                column_mapping[col] = 'preco_unitario'
            elif 'valor bruto' in col_clean:
                column_mapping[col] = 'valor_bruto'
            elif col == 'Unidade Medida':
                column_mapping[col] = 'unidade_medida'
            elif 'qtd. un' in col_clean:
                column_mapping[col] = 'qtd_un_medida'
            elif col == 'Valor' and 'bruto' not in col_clean:
                column_mapping[col] = 'valor'
            elif col == 'Desconto':
                column_mapping[col] = 'desconto'
            elif 'acr' in col_clean and 'scimo' in col_clean:
                column_mapping[col] = 'acrescimo'
            elif col == 'Total':
                column_mapping[col] = 'total'
            elif col == 'Vendedor':
                column_mapping[col] = 'cod_vendedor'
            elif col == 'Nome Vendedor':
                column_mapping[col] = 'nome_vendedor'
            elif 'ref' in col_clean and 'brica' in col_clean:
                column_mapping[col] = 'ref_fabrica'
            elif col == 'Cd' or col == 'Cód':
                column_mapping[col] = 'cod_parceiro'
            elif col == 'Parceiro':
                column_mapping[col] = 'parceiro'
            elif 'final' in col_clean:
                column_mapping[col] = 'preco_final'
            elif 'base' in col_clean:
                column_mapping[col] = 'preco_base'
            elif col == 'OBS':
                column_mapping[col] = 'obs'
            elif col == 'Marca':
                column_mapping[col] = 'marca'
        
        df.rename(columns=column_mapping, inplace=True)
        
        # Limpar tabela existente
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM vendas')
        
        # Inserir dados
        df.to_sql('vendas', conn, if_exists='append', index=False)
        
        print(f"OK: {len(df)} registros importados com sucesso!")
        
        # Atualizar métricas
        self.update_metrics()
        
        return True
    
    def update_metrics(self):
        """Atualiza todas as tabelas de métricas"""
        print("Atualizando métricas...")
        conn = self.connect()
        
        # Atualizar métricas de clientes
        self._update_cliente_metrics(conn)
        
        # Atualizar produtos por cliente
        self._update_cliente_produtos(conn)
        
        # Atualizar métricas de produtos
        self._update_produto_metrics(conn)
        
        conn.commit()
        print("OK: Metricas atualizadas!")
    
    def _update_cliente_metrics(self, conn):
        """Atualiza métricas agregadas de clientes"""
        cursor = conn.cursor()
        
        # Limpar tabela
        cursor.execute('DELETE FROM clientes_metricas')
        
        # Query para calcular métricas
        query = '''
        INSERT INTO clientes_metricas (
            parceiro, total_compras, qtd_compras, ticket_medio,
            primeira_compra, ultima_compra, dias_desde_ultima,
            total_produtos_unicos
        )
        SELECT 
            parceiro,
            SUM(total) as total_compras,
            COUNT(DISTINCT n_venda) as qtd_compras,
            AVG(total) as ticket_medio,
            MIN(data) as primeira_compra,
            MAX(data) as ultima_compra,
            CAST(julianday('now') - julianday(MAX(data)) as INTEGER) as dias_desde_ultima,
            COUNT(DISTINCT produto) as total_produtos_unicos
        FROM vendas
        WHERE parceiro IS NOT NULL AND parceiro != ''
        GROUP BY parceiro
        '''
        
        cursor.execute(query)
        
        # Atualizar segmentos
        self._update_segmentos(conn)
    
    def _update_segmentos(self, conn):
        """Classifica clientes em segmentos"""
        df = pd.read_sql('''
            SELECT parceiro, total_compras, qtd_compras, dias_desde_ultima
            FROM clientes_metricas
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
                UPDATE clientes_metricas 
                SET segmento = ? 
                WHERE parceiro = ?
            ''', (row['segmento'], row['parceiro']))
    
    def _update_cliente_produtos(self, conn):
        """Atualiza produtos comprados por cada cliente"""
        cursor = conn.cursor()
        
        # Limpar tabela
        cursor.execute('DELETE FROM cliente_produtos')
        
        # Query para agregar produtos por cliente
        query = '''
        INSERT INTO cliente_produtos (
            parceiro, produto, quantidade_total, valor_total,
            qtd_compras, primeira_compra, ultima_compra, dias_desde_ultima
        )
        SELECT 
            parceiro,
            produto,
            SUM(quantidade) as quantidade_total,
            SUM(total) as valor_total,
            COUNT(*) as qtd_compras,
            MIN(data) as primeira_compra,
            MAX(data) as ultima_compra,
            CAST(julianday('now') - julianday(MAX(data)) as INTEGER) as dias_desde_ultima
        FROM vendas
        WHERE parceiro IS NOT NULL AND parceiro != ''
            AND produto IS NOT NULL AND produto != ''
        GROUP BY parceiro, produto
        '''
        
        cursor.execute(query)
    
    def _update_produto_metrics(self, conn):
        """Atualiza métricas de produtos"""
        cursor = conn.cursor()
        
        # Limpar tabela
        cursor.execute('DELETE FROM produtos_metricas')
        
        # Query para calcular métricas
        query = '''
        INSERT INTO produtos_metricas (
            produto, quantidade_vendida, valor_total, qtd_vendas,
            clientes_unicos, ticket_medio, margem_media,
            primeira_venda, ultima_venda, dias_desde_ultima
        )
        SELECT 
            produto,
            SUM(quantidade) as quantidade_vendida,
            SUM(total) as valor_total,
            COUNT(*) as qtd_vendas,
            COUNT(DISTINCT parceiro) as clientes_unicos,
            AVG(total) as ticket_medio,
            AVG(CASE 
                WHEN preco_base > 0 THEN ((preco_final - preco_base) / preco_base * 100)
                ELSE 0 
            END) as margem_media,
            MIN(data) as primeira_venda,
            MAX(data) as ultima_venda,
            CAST(julianday('now') - julianday(MAX(data)) as INTEGER) as dias_desde_ultima
        FROM vendas
        WHERE produto IS NOT NULL AND produto != ''
        GROUP BY produto
        '''
        
        cursor.execute(query)
        
        # Calcular taxa de recompra
        df_recompra = pd.read_sql('''
            SELECT produto, parceiro, COUNT(*) as compras
            FROM vendas
            GROUP BY produto, parceiro
        ''', conn)
        
        taxa_recompra = df_recompra.groupby('produto')['compras'].apply(
            lambda x: (x > 1).mean() * 100
        ).reset_index()
        taxa_recompra.columns = ['produto', 'taxa_recompra']
        
        # Atualizar taxas de recompra
        for _, row in taxa_recompra.iterrows():
            cursor.execute('''
                UPDATE produtos_metricas 
                SET taxa_recompra = ? 
                WHERE produto = ?
            ''', (row['taxa_recompra'], row['produto']))
    
    def get_cliente_data(self, parceiro=None):
        """Retorna dados de clientes"""
        conn = self.connect()
        
        if parceiro:
            query = '''
                SELECT * FROM clientes_metricas
                WHERE parceiro = ?
            '''
            return pd.read_sql(query, conn, params=[parceiro])
        else:
            return pd.read_sql('SELECT * FROM clientes_metricas ORDER BY total_compras DESC', conn)
    
    def get_produtos_cliente(self, parceiro):
        """Retorna produtos comprados por um cliente"""
        conn = self.connect()
        query = '''
            SELECT * FROM cliente_produtos
            WHERE parceiro = ?
            ORDER BY valor_total DESC
        '''
        return pd.read_sql(query, conn, params=[parceiro])
    
    def get_produto_data(self):
        """Retorna dados de produtos"""
        conn = self.connect()
        return pd.read_sql('SELECT * FROM produtos_metricas ORDER BY valor_total DESC', conn)
    
    def get_vendas_raw(self):
        """Retorna dados brutos de vendas"""
        conn = self.connect()
        return pd.read_sql('SELECT * FROM vendas', conn)
    
    def close(self):
        """Fecha conexão com banco"""
        if self.conn:
            self.conn.close()
            self.conn = None