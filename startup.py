"""
Script de inicialização automática para garantir que o banco esteja configurado
"""
import os
import sys
from pathlib import Path
from db_manager import DatabaseManager

def setup_database():
    """Configura o banco de dados na inicialização"""
    
    # Determinar qual banco usar
    db_files = ['database_production.db', 'database.db']
    db_path = None
    
    for db_file in db_files:
        if os.path.exists(db_file):
            db_path = db_file
            print(f"Usando banco de dados: {db_path}")
            break
    
    # Se não houver banco, criar um novo
    if not db_path:
        print("Nenhum banco encontrado. Criando novo...")
        db_path = 'database.db'
        db = DatabaseManager(db_path)
        
        # Procurar CSV para importar
        csv_files = [
            'ATACADO VENDAS PRODUTOS.csv',
            'data/ATACADO VENDAS PRODUTOS.csv',
            '../ATACADO VENDAS PRODUTOS.csv'
        ]
        
        for csv_file in csv_files:
            if os.path.exists(csv_file):
                print(f"Importando dados de {csv_file}...")
                try:
                    db.import_csv(csv_file)
                    print("Dados importados com sucesso!")
                    break
                except Exception as e:
                    print(f"Erro ao importar: {e}")
        
        db.close()
    
    # Verificar integridade do banco
    try:
        db = DatabaseManager(db_path)
        conn = db.connect()
        cursor = conn.cursor()
        
        # Verificar tabelas essenciais
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        required_tables = ['vendas', 'clientes_metricas', 'produtos_metricas']
        missing = [t for t in required_tables if t not in tables]
        
        if missing:
            print(f"Tabelas faltando: {missing}")
            print("Recriando estrutura do banco...")
            db.init_database()
            
            # Tentar reimportar dados se necessário
            cursor.execute("SELECT COUNT(*) FROM vendas")
            if cursor.fetchone()[0] == 0:
                for csv_file in csv_files:
                    if os.path.exists(csv_file):
                        print(f"Reimportando dados de {csv_file}...")
                        db.import_csv(csv_file)
                        break
        
        # Verificar se há dados
        cursor.execute("SELECT COUNT(*) FROM vendas")
        count = cursor.fetchone()[0]
        print(f"Total de registros no banco: {count}")
        
        if count == 0:
            print("AVISO: Banco de dados vazio. Faça upload de um CSV na interface.")
        
        db.close()
        return db_path
        
    except Exception as e:
        print(f"Erro ao verificar banco: {e}")
        return None

if __name__ == "__main__":
    db_path = setup_database()
    if db_path:
        print(f"\nBanco de dados pronto: {db_path}")
        print("Sistema pronto para uso!")
    else:
        print("\nERRO: Não foi possível configurar o banco de dados.")
        sys.exit(1)