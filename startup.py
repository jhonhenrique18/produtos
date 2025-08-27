"""
Script de inicialização automática para garantir que o banco esteja configurado
Garante que os dados locais sejam copiados para Railway
"""
import os
import sys
import shutil
from pathlib import Path
from db_manager_v2 import DatabaseManager

def copy_local_database():
    """Copia banco de dados local se disponível e necessário"""
    source_files = ['database_production.db', 'database.db']

    for source_file in source_files:
        if os.path.exists(source_file):
            # Verificar se o arquivo tem dados
            file_size = os.path.getsize(source_file)
            if file_size > 1000:  # Arquivo com pelo menos 1KB de dados
                print(f"Encontrado banco local com dados: {source_file} ({file_size} bytes)")

                # Fazer backup do arquivo existente se houver
                target_file = 'database_production.db'  # Sempre usar production como padrão
                if os.path.exists(target_file) and target_file != source_file:
                    backup_file = f"{target_file}.backup"
                    shutil.copy2(target_file, backup_file)
                    print(f"Backup criado: {backup_file}")

                # Copiar o arquivo com dados
                if target_file != source_file:
                    shutil.copy2(source_file, target_file)
                    print(f"Banco copiado: {source_file} -> {target_file}")

                return target_file

    print("Nenhum banco de dados local encontrado com dados suficientes")
    return None

def setup_database():
    """Configura o banco de dados na inicialização"""

    print("=== INICIALIZANDO SISTEMA DE VENDAS ===")

    # Primeiro, tentar copiar dados locais se necessário
    copied_db = copy_local_database()

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

        if count > 0:
            # Verificar período dos dados
            cursor.execute("SELECT MIN(data), MAX(data) FROM vendas")
            min_data, max_data = cursor.fetchone()
            print(f"Período dos dados: {min_data} até {max_data}")

            # Verificar tabelas de métricas
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('clientes_metricas', 'produtos_metricas')")
            metrics_tables = cursor.fetchone()[0]
            print(f"Tabelas de métricas encontradas: {metrics_tables}")

            if metrics_tables < 2:
                print("Atualizando métricas...")
                db.update_metrics()
                print("Métricas atualizadas com sucesso!")
        else:
            print("AVISO: Banco de dados vazio. Os dados serão carregados do repositório.")

        db.close()
        return db_path

    except Exception as e:
        print(f"Erro ao verificar banco: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    db_path = setup_database()
    if db_path:
        print(f"\nBanco de dados pronto: {db_path}")
        print("Sistema pronto para uso!")
    else:
        print("\nERRO: Não foi possível configurar o banco de dados.")
        sys.exit(1)