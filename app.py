"""
CRM de Vendas - Dashboard Streamlit
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
from pathlib import Path

from db_manager_v2 import DatabaseManager
from analise_clientes import AnalisadorClientes
from analise_produtos_v2 import AnalisadorProdutos

# Configuração da página
st.set_page_config(
    page_title="CRM Vendas Atacado",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Inicializar banco de dados
@st.cache_resource
def init_database():
    import os

    # Garantir que o setup inicial seja executado se necessário
    if not os.path.exists('database_production.db') and not os.path.exists('database.db'):
        print("Executando setup inicial...")
        try:
            from startup import setup_database
            setup_database()
        except Exception as e:
            print(f"Erro no setup inicial: {e}")

    # Usar banco de produção
    if os.path.exists('database_production.db'):
        db_path = 'database_production.db'
    else:
        db_path = 'database.db'

    db = DatabaseManager(db_path)

    # Verificar se precisa importar dados iniciais
    conn = db.connect()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM vendas")
    count = cursor.fetchone()[0]

    print(f"Inicializando banco: {db_path} com {count} registros")

    # Sempre atualizar métricas se o banco existir e tiver dados
    if count > 0:
        # Verificar se as tabelas v2 existem e têm dados
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='clientes_metricas_v2'")
        if cursor.fetchone()[0] == 0:
            print("Criando tabelas de métricas v2...")
            db.update_metrics()  # Cria as tabelas v2 e popula
        else:
            cursor.execute("SELECT COUNT(*) FROM clientes_metricas_v2")
            if cursor.fetchone()[0] == 0:
                db.update_metrics()  # Popula as tabelas v2
    elif count == 0:
        # Importar CSV inicial se existir
        csv_path = Path("ATACADO VENDAS PRODUTOS.csv")
        if csv_path.exists():
            db.import_csv(str(csv_path))
    
    return db

# Inicializar analisadores
@st.cache_resource
def init_analyzers(_db):
    return AnalisadorClientes(_db), AnalisadorProdutos(_db)

# Função para aplicar estilos
def apply_custom_css():
    st.markdown("""
    <style>
        .main {
            padding: 0rem 1rem;
        }
        .stButton>button {
            background-color: #0066CC;
            color: white;
        }
        .metric-card {
            background-color: #f0f2f6;
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 0.5rem 0;
        }
        div[data-testid="metric-container"] {
            background-color: #f0f2f6;
            border: 1px solid #cccccc;
            padding: 15px;
            border-radius: 5px;
            margin: 10px 0;
        }
    </style>
    """, unsafe_allow_html=True)

# Main App
def main():
    apply_custom_css()
    
    # Inicializar componentes
    db = init_database()
    analisador_clientes, analisador_produtos = init_analyzers(db)
    
    # Sidebar
    st.sidebar.title("🎯 CRM Vendas Atacado")
    
    # Menu de navegação
    menu = st.sidebar.selectbox(
        "Navegação",
        ["📊 Dashboard Principal",
         "👥 Análise de Clientes", 
         "📦 Análise de Produtos",
         "🎯 Ações de Follow-up",
         "📈 Relatórios",
         "⚙️ Atualizar Dados"]
    )
    
    # Páginas
    if menu == "📊 Dashboard Principal":
        show_dashboard(db)
    
    elif menu == "👥 Análise de Clientes":
        show_analise_clientes(db, analisador_clientes)
    
    elif menu == "📦 Análise de Produtos":
        show_analise_produtos(db, analisador_produtos)
    
    elif menu == "🎯 Ações de Follow-up":
        show_acoes_followup(db, analisador_clientes)
    
    elif menu == "📈 Relatórios":
        show_relatorios(db, analisador_clientes, analisador_produtos)
    
    elif menu == "⚙️ Atualizar Dados":
        show_atualizar_dados(db)

def safe_float_format(value, default=0.0):
    """Formata valor float tratando None/NaN"""
    if value is None or pd.isna(value):
        return default
    return float(value)

def safe_int_format(value, default=0):
    """Formata valor int tratando None/NaN"""
    if value is None or pd.isna(value):
        return default
    return int(value)

def show_dashboard(db):
    """Mostra dashboard principal com KPIs"""
    st.title("📊 Dashboard Principal")

    # KPIs principais
    col1, col2, col3, col4 = st.columns(4)

    # Buscar métricas
    conn = db.connect()

    # Total de vendas
    total_vendas_result = pd.read_sql("SELECT SUM(total) as total FROM vendas", conn)
    total_vendas = safe_float_format(total_vendas_result['total'][0] if not total_vendas_result.empty else None)

    # Total de clientes
    total_clientes_result = pd.read_sql("SELECT COUNT(DISTINCT parceiro) as total FROM vendas", conn)
    total_clientes = safe_int_format(total_clientes_result['total'][0] if not total_clientes_result.empty else None)

    # Total de produtos
    total_produtos_result = pd.read_sql("SELECT COUNT(DISTINCT produto) as total FROM vendas", conn)
    total_produtos = safe_int_format(total_produtos_result['total'][0] if not total_produtos_result.empty else None)

    # Ticket médio
    ticket_medio_result = pd.read_sql("SELECT AVG(total) as media FROM vendas", conn)
    ticket_medio = safe_float_format(ticket_medio_result['media'][0] if not ticket_medio_result.empty else None)
    
    with col1:
        st.metric("💰 Faturamento Total", f"R$ {total_vendas:,.2f}")
    
    with col2:
        st.metric("👥 Total de Clientes", f"{total_clientes:,}")
    
    with col3:
        st.metric("📦 Total de Produtos", f"{total_produtos:,}")
    
    with col4:
        st.metric("🎯 Ticket Médio", f"R$ {ticket_medio:,.2f}")
    
    st.divider()
    
    # Gráficos
    col1, col2 = st.columns(2)
    
    with col1:
        # Evolução mensal
        vendas_mensais = pd.read_sql("""
            SELECT 
                strftime('%Y-%m', data) as mes,
                SUM(total) as valor
            FROM vendas
            GROUP BY mes
            ORDER BY mes
        """, conn)
        
        fig = px.line(vendas_mensais, x='mes', y='valor', 
                     title='Evolução Mensal de Vendas',
                     labels={'valor': 'Valor (R$)', 'mes': 'Mês'})
        fig.update_traces(mode='lines+markers')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Top clientes
        top_clientes = pd.read_sql("""
            SELECT parceiro, SUM(total) as valor
            FROM vendas
            GROUP BY parceiro
            ORDER BY valor DESC
            LIMIT 10
        """, conn)
        
        fig = px.bar(top_clientes, x='valor', y='parceiro',
                    title='Top 10 Clientes', orientation='h',
                    labels={'valor': 'Valor (R$)', 'parceiro': 'Cliente'})
        st.plotly_chart(fig, use_container_width=True)
    
    # Segmentação de clientes
    st.subheader("📊 Segmentação de Clientes")
    
    segmentos = pd.read_sql("""
        SELECT 
            segmento,
            COUNT(*) as quantidade,
            SUM(total_compras) as valor_total
        FROM clientes_metricas
        GROUP BY segmento
    """, conn)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.pie(segmentos, values='quantidade', names='segmento',
                    title='Distribuição por Segmento')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.bar(segmentos, x='segmento', y='valor_total',
                    title='Valor por Segmento',
                    labels={'valor_total': 'Valor Total (R$)'})
        st.plotly_chart(fig, use_container_width=True)

def show_analise_clientes(db, analisador):
    """Página de análise detalhada de clientes"""
    st.title("👥 Análise de Clientes")
    
    tab1, tab2, tab3 = st.tabs(["Lista Completa", "Análise Individual", "Segmentação"])
    
    with tab1:
        st.subheader("📋 Lista Completa de Clientes")
        
        # Filtros
        col1, col2, col3 = st.columns(3)
        
        clientes_df = db.get_cliente_data()
        
        with col1:
            segmento_filter = st.multiselect(
                "Filtrar por Segmento",
                options=clientes_df['segmento'].unique() if 'segmento' in clientes_df.columns else [],
                default=[]
            )
        
        with col2:
            min_valor = st.number_input(
                "Valor mínimo de compras",
                min_value=0.0,
                value=0.0,
                step=100.0
            )
        
        with col3:
            # Calcular max_value tratando NaN
            max_dias = safe_int_format(clientes_df['dias_desde_ultima'].max(), default=365)
            dias_filter = st.slider(
                "Dias desde última compra",
                min_value=0,
                max_value=max_dias,
                value=(0, max_dias)
            )
        
        # Aplicar filtros
        filtered_df = clientes_df.copy()
        
        if segmento_filter:
            filtered_df = filtered_df[filtered_df['segmento'].isin(segmento_filter)]
        
        filtered_df = filtered_df[filtered_df['total_compras'] >= min_valor]
        filtered_df = filtered_df[
            (filtered_df['dias_desde_ultima'] >= dias_filter[0]) &
            (filtered_df['dias_desde_ultima'] <= dias_filter[1])
        ]
        
        # Mostrar métricas
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total de Clientes", len(filtered_df))
        with col2:
            valor_total = safe_float_format(filtered_df['total_compras'].sum() if not filtered_df.empty else 0)
            st.metric("Valor Total", f"R$ {valor_total:,.2f}")
        with col3:
            ticket_medio_calc = safe_float_format(filtered_df['ticket_medio'].mean() if not filtered_df.empty else 0)
            st.metric("Ticket Médio", f"R$ {ticket_medio_calc:,.2f}")
        
        # Tabela de clientes - Mostrar código e nome
        if 'cod_parceiro' in filtered_df.columns:
            display_cols = ['cod_parceiro', 'parceiro', 'segmento', 'total_compras', 'qtd_compras', 
                          'ticket_medio', 'dias_desde_ultima', 'total_produtos_unicos']
        else:
            display_cols = ['parceiro', 'segmento', 'total_compras', 'qtd_compras', 
                          'ticket_medio', 'dias_desde_ultima', 'total_produtos_unicos']
        
        # Filtrar apenas colunas que existem
        existing_cols = [col for col in display_cols if col in filtered_df.columns]
        
        st.dataframe(
            filtered_df[existing_cols].round(2),
            use_container_width=True,
            hide_index=True
        )
        
        # Botão para exportar
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="📥 Baixar Lista em CSV",
            data=csv,
            file_name=f"clientes_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
    
    with tab2:
        st.subheader("🔍 Análise Individual do Cliente")
        
        # Seletor de cliente com código
        if 'cod_parceiro' in clientes_df.columns:
            # Criar opções com código e nome
            clientes_df['display_option'] = clientes_df['cod_parceiro'].astype(str) + ' - ' + clientes_df['parceiro'].astype(str)
            cliente_opcoes = clientes_df['display_option'].tolist()
            cliente_selecionado_display = st.selectbox(
                "Selecione o Cliente (Código - Nome)",
                options=cliente_opcoes
            )
            # Extrair o código do cliente selecionado
            if cliente_selecionado_display:
                cod_cliente = cliente_selecionado_display.split(' - ')[0]
                # Usar código para buscar análise
                cliente_selecionado = cod_cliente
        else:
            cliente_selecionado = st.selectbox(
                "Selecione o Cliente",
                options=clientes_df['parceiro'].tolist()
            )
        
        if cliente_selecionado:
            # Análise completa
            analise = analisador.get_analise_completa_cliente(cliente_selecionado)
            
            if analise:
                # Informações básicas
                col1, col2, col3, col4 = st.columns(4)
                
                info = analise['info_basica']
                
                with col1:
                    st.metric("Segmento", info['segmento'])
                with col2:
                    st.metric("Total Comprado", f"R$ {info['total_compras']:,.2f}")
                with col3:
                    st.metric("Qtd Compras", info['qtd_compras'])
                with col4:
                    st.metric("Dias desde última", info['dias_desde_ultima'])
                
                # Frequência de compra
                freq = analise['frequencia']
                if freq['frequencia_media_dias']:
                    st.info(f"📅 Frequência: compra a cada {freq['frequencia_media_dias']:.0f} dias | Status: {freq['status_frequencia']}")
                
                # Produtos comprados
                st.subheader("📦 Produtos Comprados")
                
                produtos_df = pd.DataFrame(analise['produtos_comprados'])
                if not produtos_df.empty:
                    # Ordenar por valor
                    produtos_df = produtos_df.sort_values('valor_total', ascending=False)
                    
                    # Adicionar análise de recompra
                    produtos_df['tipo'] = produtos_df['qtd_compras'].apply(
                        lambda x: 'Recorrente' if x > 1 else 'Única vez'
                    )
                    
                    st.dataframe(
                        produtos_df[['produto', 'quantidade_total', 'valor_total', 
                                    'qtd_compras', 'dias_desde_ultima', 'tipo']],
                        use_container_width=True,
                        hide_index=True
                    )
                
                # Categorias
                st.subheader("📊 Análise por Categorias")
                categorias = analise['categorias']
                if categorias:
                    cat_df = pd.DataFrame([
                        {'Categoria': k, 'Valor Total': v['valor_total'], 'Qtd Produtos': v['qtd_produtos']}
                        for k, v in categorias.items()
                    ]).sort_values('Valor Total', ascending=False)
                    
                    fig = px.pie(cat_df, values='Valor Total', names='Categoria',
                                title='Distribuição de Compras por Categoria')
                    st.plotly_chart(fig, use_container_width=True)
                
                # Recomendações
                st.subheader("💡 Recomendações de Ação")
                recomendacoes = analise['recomendacoes']
                
                for rec in recomendacoes:
                    if rec['urgencia'] == 'Alta':
                        st.error(f"🚨 **{rec['tipo']}**: {rec['acao']} - {rec['motivo']}")
                    elif rec['urgencia'] == 'Média':
                        st.warning(f"⚠️ **{rec['tipo']}**: {rec['acao']} - {rec['motivo']}")
                    else:
                        st.info(f"ℹ️ **{rec['tipo']}**: {rec['acao']} - {rec['motivo']}")
                
                # Script de abordagem
                with st.expander("📝 Ver Script de Abordagem"):
                    script = analisador.gerar_script_abordagem(cliente_selecionado)
                    st.code(script, language=None)
                
                # Produtos não comprados (oportunidades)
                st.subheader("🎯 Oportunidades de Cross-sell")
                produtos_nao_comprados = analise['produtos_nao_comprados'][:20]  # Top 20
                
                if produtos_nao_comprados:
                    oport_df = pd.DataFrame(produtos_nao_comprados)
                        # Mostrar código do produto se disponível
                    if 'cod_produto' in oport_df.columns:
                        display_cols = ['cod_produto', 'produto', 'clientes_unicos', 'taxa_recompra', 'score_recomendacao']
                    else:
                        display_cols = ['produto', 'clientes_unicos', 'taxa_recompra', 'score_recomendacao']
                    
                    available_cols = [col for col in display_cols if col in oport_df.columns]
                    st.dataframe(
                        oport_df[available_cols].round(2),
                        use_container_width=True,
                        hide_index=True
                    )
    
    with tab3:
        st.subheader("📊 Análise de Segmentação")
        
        # Estatísticas por segmento
        segmento_stats = clientes_df.groupby('segmento').agg({
            'parceiro': 'count',
            'total_compras': 'sum',
            'ticket_medio': 'mean',
            'qtd_compras': 'mean',
            'dias_desde_ultima': 'mean'
        }).round(2)
        
        segmento_stats.columns = ['Qtd Clientes', 'Valor Total', 'Ticket Médio', 
                                  'Média Compras', 'Média Dias']
        
        st.dataframe(segmento_stats, use_container_width=True)
        
        # Gráfico de distribuição
        fig = px.treemap(
            clientes_df,
            path=['segmento', 'parceiro'],
            values='total_compras',
            title='Mapa de Clientes por Segmento e Valor'
        )
        st.plotly_chart(fig, use_container_width=True)

def show_analise_produtos(db, analisador):
    """Página de análise detalhada de produtos"""
    st.title("📦 Análise de Produtos")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Lista Completa", "Análise Individual", 
                                       "Mix de Produtos", "Produtos Problemáticos"])
    
    with tab1:
        st.subheader("📋 Todos os Produtos")
        
        # Buscar todos os produtos
        produtos_df = analisador.get_todos_produtos_analise()
        
        # Filtros
        col1, col2, col3 = st.columns(3)
        
        # Verificar se produtos_df tem dados e colunas necessárias
        if produtos_df.empty:
            st.warning("Nenhum produto encontrado no banco de dados.")
            return
        
        with col1:
            # Verificar se a coluna categoria existe
            if 'categoria' in produtos_df.columns:
                categoria_filter = st.multiselect(
                    "Filtrar por Categoria",
                    options=produtos_df['categoria'].unique(),
                    default=[]
                )
            else:
                categoria_filter = []
                st.info("Categorias não disponíveis")
        
        with col2:
            # Classificação ABC sempre disponível
            class_abc_filter = st.multiselect(
                "Classificação ABC",
                options=['A', 'B', 'C'],
                default=[]
            )
        
        with col3:
            min_vendas = st.number_input(
                "Mínimo de vendas",
                min_value=0,
                value=0
            )
        
        # Aplicar filtros
        filtered_df = produtos_df.copy()
        
        if categoria_filter and 'categoria' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['categoria'].isin(categoria_filter)]
        
        if class_abc_filter and 'classificacao_abc' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['classificacao_abc'].isin(class_abc_filter)]
        
        if 'total_vendas' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['total_vendas'] >= min_vendas]
        
        # Métricas
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total de Produtos", len(filtered_df))
        with col2:
            valor_sum = filtered_df['valor_total'].sum() if not filtered_df.empty else 0
            st.metric("Valor Total", f"R$ {valor_sum:,.2f}")
        with col3:
            taxa_mean = filtered_df['taxa_recompra'].mean() if not filtered_df.empty and 'taxa_recompra' in filtered_df.columns else 0
            st.metric("Taxa Recompra Média", f"{taxa_mean:.1f}%")
        with col4:
            margem_mean = filtered_df['margem_media'].mean() if not filtered_df.empty and 'margem_media' in filtered_df.columns else 0
            st.metric("Margem Média", f"{margem_mean:.1f}%")
        
        # Tabela - Incluir código do produto
        if 'cod_produto' in filtered_df.columns:
            display_cols = ['cod_produto', 'produto', 'categoria', 'classificacao_abc', 'valor_total', 
                          'quantidade_vendida', 'clientes_unicos', 'taxa_recompra', 
                          'dias_desde_ultima', 'score_performance']
        else:
            display_cols = ['produto', 'categoria', 'classificacao_abc', 'valor_total', 
                          'quantidade_vendida', 'clientes_unicos', 'taxa_recompra', 
                          'dias_desde_ultima', 'score_performance']
        
        # Verificar quais colunas existem
        available_cols = [col for col in display_cols if col in filtered_df.columns]
        
        if available_cols and not filtered_df.empty:
            st.dataframe(
                filtered_df[available_cols].round(2),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("Nenhum produto encontrado com os filtros selecionados.")
        
        # Download
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="📥 Baixar Lista Completa",
            data=csv,
            file_name=f"produtos_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
    
    with tab2:
        st.subheader("🔍 Análise Individual do Produto")
        
        # Seletor com código
        if 'cod_produto' in produtos_df.columns:
            # Criar opções com código e nome
            produtos_df['display_option'] = produtos_df['cod_produto'].astype(str) + ' - ' + produtos_df['produto'].astype(str)
            produto_opcoes = produtos_df['display_option'].tolist()
            produto_selecionado_display = st.selectbox(
                "Selecione o Produto (Código - Nome)",
                options=produto_opcoes
            )
            # Extrair o código do produto selecionado
            if produto_selecionado_display:
                cod_produto = produto_selecionado_display.split(' - ')[0]
                produto_selecionado = cod_produto
        else:
            produto_selecionado = st.selectbox(
                "Selecione o Produto",
                options=produtos_df['produto'].tolist()
            )
        
        if produto_selecionado:
            analise = analisador.get_analise_completa_produto(produto_selecionado)
            
            if analise:
                # Métricas principais
                col1, col2, col3, col4 = st.columns(4)
                
                metricas = analise['metricas']
                
                with col1:
                    valor_total_prod = safe_float_format(metricas.get('valor_total', 0))
                    st.metric("Valor Total", f"R$ {valor_total_prod:,.2f}")
                with col2:
                    qtd_vendida = safe_int_format(metricas.get('quantidade_vendida', 0))
                    st.metric("Qtd Vendida", f"{qtd_vendida:,.0f}")
                with col3:
                    clientes_unicos = safe_int_format(metricas.get('clientes_unicos', 0))
                    st.metric("Clientes Únicos", clientes_unicos)
                with col4:
                    taxa_recompra = safe_float_format(metricas.get('taxa_recompra', 0))
                    st.metric("Taxa Recompra", f"{taxa_recompra:.1f}%")
                
                # Evolução temporal
                st.subheader("📈 Evolução de Vendas")
                evolucao_df = pd.DataFrame(analise['evolucao'])
                if not evolucao_df.empty:
                    fig = px.line(evolucao_df, x='mes', y='valor_total',
                                 title='Vendas Mensais',
                                 labels={'valor_total': 'Valor (R$)', 'mes': 'Mês'})
                    fig.update_traces(mode='lines+markers')
                    st.plotly_chart(fig, use_container_width=True)
                
                # Clientes que compraram
                st.subheader("👥 Clientes")
                clientes_df = pd.DataFrame(analise['clientes'])
                if not clientes_df.empty:
                    st.dataframe(
                        clientes_df[['parceiro', 'qtd_total', 'valor_total', 
                                    'frequencia', 'ultima_compra']].round(2),
                        use_container_width=True,
                        hide_index=True
                    )
                
                # Produtos complementares
                st.subheader("🔗 Produtos Complementares")
                complementares = analise['complementares']
                if complementares:
                    comp_df = pd.DataFrame(complementares)
                    st.dataframe(
                        comp_df[['produto', 'freq_conjunta', 'confianca']],
                        use_container_width=True,
                        hide_index=True
                    )
                
                # Sazonalidade
                if analise['sazonalidade']:
                    st.subheader("📅 Análise de Sazonalidade")
                    sazon_df = pd.DataFrame(analise['sazonalidade'])
                    
                    fig = px.bar(sazon_df, x='mes_nome', y='valor',
                               color='tipo', title='Padrão Sazonal',
                               labels={'valor': 'Valor (R$)', 'mes_nome': 'Mês'})
                    st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.subheader("📊 Análise do Mix de Produtos")
        
        mix_categorias = analisador.analisar_mix_produtos()
        
        # Gráfico de distribuição
        mix_df = mix_categorias.reset_index()
        mix_df.rename(columns={'index': 'categoria'}, inplace=True)
        fig = px.pie(mix_df, 
                    values='valor_total', 
                    names='categoria',
                    title='Distribuição de Faturamento por Categoria')
        st.plotly_chart(fig, use_container_width=True)
        
        # Tabela detalhada
        st.dataframe(mix_categorias, use_container_width=True)
        
        # Análise de potencial
        st.subheader("💡 Categorias com Potencial")
        potencial_df = mix_categorias.sort_values('potencial', ascending=False)
        
        for idx, row in potencial_df.head(5).iterrows():
            st.info(f"**{idx}**: Potencial {row['potencial']:.1f} | "
                   f"Taxa Recompra: {row['taxa_recompra_media']:.1f}% | "
                   f"Produtos: {row['qtd_produtos']:.0f}")
    
    with tab4:
        st.subheader("⚠️ Produtos que Precisam Atenção")
        
        problemas = analisador.get_produtos_para_acao()
        
        # Sem venda recente
        st.warning("📉 Produtos Sem Venda Recente")
        if problemas['sem_venda_recente']:
            sem_venda_df = pd.DataFrame(problemas['sem_venda_recente'])
            st.dataframe(
                sem_venda_df[['produto', 'valor_total', 'dias_desde_ultima']],
                use_container_width=True,
                hide_index=True
            )
        
        # Baixa taxa de recompra
        st.warning("🔄 Produtos com Baixa Taxa de Recompra")
        if problemas['baixa_recompra']:
            baixa_recompra_df = pd.DataFrame(problemas['baixa_recompra'])
            st.dataframe(
                baixa_recompra_df[['produto', 'taxa_recompra', 'clientes_unicos']],
                use_container_width=True,
                hide_index=True
            )
        
        # Margem baixa
        st.warning("💰 Produtos com Margem Baixa")
        if problemas['margem_baixa']:
            margem_df = pd.DataFrame(problemas['margem_baixa'])
            st.dataframe(
                margem_df[['produto', 'valor_total', 'margem_media']],
                use_container_width=True,
                hide_index=True
            )

def show_acoes_followup(db, analisador):
    """Página de ações de follow-up"""
    st.title("🎯 Ações de Follow-up")
    
    # Buscar clientes para ação
    acoes = analisador.get_clientes_para_acao()
    
    tab1, tab2, tab3 = st.tabs(["Clientes em Risco", "Reativação", "Cross-sell"])
    
    with tab1:
        st.subheader("🚨 Clientes em Risco - Ação Imediata")
        
        if acoes['em_risco']:
            em_risco_df = pd.DataFrame(acoes['em_risco'])
            
            st.error(f"⚠️ {len(em_risco_df)} clientes precisam de contato urgente!")
            
            # Para cada cliente em risco
            for _, cliente in em_risco_df.head(10).iterrows():
                with st.expander(f"📞 {cliente['parceiro']} - {cliente['segmento']}"):
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("Total Comprado", f"R$ {cliente['total_compras']:,.2f}")
                    with col2:
                        st.metric("Qtd Compras", cliente['qtd_compras'])
                    with col3:
                        st.metric("Dias sem comprar", cliente['dias_desde_ultima'])
                    
                    # Gerar script
                    script = analisador.gerar_script_abordagem(cliente['parceiro'])
                    st.code(script, language=None)
            
            # Download lista completa
            csv = em_risco_df.to_csv(index=False)
            st.download_button(
                label="📥 Baixar Lista Completa de Clientes em Risco",
                data=csv,
                file_name=f"clientes_em_risco_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
    
    with tab2:
        st.subheader("♻️ Clientes para Reativação")
        
        if acoes['para_reativar']:
            reativar_df = pd.DataFrame(acoes['para_reativar'])
            
            st.warning(f"📞 {len(reativar_df)} clientes inativos com potencial de reativação")
            
            # Filtro por valor
            min_valor_reativ = st.slider(
                "Filtrar por valor mínimo de compras anteriores",
                min_value=0,
                max_value=int(reativar_df['total_compras'].max()),
                value=1000
            )
            
            filtered_reativ = reativar_df[reativar_df['total_compras'] >= min_valor_reativ]
            
            st.dataframe(
                filtered_reativ[['parceiro', 'total_compras', 'dias_desde_ultima']],
                use_container_width=True,
                hide_index=True
            )
            
            # Sugestão de campanha
            st.info("""
            💡 **Sugestão de Campanha de Reativação:**
            - Desconto de 15% para voltar a comprar
            - Frete grátis na primeira compra
            - Brinde especial para pedidos acima de R$ 500
            """)
    
    with tab3:
        st.subheader("🎯 Oportunidades de Cross-sell")
        
        if acoes['cross_sell']:
            cross_df = pd.DataFrame(acoes['cross_sell'])
            
            st.success(f"✨ {len(cross_df)} clientes com potencial de cross-sell")
            
            # Mostrar oportunidades
            for _, cliente in cross_df.head(10).iterrows():
                with st.expander(f"💼 {cliente['parceiro']}"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.metric("Produtos únicos comprados", cliente['total_produtos_unicos'])
                        st.metric("Total comprado", f"R$ {cliente['total_compras']:,.2f}")
                    
                    with col2:
                        # Buscar produtos não comprados
                        produtos_nao = analisador.get_produtos_nao_comprados(cliente['parceiro'])[:5]
                        
                        st.write("**Produtos para oferecer:**")
                        for prod in produtos_nao:
                            st.write(f"• {prod['produto']}")

def show_relatorios(db, analisador_clientes, analisador_produtos):
    """Página de relatórios executivos"""
    st.title("📈 Relatórios Executivos")
    
    tab1, tab2 = st.tabs(["Relatório de Clientes", "Relatório de Produtos"])
    
    with tab1:
        st.subheader("📊 Relatório Executivo - Clientes")
        
        # KPIs
        conn = db.connect()
        
        kpis_result = pd.read_sql("""
            SELECT
                COUNT(*) as total_clientes,
                SUM(total_compras) as faturamento_total,
                AVG(ticket_medio) as ticket_medio_geral,
                AVG(dias_desde_ultima) as media_dias_inativos
            FROM clientes_metricas
        """, conn)

        if kpis_result.empty:
            st.error("Não há dados suficientes para gerar o relatório.")
            return

        kpis = kpis_result.iloc[0]

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            total_clientes = safe_int_format(kpis['total_clientes'])
            st.metric("Total Clientes", f"{total_clientes:,.0f}")
        with col2:
            faturamento_total = safe_float_format(kpis['faturamento_total'])
            st.metric("Faturamento Total", f"R$ {faturamento_total:,.2f}")
        with col3:
            ticket_medio = safe_float_format(kpis['ticket_medio_geral'])
            st.metric("Ticket Médio", f"R$ {ticket_medio:,.2f}")
        with col4:
            media_dias = safe_float_format(kpis['media_dias_inativos'])
            st.metric("Média Dias Inativos", f"{media_dias:.0f}")
        
        # Distribuição de segmentos
        segmentos_df = pd.read_sql("""
            SELECT 
                segmento,
                COUNT(*) as quantidade,
                SUM(total_compras) as valor,
                AVG(ticket_medio) as ticket_medio
            FROM clientes_metricas
            GROUP BY segmento
        """, conn)
        
        fig = px.sunburst(
            segmentos_df,
            path=['segmento'],
            values='valor',
            title='Distribuição de Valor por Segmento'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Análise de retenção
        st.subheader("📈 Análise de Retenção")
        
        retencao = pd.read_sql("""
            SELECT 
                CASE 
                    WHEN dias_desde_ultima <= 30 THEN 'Ativo (0-30 dias)'
                    WHEN dias_desde_ultima <= 60 THEN 'Em Alerta (31-60 dias)'
                    WHEN dias_desde_ultima <= 90 THEN 'Em Risco (61-90 dias)'
                    ELSE 'Inativo (>90 dias)'
                END as status,
                COUNT(*) as quantidade,
                SUM(total_compras) as valor_total
            FROM clientes_metricas
            GROUP BY status
        """, conn)
        
        fig = px.bar(retencao, x='status', y='quantidade',
                    title='Status de Atividade dos Clientes',
                    labels={'quantidade': 'Quantidade de Clientes'})
        st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("📊 Relatório Executivo - Produtos")
        
        relatorio = analisador_produtos.get_relatorio_executivo_produtos()
        
        # KPIs
        kpis = relatorio['kpis']
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Produtos", f"{kpis.get('total_produtos', 0):,.0f}")
        with col2:
            valor_total = kpis.get('faturamento_total', 0) or 0
            st.metric("Faturamento Total", f"R$ {valor_total:,.2f}")
        with col3:
            taxa_recompra = kpis.get('taxa_recompra_media', 0) or 0
            st.metric("Taxa Recompra Média", f"{taxa_recompra:.1f}%")
        with col4:
            margem = kpis.get('margem_media_geral', 0) or 0
            st.metric("Margem Média", f"{margem:.1f}%")
        
        # Top produtos
        st.subheader("🏆 Top 5 Produtos")
        if relatorio['top_produtos']:
            top_df = pd.DataFrame(relatorio['top_produtos'])
            if not top_df.empty and 'produto' in top_df.columns:
                fig = px.bar(top_df, x='produto', y='valor_total',
                            title='Produtos com Maior Faturamento',
                            labels={'valor_total': 'Valor Total (R$)'})
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Dados de produtos sendo processados...")
        
        # Produtos problemáticos
        if relatorio.get('produtos_problematicos'):
            st.warning("⚠️ Produtos Sem Venda Recente (>60 dias)")
            prob_df = pd.DataFrame(relatorio['produtos_problematicos'])
            if not prob_df.empty:
                st.dataframe(prob_df, use_container_width=True, hide_index=True)

def show_atualizar_dados(db):
    """Página para atualizar dados do banco"""
    st.title("⚙️ Atualizar Dados")
    
    st.info("📤 Faça upload de um novo arquivo CSV para atualizar os dados")
    
    # Upload de arquivo
    uploaded_file = st.file_uploader(
        "Escolha o arquivo CSV",
        type=['csv'],
        help="O arquivo deve estar no mesmo formato do original"
    )
    
    if uploaded_file is not None:
        # Salvar arquivo temporário
        temp_path = Path("data/uploads") / uploaded_file.name
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(temp_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        # Preview dos dados
        st.subheader("📋 Preview dos Dados")
        df_preview = pd.read_csv(temp_path, encoding='latin-1', sep=';', nrows=10)
        st.dataframe(df_preview, use_container_width=True)
        
        # Botão para confirmar importação
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("✅ Confirmar e Importar", type="primary"):
                with st.spinner("Importando dados..."):
                    try:
                        # Importar para o banco
                        db.import_csv(str(temp_path))
                        st.success("✅ Dados importados com sucesso!")
                        
                        # Limpar cache
                        st.cache_resource.clear()
                        st.cache_data.clear()
                        
                        # Recarregar página
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"❌ Erro ao importar: {str(e)}")
        
        with col2:
            if st.button("❌ Cancelar"):
                # Remover arquivo temporário
                temp_path.unlink()
                st.info("Importação cancelada")
    
    # Informações do banco atual
    st.divider()
    st.subheader("📊 Informações do Banco Atual")
    
    conn = db.connect()
    
    info_result = pd.read_sql("""
        SELECT
            (SELECT COUNT(*) FROM vendas) as total_vendas,
            (SELECT COUNT(DISTINCT parceiro) FROM vendas) as total_clientes,
            (SELECT COUNT(DISTINCT produto) FROM vendas) as total_produtos,
            (SELECT MIN(data) FROM vendas) as primeira_venda,
            (SELECT MAX(data) FROM vendas) as ultima_venda
    """, conn)

    if info_result.empty:
        st.error("Não há dados no banco de dados.")
        return

    info = info_result.iloc[0]

    col1, col2, col3 = st.columns(3)

    with col1:
        total_vendas = safe_int_format(info['total_vendas'])
        st.metric("Total de Vendas", f"{total_vendas:,}")
    with col2:
        total_clientes = safe_int_format(info['total_clientes'])
        st.metric("Total de Clientes", f"{total_clientes:,}")
    with col3:
        total_produtos = safe_int_format(info['total_produtos'])
        st.metric("Total de Produtos", f"{total_produtos:,}")

    # Verificar se há datas válidas
    primeira_venda = info['primeira_venda'] if info['primeira_venda'] is not None else "N/A"
    ultima_venda = info['ultima_venda'] if info['ultima_venda'] is not None else "N/A"
    st.info(f"📅 Período: {primeira_venda} até {ultima_venda}")

if __name__ == "__main__":
    main()