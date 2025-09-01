"""
Módulo de Geração de Relatórios - Versão PySpark
Responsável por criar relatórios visuais e em PDF dos dados analisados usando Apache Spark
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from fpdf import FPDF
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração do matplotlib para português
plt.rcParams['font.size'] = 10
plt.rcParams['figure.figsize'] = (12, 8)

class SparkReportGenerator:
    """Classe para geração de relatórios usando PySpark"""
    
    def __init__(self, spark: SparkSession, output_dir: str = "reports/"):
        """
        Inicializa o gerador de relatórios Spark
        
        Args:
            spark (SparkSession): Sessão Spark
            output_dir (str): Diretório para salvar relatórios
        """
        self.spark = spark
        self.output_dir = output_dir
        self.ensure_output_directory()
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def ensure_output_directory(self):
        """Garante que o diretório de saída existe"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Diretório de relatórios criado: {self.output_dir}")
    
    def create_sales_summary_report(self, df: DataFrame) -> Dict:
        """
        Cria relatório resumo de vendas
        
        Args:
            df (DataFrame): DataFrame com dados de vendas
            
        Returns:
            Dict: Dicionário com métricas do relatório
        """
        report = {}
        
        # Cache do DataFrame para múltiplas operações
        df.cache()
        
        try:
            # Métricas básicas
            if 'total' in df.columns:
                total_sales = df.select(sum(col('total'))).collect()[0][0]
                avg_transaction = df.select(avg(col('total'))).collect()[0][0]
                report['total_sales'] = float(total_sales) if total_sales else 0
                report['average_transaction'] = float(avg_transaction) if avg_transaction else 0
            
            report['total_transactions'] = df.count()
            
            if 'product' in df.columns:
                report['unique_products'] = df.select('product').distinct().count()
            
            # Métricas por período
            if 'date' in df.columns:
                date_range = df.select(
                    min(col('date')).alias('min_date'),
                    max(col('date')).alias('max_date')
                ).collect()[0]
                
                report['date_range'] = {
                    'start': date_range['min_date'].strftime('%Y-%m-%d') if date_range['min_date'] else None,
                    'end': date_range['max_date'].strftime('%Y-%m-%d') if date_range['max_date'] else None
                }
                
                # Vendas por mês
                monthly_sales = df.groupBy(
                    year(col('date')).alias('year'),
                    month(col('date')).alias('month')
                ).agg(sum(col('total')).alias('total_sales')).orderBy('year', 'month')
                
                monthly_data = monthly_sales.collect()
                report['monthly_sales'] = {
                    f"{row['year']}-{row['month']:02d}": float(row['total_sales']) 
                    for row in monthly_data
                }
            
            # Top produtos
            if 'product' in df.columns and 'total' in df.columns:
                top_products = df.groupBy('product').agg(
                    sum(col('total')).alias('total_sales')
                ).orderBy(desc('total_sales')).limit(10)
                
                top_products_data = top_products.collect()
                report['top_products'] = {
                    row['product']: float(row['total_sales']) 
                    for row in top_products_data
                }
            
            # Estatísticas por categoria de vendas
            if 'sales_category' in df.columns:
                category_stats = df.groupBy('sales_category').agg(
                    count('*').alias('count'),
                    sum(col('total')).alias('total_sales'),
                    avg(col('total')).alias('avg_sales')
                ).collect()
                
                report['category_stats'] = {
                    row['sales_category']: {
                        'count': row['count'],
                        'total_sales': float(row['total_sales']),
                        'avg_sales': float(row['avg_sales'])
                    }
                    for row in category_stats
                }
            
            logger.info("Relatório resumo de vendas criado")
            return report
            
        finally:
            df.unpersist()
    
    def create_visualizations(self, df: DataFrame, report_data: Dict) -> List[str]:
        """
        Cria visualizações dos dados
        
        Args:
            df (DataFrame): DataFrame com dados
            report_data (Dict): Dados do relatório
            
        Returns:
            List[str]: Lista de caminhos dos arquivos de imagem criados
        """
        image_paths = []
        
        # Converte DataFrame para Pandas para visualização
        # (Para datasets grandes, considere usar amostragem)
        sample_size = min(10000, df.count())
        df_pandas = df.sample(fraction=sample_size/df.count(), seed=42).toPandas()
        
        # 1. Gráfico de vendas ao longo do tempo
        if 'date' in df_pandas.columns:
            plt.figure(figsize=(12, 6))
            daily_sales = df_pandas.groupby(pd.to_datetime(df_pandas['date']).dt.date)['total'].sum()
            plt.plot(daily_sales.index, daily_sales.values, linewidth=2)
            plt.title('Vendas Diárias', fontsize=14, fontweight='bold')
            plt.xlabel('Data')
            plt.ylabel('Total de Vendas')
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            
            time_plot_path = os.path.join(self.output_dir, f'sales_timeline_{self.timestamp}.png')
            plt.savefig(time_plot_path, dpi=300, bbox_inches='tight')
            plt.close()
            image_paths.append(time_plot_path)
        
        # 2. Gráfico de top produtos
        if 'top_products' in report_data:
            plt.figure(figsize=(12, 8))
            top_products = list(report_data['top_products'].keys())[:10]
            top_values = list(report_data['top_products'].values())[:10]
            
            bars = plt.bar(range(len(top_products)), top_values, color='skyblue', alpha=0.7)
            plt.title('Top 10 Produtos por Vendas', fontsize=14, fontweight='bold')
            plt.xlabel('Produtos')
            plt.ylabel('Total de Vendas')
            plt.xticks(range(len(top_products)), top_products, rotation=45, ha='right')
            
            # Adiciona valores nas barras
            for bar, value in zip(bars, top_values):
                plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(top_values)*0.01,
                        f'{value:.0f}', ha='center', va='bottom')
            
            plt.grid(True, alpha=0.3, axis='y')
            plt.tight_layout()
            
            products_plot_path = os.path.join(self.output_dir, f'top_products_{self.timestamp}.png')
            plt.savefig(products_plot_path, dpi=300, bbox_inches='tight')
            plt.close()
            image_paths.append(products_plot_path)
        
        # 3. Distribuição de vendas
        if 'total' in df_pandas.columns:
            plt.figure(figsize=(10, 6))
            plt.hist(df_pandas['total'], bins=30, alpha=0.7, color='lightgreen', edgecolor='black')
            plt.title('Distribuição de Valores de Vendas', fontsize=14, fontweight='bold')
            plt.xlabel('Valor da Venda')
            plt.ylabel('Frequência')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            
            distribution_plot_path = os.path.join(self.output_dir, f'sales_distribution_{self.timestamp}.png')
            plt.savefig(distribution_plot_path, dpi=300, bbox_inches='tight')
            plt.close()
            image_paths.append(distribution_plot_path)
        
        # 4. Heatmap de vendas por produto e mês
        if 'product' in df_pandas.columns and 'date' in df_pandas.columns:
            df_pandas['month'] = pd.to_datetime(df_pandas['date']).dt.to_period('M')
            pivot_data = df_pandas.pivot_table(values='total', index='product', columns='month', aggfunc='sum', fill_value=0)
            
            plt.figure(figsize=(12, 8))
            sns.heatmap(pivot_data, annot=True, fmt='.0f', cmap='YlOrRd', cbar_kws={'label': 'Vendas'})
            plt.title('Heatmap de Vendas por Produto e Mês', fontsize=14, fontweight='bold')
            plt.xlabel('Mês')
            plt.ylabel('Produto')
            plt.tight_layout()
            
            heatmap_path = os.path.join(self.output_dir, f'sales_heatmap_{self.timestamp}.png')
            plt.savefig(heatmap_path, dpi=300, bbox_inches='tight')
            plt.close()
            image_paths.append(heatmap_path)
        
        # 5. Gráfico de vendas por categoria
        if 'sales_category' in df_pandas.columns:
            plt.figure(figsize=(10, 6))
            category_sales = df_pandas.groupby('sales_category')['total'].sum().sort_values(ascending=False)
            
            colors = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99']
            plt.pie(category_sales.values, labels=category_sales.index, autopct='%1.1f%%', colors=colors)
            plt.title('Distribuição de Vendas por Categoria', fontsize=14, fontweight='bold')
            plt.tight_layout()
            
            category_plot_path = os.path.join(self.output_dir, f'sales_category_{self.timestamp}.png')
            plt.savefig(category_plot_path, dpi=300, bbox_inches='tight')
            plt.close()
            image_paths.append(category_plot_path)
        
        logger.info(f"Criadas {len(image_paths)} visualizações")
        return image_paths
    
    def create_pdf_report(self, report_data: Dict, image_paths: List[str], 
                         title: str = "Relatório de Vendas - Spark") -> str:
        """
        Cria relatório em PDF
        
        Args:
            report_data (Dict): Dados do relatório
            image_paths (List[str]): Caminhos das imagens
            title (str): Título do relatório
            
        Returns:
            str: Caminho do arquivo PDF criado
        """
        pdf = FPDF()
        pdf.add_page()
        pdf.set_auto_page_break(auto=True, margin=15)
        
        # Título
        pdf.set_font('Arial', 'B', 16)
        pdf.cell(0, 10, title, 0, 1, 'C')
        pdf.ln(10)
        
        # Data de geração
        pdf.set_font('Arial', '', 10)
        pdf.cell(0, 10, f'Gerado em: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}', 0, 1, 'C')
        pdf.ln(10)
        
        # Resumo executivo
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 10, 'Resumo Executivo', 0, 1)
        pdf.set_font('Arial', '', 10)
        
        summary_text = f"""
        Total de Vendas: R$ {report_data.get('total_sales', 0):,.2f}
        Total de Transações: {report_data.get('total_transactions', 0):,}
        Ticket Médio: R$ {report_data.get('average_transaction', 0):,.2f}
        Produtos Únicos: {report_data.get('unique_products', 0):,}
        """
        
        for line in summary_text.strip().split('\n'):
            pdf.cell(0, 6, line.strip(), 0, 1)
        
        pdf.ln(10)
        
        # Estatísticas por categoria
        if 'category_stats' in report_data:
            pdf.set_font('Arial', 'B', 12)
            pdf.cell(0, 10, 'Estatísticas por Categoria', 0, 1)
            pdf.set_font('Arial', '', 10)
            
            for category, stats in report_data['category_stats'].items():
                category_text = f"""
                {category}:
                  - Transações: {stats['count']:,}
                  - Total: R$ {stats['total_sales']:,.2f}
                  - Média: R$ {stats['avg_sales']:,.2f}
                """
                for line in category_text.strip().split('\n'):
                    pdf.cell(0, 6, line.strip(), 0, 1)
                pdf.ln(2)
        
        # Adiciona imagens
        for i, image_path in enumerate(image_paths):
            if os.path.exists(image_path):
                pdf.add_page()
                pdf.set_font('Arial', 'B', 12)
                pdf.cell(0, 10, f'Gráfico {i+1}', 0, 1)
                pdf.image(image_path, x=10, y=30, w=190)
        
        # Salva o PDF
        pdf_path = os.path.join(self.output_dir, f'spark_report_{self.timestamp}.pdf')
        pdf.output(pdf_path)
        
        logger.info(f"Relatório PDF criado: {pdf_path}")
        return pdf_path
    
    def create_excel_report(self, df: DataFrame, report_data: Dict) -> str:
        """
        Cria relatório em Excel
        
        Args:
            df (DataFrame): DataFrame com dados
            report_data (Dict): Dados do relatório
            
        Returns:
            str: Caminho do arquivo Excel criado
        """
        excel_path = os.path.join(self.output_dir, f'spark_report_{self.timestamp}.xlsx')
        
        # Converte DataFrame para Pandas para salvar em Excel
        df_pandas = df.toPandas()
        
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # Dados originais
            df_pandas.to_excel(writer, sheet_name='Dados_Originais', index=False)
            
            # Resumo
            summary_df = pd.DataFrame([
                ['Total de Vendas', report_data.get('total_sales', 0)],
                ['Total de Transações', report_data.get('total_transactions', 0)],
                ['Ticket Médio', report_data.get('average_transaction', 0)],
                ['Produtos Únicos', report_data.get('unique_products', 0)]
            ], columns=['Métrica', 'Valor'])
            summary_df.to_excel(writer, sheet_name='Resumo', index=False)
            
            # Top produtos
            if 'top_products' in report_data:
                top_products_df = pd.DataFrame(
                    list(report_data['top_products'].items()),
                    columns=['Produto', 'Total_Vendas']
                )
                top_products_df.to_excel(writer, sheet_name='Top_Produtos', index=False)
            
            # Vendas mensais
            if 'monthly_sales' in report_data:
                monthly_df = pd.DataFrame(
                    list(report_data['monthly_sales'].items()),
                    columns=['Mês', 'Vendas']
                )
                monthly_df.to_excel(writer, sheet_name='Vendas_Mensais', index=False)
            
            # Estatísticas por categoria
            if 'category_stats' in report_data:
                category_data = []
                for category, stats in report_data['category_stats'].items():
                    category_data.append([
                        category,
                        stats['count'],
                        stats['total_sales'],
                        stats['avg_sales']
                    ])
                
                category_df = pd.DataFrame(
                    category_data,
                    columns=['Categoria', 'Transações', 'Total_Vendas', 'Media_Vendas']
                )
                category_df.to_excel(writer, sheet_name='Categorias', index=False)
        
        logger.info(f"Relatório Excel criado: {excel_path}")
        return excel_path
    
    def save_aggregated_data(self, df: DataFrame) -> str:
        """
        Salva dados agregados em Parquet
        
        Args:
            df (DataFrame): DataFrame para salvar
            
        Returns:
            str: Caminho do arquivo salvo
        """
        output_path = os.path.join(self.output_dir, f'aggregated_data_{self.timestamp}.parquet')
        
        try:
            df.write.mode("overwrite").parquet(output_path)
            logger.info(f"Dados agregados salvos em: {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Erro ao salvar dados agregados: {str(e)}")
            raise
    
    def generate_complete_report(self, df: DataFrame) -> Dict[str, str]:
        """
        Gera relatório completo (PDF, Excel e visualizações)
        
        Args:
            df (DataFrame): DataFrame com dados
            
        Returns:
            Dict[str, str]: Dicionário com caminhos dos arquivos gerados
        """
        # Cria dados do relatório
        report_data = self.create_sales_summary_report(df)
        
        # Cria visualizações
        image_paths = self.create_visualizations(df, report_data)
        
        # Cria PDF
        pdf_path = self.create_pdf_report(report_data, image_paths)
        
        # Cria Excel
        excel_path = self.create_excel_report(df, report_data)
        
        # Salva dados agregados
        aggregated_path = self.save_aggregated_data(df)
        
        return {
            'pdf': pdf_path,
            'excel': excel_path,
            'images': image_paths,
            'aggregated_data': aggregated_path,
            'report_data': report_data
        }

def main():
    """Função principal para teste do módulo"""
    from extract import SparkDataExtractor
    from transform import SparkDataTransformer
    
    extractor = SparkDataExtractor()
    transformer = SparkDataTransformer(extractor.spark)
    generator = SparkReportGenerator(extractor.spark)
    
    try:
        # Carrega e transforma dados
        df = extractor.extract_sales_data()
        df_transformed = transformer.transform_sales_data(df)
        
        print(f"Dados carregados e transformados. Contagem: {df_transformed.count()}")
        
        # Gera relatório completo
        results = generator.generate_complete_report(df_transformed)
        
        print("Relatórios gerados:")
        print(f"  PDF: {results['pdf']}")
        print(f"  Excel: {results['excel']}")
        print(f"  Imagens: {len(results['images'])} arquivos")
        print(f"  Dados Agregados: {results['aggregated_data']}")
        
        # Mostra resumo dos dados
        report_data = results['report_data']
        print(f"\nResumo:")
        print(f"  Total de Vendas: R$ {report_data.get('total_sales', 0):,.2f}")
        print(f"  Total de Transações: {report_data.get('total_transactions', 0):,}")
        print(f"  Ticket Médio: R$ {report_data.get('average_transaction', 0):,.2f}")
        
    except FileNotFoundError:
        print("Arquivo de dados não encontrado. Execute primeiro o extract.py")
    
    finally:
        extractor.stop_spark()

if __name__ == "__main__":
    main()
