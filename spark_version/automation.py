"""
Módulo de Automação - Versão PySpark
Responsável por automatizar todo o pipeline de análise de dados usando Apache Spark
"""

from pyspark.sql import SparkSession
import schedule
import time
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Optional
import sys
import json

# Importa módulos locais
from extract import SparkDataExtractor
from transform import SparkDataTransformer
from anomaly import SparkAnomalyDetector
from report import SparkReportGenerator

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('spark_automation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SparkDataPipeline:
    """Classe principal para automação do pipeline de dados usando Spark"""
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Inicializa o pipeline de dados Spark
        
        Args:
            config (Dict): Configurações do pipeline
        """
        self.config = config or self.get_default_config()
        self.spark = self._create_spark_session()
        self.extractor = SparkDataExtractor("DataPipeline", self.config['data_path'])
        self.transformer = SparkDataTransformer(self.spark)
        self.anomaly_detector = SparkAnomalyDetector(self.spark)
        self.report_generator = SparkReportGenerator(self.spark, self.config['reports_path'])
        
        # Cria diretórios necessários
        self.ensure_directories()
    
    def _create_spark_session(self) -> SparkSession:
        """Cria sessão Spark otimizada para o pipeline"""
        return SparkSession.builder \
            .appName("DataPipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
            .getOrCreate()
    
    def get_default_config(self) -> Dict:
        """Retorna configuração padrão"""
        return {
            'data_path': 'data/',
            'reports_path': 'reports/',
            'input_file': 'sales_data.csv',
            'output_file': 'processed_sales_data.parquet',
            'anomaly_threshold': 0.1,
            'enable_anomaly_detection': True,
            'enable_reporting': True,
            'schedule_time': '09:00',  # Hora para execução diária
            'retention_days': 30,  # Dias para manter relatórios
            'spark_config': {
                'executor_memory': '2g',
                'driver_memory': '1g',
                'max_result_size': '1g'
            }
        }
    
    def ensure_directories(self):
        """Garante que todos os diretórios necessários existem"""
        directories = [
            self.config['data_path'],
            self.config['reports_path'],
            'logs/'
        ]
        
        for directory in directories:
            if not os.path.exists(directory):
                os.makedirs(directory)
                logger.info(f"Diretório criado: {directory}")
    
    def run_extraction(self) -> 'DataFrame':
        """
        Executa etapa de extração
        
        Returns:
            DataFrame: Dados extraídos
        """
        logger.info("Iniciando extração de dados...")
        
        try:
            # Extrai dados de vendas
            df = self.extractor.extract_sales_data(self.config['input_file'])
            
            # Salva informações da extração
            info = self.extractor.get_data_info(df)
            logger.info(f"Extração concluída. Contagem: {info['count']}")
            
            return df
            
        except Exception as e:
            logger.error(f"Erro na extração: {str(e)}")
            raise
    
    def run_transformation(self, df: 'DataFrame') -> 'DataFrame':
        """
        Executa etapa de transformação
        
        Args:
            df (DataFrame): Dados para transformação
            
        Returns:
            DataFrame: Dados transformados
        """
        logger.info("Iniciando transformação de dados...")
        
        try:
            # Transforma dados de vendas
            df_transformed = self.transformer.transform_sales_data(df)
            
            # Salva dados transformados
            output_path = os.path.join(self.config['data_path'], self.config['output_file'])
            self.extractor.save_dataframe(df_transformed, output_path, "parquet")
            
            logger.info(f"Transformação concluída. Contagem: {df_transformed.count()}")
            return df_transformed
            
        except Exception as e:
            logger.error(f"Erro na transformação: {str(e)}")
            raise
    
    def run_anomaly_detection(self, df: 'DataFrame') -> 'DataFrame':
        """
        Executa detecção de anomalias
        
        Args:
            df (DataFrame): Dados para análise
            
        Returns:
            DataFrame: Dados com flags de anomalias
        """
        if not self.config['enable_anomaly_detection']:
            logger.info("Detecção de anomalias desabilitada")
            return df
        
        logger.info("Iniciando detecção de anomalias...")
        
        try:
            # Detecta outliers estatísticos
            df_with_outliers = self.anomaly_detector.detect_statistical_outliers(
                df, 
                ['total', 'quantity', 'price'],
                method='iqr'
            )
            
            # Detecta anomalias com K-Means
            feature_columns = ['quantity', 'price', 'total']
            df_with_anomalies = self.anomaly_detector.detect_kmeans_anomalies(
                df_with_outliers,
                feature_columns,
                contamination=self.config['anomaly_threshold']
            )
            
            # Detecta anomalias temporais se existir coluna de data
            if 'date' in df_with_anomalies.columns:
                df_with_anomalies = self.anomaly_detector.detect_temporal_anomalies(
                    df_with_anomalies,
                    'date',
                    'total'
                )
            
            # Gera relatório de anomalias
            anomaly_report = self.anomaly_detector.generate_anomaly_report(df_with_anomalies)
            
            # Salva relatório de anomalias
            anomaly_report_path = os.path.join(
                self.config['reports_path'], 
                f'spark_anomaly_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            )
            
            with open(anomaly_report_path, 'w') as f:
                json.dump(anomaly_report, f, indent=2)
            
            logger.info(f"Detecção de anomalias concluída. Relatório salvo: {anomaly_report_path}")
            return df_with_anomalies
            
        except Exception as e:
            logger.error(f"Erro na detecção de anomalias: {str(e)}")
            return df
    
    def run_reporting(self, df: 'DataFrame') -> Dict:
        """
        Executa geração de relatórios
        
        Args:
            df (DataFrame): Dados para relatório
            
        Returns:
            Dict: Resultados da geração de relatórios
        """
        if not self.config['enable_reporting']:
            logger.info("Geração de relatórios desabilitada")
            return {}
        
        logger.info("Iniciando geração de relatórios...")
        
        try:
            # Gera relatório completo
            results = self.report_generator.generate_complete_report(df)
            
            logger.info("Geração de relatórios concluída")
            return results
            
        except Exception as e:
            logger.error(f"Erro na geração de relatórios: {str(e)}")
            raise
    
    def cleanup_old_reports(self):
        """Remove relatórios antigos baseado na configuração de retenção"""
        logger.info("Iniciando limpeza de relatórios antigos...")
        
        try:
            cutoff_date = datetime.now() - timedelta(days=self.config['retention_days'])
            
            for filename in os.listdir(self.config['reports_path']):
                filepath = os.path.join(self.config['reports_path'], filename)
                
                if os.path.isfile(filepath):
                    file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                    
                    if file_time < cutoff_date:
                        os.remove(filepath)
                        logger.info(f"Arquivo removido: {filename}")
            
            logger.info("Limpeza de relatórios concluída")
            
        except Exception as e:
            logger.error(f"Erro na limpeza: {str(e)}")
    
    def optimize_dataframe(self, df: 'DataFrame') -> 'DataFrame':
        """
        Otimiza DataFrame para melhor performance
        
        Args:
            df (DataFrame): DataFrame para otimização
            
        Returns:
            DataFrame: DataFrame otimizado
        """
        # Cache do DataFrame
        df.cache()
        
        # Reparticiona se necessário
        num_partitions = df.rdd.getNumPartitions()
        if num_partitions > 200:  # Muitas partições pequenas
            df = df.coalesce(200)
            logger.info(f"DataFrame reparticionado de {num_partitions} para 200 partições")
        
        return df
    
    def run_full_pipeline(self) -> Dict:
        """
        Executa o pipeline completo
        
        Returns:
            Dict: Resultados de todas as etapas
        """
        start_time = datetime.now()
        logger.info("=== INICIANDO PIPELINE SPARK COMPLETO ===")
        
        results = {
            'start_time': start_time,
            'success': False,
            'stages': {},
            'spark_info': {
                'app_name': self.spark.sparkContext.appName,
                'spark_version': self.spark.version
            }
        }
        
        try:
            # 1. Extração
            df = self.run_extraction()
            df = self.optimize_dataframe(df)
            results['stages']['extraction'] = {'success': True, 'count': df.count()}
            
            # 2. Transformação
            df_transformed = self.run_transformation(df)
            df_transformed = self.optimize_dataframe(df_transformed)
            results['stages']['transformation'] = {'success': True, 'count': df_transformed.count()}
            
            # 3. Detecção de Anomalias
            df_with_anomalies = self.run_anomaly_detection(df_transformed)
            results['stages']['anomaly_detection'] = {'success': True, 'count': df_with_anomalies.count()}
            
            # 4. Geração de Relatórios
            report_results = self.run_reporting(df_with_anomalies)
            results['stages']['reporting'] = {'success': True, 'files': report_results}
            
            # 5. Limpeza
            self.cleanup_old_reports()
            results['stages']['cleanup'] = {'success': True}
            
            results['success'] = True
            results['end_time'] = datetime.now()
            results['duration'] = results['end_time'] - start_time
            
            logger.info(f"=== PIPELINE SPARK CONCLUÍDO COM SUCESSO em {results['duration']} ===")
            
        except Exception as e:
            results['error'] = str(e)
            results['end_time'] = datetime.now()
            results['duration'] = results['end_time'] - start_time
            
            logger.error(f"=== PIPELINE SPARK FALHOU após {results['duration']}: {str(e)} ===")
        
        finally:
            # Limpa cache
            try:
                self.spark.catalog.clearCache()
            except:
                pass
        
        return results
    
    def schedule_daily_run(self):
        """Agenda execução diária do pipeline"""
        schedule_time = self.config['schedule_time']
        schedule.every().day.at(schedule_time).do(self.run_full_pipeline)
        
        logger.info(f"Pipeline Spark agendado para execução diária às {schedule_time}")
        
        # Loop de execução
        while True:
            schedule.run_pending()
            time.sleep(60)  # Verifica a cada minuto
    
    def run_once(self):
        """Executa o pipeline uma única vez"""
        return self.run_full_pipeline()
    
    def stop_spark(self):
        """Para a sessão Spark"""
        if self.spark:
            self.spark.stop()
            logger.info("Sessão Spark finalizada")

def main():
    """Função principal para execução do pipeline"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Pipeline de Análise de Dados - Spark')
    parser.add_argument('--mode', choices=['once', 'schedule'], default='once',
                       help='Modo de execução: once (uma vez) ou schedule (agendado)')
    parser.add_argument('--config', type=str, help='Arquivo de configuração JSON')
    
    args = parser.parse_args()
    
    # Carrega configuração se fornecida
    config = None
    if args.config and os.path.exists(args.config):
        with open(args.config, 'r') as f:
            config = json.load(f)
    
    # Inicializa pipeline
    pipeline = SparkDataPipeline(config)
    
    try:
        if args.mode == 'once':
            # Executa uma vez
            results = pipeline.run_once()
            
            if results['success']:
                print("Pipeline Spark executado com sucesso!")
                print(f"Duração: {results['duration']}")
                print(f"Spark Version: {results['spark_info']['spark_version']}")
                
                # Mostra resumo das etapas
                for stage, info in results['stages'].items():
                    if 'count' in info:
                        print(f"  {stage}: {info['count']} registros processados")
                    else:
                        print(f"  {stage}: concluído")
            else:
                print(f"Pipeline Spark falhou: {results.get('error', 'Erro desconhecido')}")
                sys.exit(1)
        
        elif args.mode == 'schedule':
            # Executa em modo agendado
            print("Iniciando pipeline Spark em modo agendado...")
            print("Pressione Ctrl+C para parar")
            try:
                pipeline.schedule_daily_run()
            except KeyboardInterrupt:
                print("\nPipeline Spark interrompido pelo usuário")
    
    finally:
        pipeline.stop_spark()

if __name__ == "__main__":
    main()
