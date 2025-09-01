"""
Módulo de Extração de Dados - Versão PySpark
Responsável por extrair dados de diferentes fontes usando Apache Spark
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from typing import Optional, Dict, Any
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkDataExtractor:
    """Classe para extração de dados usando PySpark"""
    
    def __init__(self, app_name: str = "DataExtractor", data_path: str = "data/"):
        """
        Inicializa o extrator de dados Spark
        
        Args:
            app_name (str): Nome da aplicação Spark
            data_path (str): Caminho para o diretório de dados
        """
        self.data_path = data_path
        self.spark = self._create_spark_session(app_name)
        self.ensure_data_directory()
    
    def _create_spark_session(self, app_name: str) -> SparkSession:
        """Cria sessão Spark otimizada"""
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
    
    def ensure_data_directory(self):
        """Garante que o diretório de dados existe"""
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)
            logger.info(f"Diretório de dados criado: {self.data_path}")
    
    def extract_from_csv(self, filename: str, **options) -> 'DataFrame':
        """
        Extrai dados de um arquivo CSV
        
        Args:
            filename (str): Nome do arquivo CSV
            **options: Opções adicionais para leitura do CSV
            
        Returns:
            DataFrame: DataFrame Spark com os dados extraídos
        """
        filepath = os.path.join(self.data_path, filename)
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Arquivo não encontrado: {filepath}")
        
        try:
            # Configurações padrão
            default_options = {
                "header": "true",
                "inferSchema": "true",
                "multiline": "true"
            }
            default_options.update(options)
            
            df = self.spark.read.options(**default_options).csv(filepath)
            
            logger.info(f"Dados extraídos com sucesso de {filename}")
            logger.info(f"Schema: {df.schema}")
            logger.info(f"Contagem de registros: {df.count()}")
            
            return df
        except Exception as e:
            logger.error(f"Erro ao extrair dados de {filename}: {str(e)}")
            raise
    
    def extract_from_parquet(self, filename: str) -> 'DataFrame':
        """
        Extrai dados de um arquivo Parquet
        
        Args:
            filename (str): Nome do arquivo Parquet
            
        Returns:
            DataFrame: DataFrame Spark com os dados extraídos
        """
        filepath = os.path.join(self.data_path, filename)
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Arquivo não encontrado: {filepath}")
        
        try:
            df = self.spark.read.parquet(filepath)
            
            logger.info(f"Dados extraídos com sucesso de {filename}")
            logger.info(f"Schema: {df.schema}")
            logger.info(f"Contagem de registros: {df.count()}")
            
            return df
        except Exception as e:
            logger.error(f"Erro ao extrair dados de {filename}: {str(e)}")
            raise
    
    def extract_from_database(self, connection_string: str, query: str, 
                            driver: str = "org.postgresql.Driver") -> 'DataFrame':
        """
        Extrai dados de um banco de dados
        
        Args:
            connection_string (str): String de conexão com o banco
            query (str): Query SQL para extração
            driver (str): Driver JDBC
            
        Returns:
            DataFrame: DataFrame Spark com os dados extraídos
        """
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", connection_string) \
                .option("dbtable", f"({query}) as subquery") \
                .option("driver", driver) \
                .load()
            
            logger.info(f"Dados extraídos do banco de dados")
            logger.info(f"Schema: {df.schema}")
            logger.info(f"Contagem de registros: {df.count()}")
            
            return df
        except Exception as e:
            logger.error(f"Erro ao extrair dados do banco: {str(e)}")
            raise
    
    def extract_sales_data(self, filename: str = "Coffe_sales.csv") -> 'DataFrame':
        """
        Extrai dados de vendas de café (exemplo específico)
        
        Args:
            filename (str): Nome do arquivo de vendas de café
            
        Returns:
            DataFrame: DataFrame com dados de vendas de café
        """
        df = self.extract_from_csv(filename)
        
        # Converte coluna de data se existir
        if 'Date' in df.columns:
            df = df.withColumn('Date', to_date(col('Date'), 'yyyy-MM-dd'))
        
        # Adiciona índice se não existir
        if 'id' not in df.columns:
            df = df.withColumn('id', monotonically_increasing_id())
        
        return df
    
    def extract_from_multiple_sources(self, sources: Dict[str, Dict]) -> Dict[str, 'DataFrame']:
        """
        Extrai dados de múltiplas fontes
        
        Args:
            sources (Dict): Dicionário com configurações das fontes
            
        Returns:
            Dict[str, DataFrame]: Dicionário com DataFrames extraídos
        """
        dataframes = {}
        
        for name, config in sources.items():
            try:
                source_type = config.get('type', 'csv')
                
                if source_type == 'csv':
                    df = self.extract_from_csv(config['filename'], **config.get('options', {}))
                elif source_type == 'parquet':
                    df = self.extract_from_parquet(config['filename'])
                elif source_type == 'database':
                    df = self.extract_from_database(
                        config['connection_string'],
                        config['query'],
                        config.get('driver', 'org.postgresql.Driver')
                    )
                else:
                    raise ValueError(f"Tipo de fonte não suportado: {source_type}")
                
                dataframes[name] = df
                logger.info(f"Fonte '{name}' extraída com sucesso")
                
            except Exception as e:
                logger.error(f"Erro ao extrair fonte '{name}': {str(e)}")
                raise
        
        return dataframes
    
    def get_data_info(self, df: 'DataFrame') -> Dict[str, Any]:
        """
        Retorna informações sobre o DataFrame
        
        Args:
            df (DataFrame): DataFrame para análise
            
        Returns:
            Dict[str, Any]: Informações do DataFrame
        """
        # Cache do DataFrame para múltiplas operações
        df.cache()
        
        try:
            info = {
                'schema': [str(field) for field in df.schema.fields],
                'count': df.count(),
                'columns': df.columns,
                'dtypes': {field.name: str(field.dataType) for field in df.schema.fields}
            }
            
            # Informações sobre valores nulos
            null_counts = {}
            for col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                null_counts[col_name] = null_count
            
            info['null_counts'] = null_counts
            
            # Estatísticas básicas para colunas numéricas
            numeric_cols = [field.name for field in df.schema.fields 
                          if isinstance(field.dataType, (IntegerType, LongType, FloatType, DoubleType))]
            
            if numeric_cols:
                stats = df.select(*[col(c).alias(c) for c in numeric_cols]).describe()
                info['statistics'] = stats.collect()
            
            return info
            
        finally:
            df.unpersist()
    
    def save_dataframe(self, df: 'DataFrame', filename: str, format: str = "parquet"):
        """
        Salva DataFrame em arquivo
        
        Args:
            df (DataFrame): DataFrame para salvar
            filename (str): Nome do arquivo
            format (str): Formato de saída (parquet, csv, json)
        """
        filepath = os.path.join(self.data_path, filename)
        
        try:
            if format.lower() == "parquet":
                df.write.mode("overwrite").parquet(filepath)
            elif format.lower() == "csv":
                df.write.mode("overwrite").option("header", "true").csv(filepath)
            elif format.lower() == "json":
                df.write.mode("overwrite").json(filepath)
            else:
                raise ValueError(f"Formato não suportado: {format}")
            
            logger.info(f"DataFrame salvo em {filepath} no formato {format}")
            
        except Exception as e:
            logger.error(f"Erro ao salvar DataFrame: {str(e)}")
            raise
    
    def stop_spark(self):
        """Para a sessão Spark"""
        if self.spark:
            self.spark.stop()
            logger.info("Sessão Spark finalizada")

def main():
    """Função principal para teste do módulo"""
    extractor = SparkDataExtractor()
    
    try:
        # Exemplo de uso
        try:
            # Tenta extrair dados de vendas
            sales_df = extractor.extract_sales_data()
            print("Dados de vendas extraídos com sucesso!")
            
            # Informações do DataFrame
            info = extractor.get_data_info(sales_df)
            print(f"Contagem de registros: {info['count']}")
            print(f"Colunas: {info['columns']}")
            print(f"Schema: {info['schema']}")
            
            # Mostra algumas linhas
            sales_df.show(5)
            
        except FileNotFoundError:
            print("Arquivo de dados não encontrado. Criando dados de exemplo...")
            
            # Cria dados de exemplo usando Spark
            from pyspark.sql.functions import lit, rand, when
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
            
            # Schema para dados de vendas de café
            schema = StructType([
                StructField("hour_of_day", IntegerType(), True),
                StructField("cash_type", StringType(), True),
                StructField("money", DoubleType(), True),
                StructField("coffee_name", StringType(), True),
                StructField("Time_of_Day", StringType(), True),
                StructField("Weekday", StringType(), True),
                StructField("Month_name", StringType(), True),
                StructField("Weekdaysort", IntegerType(), True),
                StructField("Monthsort", IntegerType(), True),
                StructField("Date", DateType(), True),
                StructField("Time", StringType(), True)
            ])
            
            # Cria DataFrame com dados de exemplo
            sample_data = []
            coffee_names = ["Latte", "Americano", "Hot Chocolate", "Cappuccino", "Espresso"]
            cash_types = ["card", "cash"]
            time_of_day = ["Morning", "Afternoon", "Evening"]
            weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri"]
            months = ["Jan", "Feb", "Mar", "Apr", "May"]
            
            for i in range(1000):
                sample_data.append((
                    (i % 24),  # hour_of_day
                    cash_types[i % len(cash_types)],  # cash_type
                    round(20.0 + (i % 30), 1),  # money
                    coffee_names[i % len(coffee_names)],  # coffee_name
                    time_of_day[i % len(time_of_day)],  # Time_of_Day
                    weekdays[i % len(weekdays)],  # Weekday
                    months[i % len(months)],  # Month_name
                    (i % 7) + 1,  # Weekdaysort
                    (i % 12) + 1,  # Monthsort
                    f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",  # Date
                    f"{(i % 24):02d}:{(i % 60):02d}:{(i % 60):02d}"  # Time
                ))
            
            sample_df = extractor.spark.createDataFrame(sample_data, schema)
            
            # Salva dados de exemplo
            extractor.save_dataframe(sample_df, "Coffe_sales.csv", "csv")
            print("Arquivo de exemplo criado: data/Coffe_sales.csv")
            
            # Mostra algumas linhas
            sample_df.show(5)
    
    finally:
        extractor.stop_spark()

if __name__ == "__main__":
    main()
