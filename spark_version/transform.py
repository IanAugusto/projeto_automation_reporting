"""
Módulo de Transformação de Dados - Versão PySpark
Responsável por limpar, transformar e preparar dados para análise usando Apache Spark
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from typing import Dict, List, Optional, Tuple
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkDataTransformer:
    """Classe para transformação de dados usando PySpark"""
    
    def __init__(self, spark: SparkSession):
        """
        Inicializa o transformador de dados Spark
        
        Args:
            spark (SparkSession): Sessão Spark
        """
        self.spark = spark
    
    def clean_data(self, df: DataFrame, 
                   remove_duplicates: bool = True,
                   handle_missing: str = 'drop') -> DataFrame:
        """
        Limpa os dados removendo duplicatas e tratando valores ausentes
        
        Args:
            df (DataFrame): DataFrame para limpeza
            remove_duplicates (bool): Se deve remover duplicatas
            handle_missing (str): Como tratar valores ausentes ('drop', 'fill', 'interpolate')
            
        Returns:
            DataFrame: DataFrame limpo
        """
        df_clean = df
        
        # Remove duplicatas
        if remove_duplicates:
            initial_count = df_clean.count()
            df_clean = df_clean.dropDuplicates()
            final_count = df_clean.count()
            removed_rows = initial_count - final_count
            if removed_rows > 0:
                logger.info(f"Removidas {removed_rows} linhas duplicadas")
        
        # Trata valores ausentes
        if handle_missing == 'drop':
            df_clean = df_clean.dropna()
        elif handle_missing == 'fill':
            # Preenche com valores apropriados baseado no tipo
            for field in df_clean.schema.fields:
                col_name = field.name
                if isinstance(field.dataType, (IntegerType, LongType, FloatType, DoubleType)):
                    # Para colunas numéricas, preenche com mediana
                    median_value = df_clean.select(percentile_approx(col(col_name), 0.5).alias("median")).collect()[0]["median"]
                    df_clean = df_clean.fillna({col_name: median_value})
                else:
                    # Para colunas não numéricas, preenche com valor mais frequente
                    mode_value = df_clean.groupBy(col_name).count().orderBy(desc("count")).first()
                    if mode_value:
                        df_clean = df_clean.fillna({col_name: mode_value[0]})
                    else:
                        df_clean = df_clean.fillna({col_name: "Unknown"})
        elif handle_missing == 'interpolate':
            # Para Spark, usamos forward fill com window functions
            window_spec = Window.partitionBy().orderBy("id")
            for field in df_clean.schema.fields:
                if isinstance(field.dataType, (IntegerType, LongType, FloatType, DoubleType)):
                    col_name = field.name
                    df_clean = df_clean.withColumn(
                        col_name,
                        last(col_name, ignorenulls=True).over(window_spec)
                    )
        
        final_count = df_clean.count()
        logger.info(f"Dados limpos. Contagem final: {final_count}")
        return df_clean
    
    def add_calculated_columns(self, df: DataFrame) -> DataFrame:
        """
        Adiciona colunas calculadas ao DataFrame
        
        Args:
            df (DataFrame): DataFrame base
            
        Returns:
            DataFrame: DataFrame com colunas adicionais
        """
        df_transformed = df
        
        # Adiciona colunas de data/hora se existir coluna de data
        if 'date' in df_transformed.columns:
            df_transformed = df_transformed.withColumn('year', year(col('date')))
            df_transformed = df_transformed.withColumn('month', month(col('date')))
            df_transformed = df_transformed.withColumn('day_of_week', date_format(col('date'), 'EEEE'))
            df_transformed = df_transformed.withColumn('quarter', quarter(col('date')))
            df_transformed = df_transformed.withColumn('day_of_month', dayofmonth(col('date')))
        
        # Adiciona colunas de vendas se existirem
        if 'quantity' in df_transformed.columns and 'price' in df_transformed.columns:
            df_transformed = df_transformed.withColumn(
                'total_calculated', 
                col('quantity') * col('price')
            )
        
        # Adiciona categorização de vendas
        if 'total' in df_transformed.columns:
            df_transformed = df_transformed.withColumn(
                'sales_category',
                when(col('total') <= 50, 'Baixa')
                .when(col('total') <= 100, 'Média')
                .when(col('total') <= 200, 'Alta')
                .otherwise('Muito Alta')
            )
        
        # Adiciona coluna de ranking por produto
        if 'product' in df_transformed.columns and 'total' in df_transformed.columns:
            window_spec = Window.partitionBy('product').orderBy(desc('total'))
            df_transformed = df_transformed.withColumn('product_rank', rank().over(window_spec))
        
        logger.info("Colunas calculadas adicionadas com sucesso")
        return df_transformed
    
    def aggregate_data(self, df: DataFrame, 
                      group_columns: List[str],
                      agg_functions: Dict[str, List[str]]) -> DataFrame:
        """
        Agrega dados por grupos especificados
        
        Args:
            df (DataFrame): DataFrame para agregação
            group_columns (List[str]): Colunas para agrupamento
            agg_functions (Dict[str, List[str]]): Funções de agregação por coluna
            
        Returns:
            DataFrame: DataFrame agregado
        """
        try:
            # Prepara expressões de agregação
            agg_exprs = []
            for col_name, functions in agg_functions.items():
                for func in functions:
                    if func.lower() == 'sum':
                        agg_exprs.append(sum(col(col_name)).alias(f"{col_name}_sum"))
                    elif func.lower() == 'mean' or func.lower() == 'avg':
                        agg_exprs.append(avg(col(col_name)).alias(f"{col_name}_avg"))
                    elif func.lower() == 'count':
                        agg_exprs.append(count(col(col_name)).alias(f"{col_name}_count"))
                    elif func.lower() == 'min':
                        agg_exprs.append(min(col(col_name)).alias(f"{col_name}_min"))
                    elif func.lower() == 'max':
                        agg_exprs.append(max(col(col_name)).alias(f"{col_name}_max"))
                    elif func.lower() == 'stddev':
                        agg_exprs.append(stddev(col(col_name)).alias(f"{col_name}_stddev"))
            
            # Executa agregação
            df_agg = df.groupBy(*group_columns).agg(*agg_exprs)
            
            logger.info(f"Dados agregados com sucesso. Contagem: {df_agg.count()}")
            return df_agg
            
        except Exception as e:
            logger.error(f"Erro na agregação: {str(e)}")
            raise
    
    def pivot_data(self, df: DataFrame,
                   index: str,
                   columns: str,
                   values: str,
                   fill_value: float = 0) -> DataFrame:
        """
        Cria tabela pivot dos dados
        
        Args:
            df (DataFrame): DataFrame base
            index (str): Coluna para índice
            columns (str): Coluna para colunas
            values (str): Coluna para valores
            fill_value (float): Valor para preencher células vazias
            
        Returns:
            DataFrame: DataFrame pivotado
        """
        try:
            df_pivot = df.groupBy(index).pivot(columns).agg(sum(col(values))).fillna(fill_value)
            
            logger.info(f"Dados pivotados com sucesso. Contagem: {df_pivot.count()}")
            return df_pivot
            
        except Exception as e:
            logger.error(f"Erro no pivot: {str(e)}")
            raise
    
    def filter_data(self, df: DataFrame, 
                   filters: Dict[str, any]) -> DataFrame:
        """
        Filtra dados baseado em condições
        
        Args:
            df (DataFrame): DataFrame base
            filters (Dict[str, any]): Dicionário com filtros {coluna: valor}
            
        Returns:
            DataFrame: DataFrame filtrado
        """
        df_filtered = df
        
        for column, value in filters.items():
            if column in df_filtered.columns:
                if isinstance(value, tuple) and len(value) == 2:
                    # Range filter (min, max)
                    df_filtered = df_filtered.filter(
                        (col(column) >= value[0]) & (col(column) <= value[1])
                    )
                elif isinstance(value, list):
                    # List filter
                    df_filtered = df_filtered.filter(col(column).isin(value))
                else:
                    # Exact match
                    df_filtered = df_filtered.filter(col(column) == value)
        
        final_count = df_filtered.count()
        logger.info(f"Dados filtrados. Contagem final: {final_count}")
        return df_filtered
    
    def detect_outliers_iqr(self, df: DataFrame, columns: List[str]) -> DataFrame:
        """
        Detecta outliers usando método IQR
        
        Args:
            df (DataFrame): DataFrame para análise
            columns (List[str]): Colunas para análise
            
        Returns:
            DataFrame: DataFrame com flags de outliers
        """
        df_with_outliers = df
        
        for column in columns:
            if column in df_with_outliers.columns:
                # Calcula quartis
                quantiles = df_with_outliers.select(
                    percentile_approx(col(column), 0.25).alias("q1"),
                    percentile_approx(col(column), 0.75).alias("q3")
                ).collect()[0]
                
                q1 = quantiles["q1"]
                q3 = quantiles["q3"]
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                
                # Adiciona flag de outlier
                df_with_outliers = df_with_outliers.withColumn(
                    f'{column}_outlier',
                    (col(column) < lower_bound) | (col(column) > upper_bound)
                )
        
        # Flag geral de outlier
        outlier_columns = [col for col in df_with_outliers.columns if col.endswith('_outlier')]
        if outlier_columns:
            df_with_outliers = df_with_outliers.withColumn(
                'is_outlier',
                reduce(lambda a, b: a | b, [col(c) for c in outlier_columns])
            )
        
        outlier_count = df_with_outliers.filter(col('is_outlier')).count()
        logger.info(f"Detectados {outlier_count} outliers usando método IQR")
        
        return df_with_outliers
    
    def transform_sales_data(self, df: DataFrame) -> DataFrame:
        """
        Transformação específica para dados de vendas
        
        Args:
            df (DataFrame): DataFrame de vendas
            
        Returns:
            DataFrame: DataFrame transformado
        """
        # Limpa os dados
        df_clean = self.clean_data(df)
        
        # Adiciona colunas calculadas
        df_transformed = self.add_calculated_columns(df_clean)
        
        # Remove outliers usando IQR
        if 'total' in df_transformed.columns:
            df_transformed = self.detect_outliers_iqr(df_transformed, ['total'])
            
            # Filtra outliers
            initial_count = df_transformed.count()
            df_transformed = df_transformed.filter(~col('is_outlier'))
            final_count = df_transformed.count()
            removed_outliers = initial_count - final_count
            
            if removed_outliers > 0:
                logger.info(f"Removidos {removed_outliers} outliers")
        
        logger.info("Transformação de dados de vendas concluída")
        return df_transformed
    
    def add_time_series_features(self, df: DataFrame, 
                                date_column: str,
                                value_column: str,
                                window_days: int = 7) -> DataFrame:
        """
        Adiciona features de série temporal
        
        Args:
            df (DataFrame): DataFrame com dados temporais
            date_column (str): Coluna de data
            value_column (str): Coluna de valores
            window_days (int): Janela em dias para cálculos
            
        Returns:
            DataFrame: DataFrame com features temporais
        """
        # Ordena por data
        df_sorted = df.orderBy(date_column)
        
        # Window specification para cálculos temporais
        window_spec = Window.partitionBy().orderBy(date_column).rowsBetween(-window_days, 0)
        
        # Adiciona features temporais
        df_with_features = df_sorted.withColumn(
            f'{value_column}_rolling_mean',
            avg(col(value_column)).over(window_spec)
        ).withColumn(
            f'{value_column}_rolling_std',
            stddev(col(value_column)).over(window_spec)
        ).withColumn(
            f'{value_column}_lag_1',
            lag(col(value_column), 1).over(Window.orderBy(date_column))
        ).withColumn(
            f'{value_column}_lag_7',
            lag(col(value_column), 7).over(Window.orderBy(date_column))
        )
        
        logger.info("Features de série temporal adicionadas")
        return df_with_features

def main():
    """Função principal para teste do módulo"""
    from extract import SparkDataExtractor
    
    extractor = SparkDataExtractor()
    transformer = SparkDataTransformer(extractor.spark)
    
    try:
        # Carrega dados de exemplo
        df = extractor.extract_sales_data()
        print(f"Dados carregados. Contagem: {df.count()}")
        
        # Transforma os dados
        df_transformed = transformer.transform_sales_data(df)
        print(f"Dados transformados. Contagem: {df_transformed.count()}")
        print(f"Colunas: {df_transformed.columns}")
        
        # Mostra algumas linhas
        df_transformed.show(5)
        
        # Exemplo de agregação
        if 'product' in df_transformed.columns and 'total' in df_transformed.columns:
            agg_functions = {
                'total': ['sum', 'avg', 'count'],
                'quantity': ['sum', 'avg']
            }
            df_agg = transformer.aggregate_data(
                df_transformed, 
                ['product'], 
                agg_functions
            )
            print(f"Dados agregados. Contagem: {df_agg.count()}")
            df_agg.show()
        
    except FileNotFoundError:
        print("Arquivo de dados não encontrado. Execute primeiro o extract.py")
    
    finally:
        extractor.stop_spark()

if __name__ == "__main__":
    main()
