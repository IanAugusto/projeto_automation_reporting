"""
Módulo de Detecção de Anomalias - Versão PySpark
Responsável por identificar padrões anômalos nos dados usando Apache Spark
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from typing import Dict, List, Tuple, Optional
import logging
import numpy as np

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkAnomalyDetector:
    """Classe para detecção de anomalias usando PySpark"""
    
    def __init__(self, spark: SparkSession):
        """
        Inicializa o detector de anomalias Spark
        
        Args:
            spark (SparkSession): Sessão Spark
        """
        self.spark = spark
        self.kmeans_model = None
        self.scaler_model = None
    
    def detect_statistical_outliers(self, df: DataFrame, 
                                  columns: List[str],
                                  method: str = 'iqr',
                                  threshold: float = 1.5) -> DataFrame:
        """
        Detecta outliers usando métodos estatísticos
        
        Args:
            df (DataFrame): DataFrame para análise
            columns (List[str]): Colunas para análise
            method (str): Método ('iqr', 'zscore')
            threshold (float): Threshold para detecção
            
        Returns:
            DataFrame: DataFrame com flag de outliers
        """
        df_result = df
        
        for column in columns:
            if column not in df_result.columns:
                continue
                
            if method == 'iqr':
                # Calcula quartis usando percentil aproximado
                quantiles = df_result.select(
                    percentile_approx(col(column), 0.25).alias("q1"),
                    percentile_approx(col(column), 0.75).alias("q3")
                ).collect()[0]
                
                q1 = quantiles["q1"]
                q3 = quantiles["q3"]
                iqr = q3 - q1
                lower_bound = q1 - threshold * iqr
                upper_bound = q3 + threshold * iqr
                
                df_result = df_result.withColumn(
                    f'{column}_outlier',
                    (col(column) < lower_bound) | (col(column) > upper_bound)
                )
                
            elif method == 'zscore':
                # Calcula z-score
                stats = df_result.select(
                    avg(col(column)).alias("mean"),
                    stddev(col(column)).alias("std")
                ).collect()[0]
                
                mean_val = stats["mean"]
                std_val = stats["std"]
                
                df_result = df_result.withColumn(
                    f'{column}_zscore',
                    abs((col(column) - mean_val) / std_val)
                ).withColumn(
                    f'{column}_outlier',
                    col(f'{column}_zscore') > threshold
                )
        
        # Flag geral de outlier
        outlier_columns = [col for col in df_result.columns if col.endswith('_outlier')]
        if outlier_columns:
            df_result = df_result.withColumn(
                'is_outlier',
                reduce(lambda a, b: a | b, [col(c) for c in outlier_columns])
            )
        
        outlier_count = df_result.filter(col('is_outlier')).count()
        logger.info(f"Detectados {outlier_count} outliers usando método {method}")
        
        return df_result
    
    def detect_kmeans_anomalies(self, df: DataFrame,
                              feature_columns: List[str],
                              k: int = 3,
                              contamination: float = 0.1) -> DataFrame:
        """
        Detecta anomalias usando K-Means clustering
        
        Args:
            df (DataFrame): DataFrame para análise
            feature_columns (List[str]): Colunas para usar como features
            k (int): Número de clusters
            contamination (float): Proporção esperada de anomalias
            
        Returns:
            DataFrame: DataFrame com flag de anomalias
        """
        df_result = df
        
        # Prepara os dados
        features_df = df_result.select(*feature_columns).dropna()
        
        if features_df.count() == 0:
            logger.warning("Nenhuma feature válida encontrada")
            return df_result
        
        # Cria pipeline de ML
        assembler = VectorAssembler(
            inputCols=feature_columns,
            outputCol="features"
        )
        
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True
        )
        
        kmeans = KMeans(
            featuresCol="scaled_features",
            predictionCol="cluster",
            k=k,
            seed=42
        )
        
        pipeline = Pipeline(stages=[assembler, scaler, kmeans])
        
        # Treina o modelo
        model = pipeline.fit(features_df)
        self.kmeans_model = model
        self.scaler_model = model.stages[1]
        
        # Prediz clusters
        predictions = model.transform(features_df)
        
        # Calcula distâncias dos pontos aos centróides
        cluster_centers = model.stages[2].clusterCenters()
        
        def calculate_distance_to_centroid(features, cluster_id):
            """Calcula distância do ponto ao centróide do cluster"""
            center = cluster_centers[cluster_id]
            return float(np.linalg.norm(features - center))
        
        distance_udf = udf(calculate_distance_to_centroid, DoubleType())
        
        predictions_with_distance = predictions.withColumn(
            "distance_to_centroid",
            distance_udf(col("scaled_features"), col("cluster"))
        )
        
        # Calcula threshold baseado na contaminação
        threshold = predictions_with_distance.select(
            percentile_approx(col("distance_to_centroid"), 1 - contamination)
        ).collect()[0][0]
        
        # Marca anomalias
        predictions_with_anomalies = predictions_with_distance.withColumn(
            "kmeans_anomaly",
            col("distance_to_centroid") > threshold
        )
        
        # Merge com dados originais
        df_result = df_result.join(
            predictions_with_anomalies.select("id", "cluster", "distance_to_centroid", "kmeans_anomaly"),
            on="id",
            how="left"
        ).fillna({"kmeans_anomaly": False})
        
        anomaly_count = df_result.filter(col("kmeans_anomaly")).count()
        logger.info(f"Detectadas {anomaly_count} anomalias usando K-Means")
        
        return df_result
    
    def detect_temporal_anomalies(self, df: DataFrame,
                                date_column: str,
                                value_column: str,
                                window: int = 7) -> DataFrame:
        """
        Detecta anomalias temporais usando média móvel
        
        Args:
            df (DataFrame): DataFrame com dados temporais
            date_column (str): Coluna de data
            value_column (str): Coluna de valores
            window (int): Janela para média móvel
            
        Returns:
            DataFrame: DataFrame com flag de anomalias temporais
        """
        df_result = df.orderBy(date_column)
        
        # Window specification para cálculos temporais
        window_spec = Window.orderBy(date_column).rowsBetween(-window, 0)
        
        # Calcula média móvel e desvio padrão
        df_result = df_result.withColumn(
            'rolling_mean',
            avg(col(value_column)).over(window_spec)
        ).withColumn(
            'rolling_std',
            stddev(col(value_column)).over(window_spec)
        )
        
        # Detecta anomalias (valores fora de 2 desvios padrão)
        df_result = df_result.withColumn(
            'temporal_anomaly',
            abs(col(value_column) - col('rolling_mean')) > (2 * col('rolling_std'))
        )
        
        anomaly_count = df_result.filter(col('temporal_anomaly')).count()
        logger.info(f"Detectadas {anomaly_count} anomalias temporais")
        
        return df_result
    
    def detect_pattern_anomalies(self, df: DataFrame,
                               group_columns: List[str],
                               value_column: str) -> DataFrame:
        """
        Detecta anomalias em padrões de grupos
        
        Args:
            df (DataFrame): DataFrame para análise
            group_columns (List[str]): Colunas para agrupamento
            value_column (str): Coluna de valores para análise
            
        Returns:
            DataFrame: DataFrame com flag de anomalias de padrão
        """
        df_result = df
        
        # Calcula estatísticas por grupo
        group_stats = df_result.groupBy(*group_columns).agg(
            avg(col(value_column)).alias("group_mean"),
            stddev(col(value_column)).alias("group_std"),
            count(col(value_column)).alias("group_count")
        )
        
        # Merge com dados originais
        df_result = df_result.join(group_stats, on=group_columns, how="left")
        
        # Detecta anomalias (valores fora de 2 desvios padrão do grupo)
        df_result = df_result.withColumn(
            'pattern_anomaly',
            abs(col(value_column) - col('group_mean')) > (2 * col('group_std'))
        )
        
        anomaly_count = df_result.filter(col('pattern_anomaly')).count()
        logger.info(f"Detectadas {anomaly_count} anomalias de padrão")
        
        return df_result
    
    def detect_isolation_forest_anomalies(self, df: DataFrame,
                                        feature_columns: List[str],
                                        contamination: float = 0.1) -> DataFrame:
        """
        Detecta anomalias usando Isolation Forest (implementação simplificada)
        
        Args:
            df (DataFrame): DataFrame para análise
            feature_columns (List[str]): Colunas para usar como features
            contamination (float): Proporção esperada de anomalias
            
        Returns:
            DataFrame: DataFrame com flag de anomalias
        """
        # Para uma implementação completa do Isolation Forest em Spark,
        # seria necessário usar bibliotecas como Spark MLlib ou implementar
        # uma versão customizada. Aqui usamos uma abordagem simplificada
        # baseada em distâncias.
        
        df_result = df
        
        # Prepara features
        assembler = VectorAssembler(
            inputCols=feature_columns,
            outputCol="features"
        )
        
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True
        )
        
        pipeline = Pipeline(stages=[assembler, scaler])
        model = pipeline.fit(df_result)
        
        # Transforma dados
        features_scaled = model.transform(df_result)
        
        # Calcula distância média de cada ponto aos outros
        # (implementação simplificada)
        def calculate_isolation_score(features):
            # Em uma implementação real, isso seria mais complexo
            return float(np.linalg.norm(features))
        
        isolation_udf = udf(calculate_isolation_score, DoubleType())
        
        features_with_score = features_scaled.withColumn(
            "isolation_score",
            isolation_udf(col("scaled_features"))
        )
        
        # Calcula threshold
        threshold = features_with_score.select(
            percentile_approx(col("isolation_score"), 1 - contamination)
        ).collect()[0][0]
        
        # Marca anomalias
        df_result = df_result.join(
            features_with_score.select("id", "isolation_score"),
            on="id",
            how="left"
        ).withColumn(
            "isolation_forest_anomaly",
            col("isolation_score") > threshold
        )
        
        anomaly_count = df_result.filter(col("isolation_forest_anomaly")).count()
        logger.info(f"Detectadas {anomaly_count} anomalias usando Isolation Forest")
        
        return df_result
    
    def generate_anomaly_report(self, df: DataFrame) -> Dict:
        """
        Gera relatório de anomalias detectadas
        
        Args:
            df (DataFrame): DataFrame com flags de anomalias
            
        Returns:
            Dict: Relatório de anomalias
        """
        report = {}
        total_count = df.count()
        
        # Conta diferentes tipos de anomalias
        anomaly_columns = [col for col in df.columns if 'anomaly' in col or 'outlier' in col]
        
        for col_name in anomaly_columns:
            if col_name in df.columns:
                count = df.filter(col(col_name)).count()
                percentage = (count / total_count) * 100 if total_count > 0 else 0
                report[col_name] = {
                    'count': int(count),
                    'percentage': round(percentage, 2)
                }
        
        # Anomalias combinadas
        if len(anomaly_columns) > 1:
            combined_anomalies = df.filter(
                reduce(lambda a, b: a | b, [col(c) for c in anomaly_columns])
            ).count()
            report['combined_anomalies'] = {
                'count': int(combined_anomalies),
                'percentage': round((combined_anomalies / total_count) * 100, 2) if total_count > 0 else 0
            }
        
        logger.info("Relatório de anomalias gerado")
        return report
    
    def save_anomaly_data(self, df: DataFrame, output_path: str):
        """
        Salva dados com anomalias detectadas
        
        Args:
            df (DataFrame): DataFrame com anomalias
            output_path (str): Caminho para salvar
        """
        try:
            df.write.mode("overwrite").parquet(output_path)
            logger.info(f"Dados com anomalias salvos em: {output_path}")
        except Exception as e:
            logger.error(f"Erro ao salvar dados: {str(e)}")
            raise

def main():
    """Função principal para teste do módulo"""
    from extract import SparkDataExtractor
    from transform import SparkDataTransformer
    
    extractor = SparkDataExtractor()
    transformer = SparkDataTransformer(extractor.spark)
    detector = SparkAnomalyDetector(extractor.spark)
    
    try:
        # Carrega e transforma dados
        df = extractor.extract_sales_data()
        df_transformed = transformer.transform_sales_data(df)
        
        print(f"Dados carregados e transformados. Contagem: {df_transformed.count()}")
        
        # Detecta outliers estatísticos
        df_with_outliers = detector.detect_statistical_outliers(
            df_transformed, 
            ['total', 'quantity', 'price'],
            method='iqr'
        )
        
        # Detecta anomalias com K-Means
        feature_columns = ['quantity', 'price', 'total']
        df_with_anomalies = detector.detect_kmeans_anomalies(
            df_with_outliers,
            feature_columns
        )
        
        # Detecta anomalias temporais
        if 'date' in df_with_anomalies.columns:
            df_with_anomalies = detector.detect_temporal_anomalies(
                df_with_anomalies,
                'date',
                'total'
            )
        
        # Gera relatório
        report = detector.generate_anomaly_report(df_with_anomalies)
        print("Relatório de Anomalias:")
        for anomaly_type, stats in report.items():
            print(f"  {anomaly_type}: {stats['count']} ({stats['percentage']}%)")
        
        # Salva dados com anomalias detectadas
        detector.save_anomaly_data(df_with_anomalies, "data/sales_data_with_anomalies.parquet")
        print("Dados com anomalias salvos em: data/sales_data_with_anomalies.parquet")
        
        # Mostra algumas anomalias
        anomalies = df_with_anomalies.filter(col('is_outlier') | col('kmeans_anomaly'))
        print(f"Exemplos de anomalias detectadas:")
        anomalies.show(10)
        
    except FileNotFoundError:
        print("Arquivo de dados não encontrado. Execute primeiro o extract.py")
    
    finally:
        extractor.stop_spark()

if __name__ == "__main__":
    main()
