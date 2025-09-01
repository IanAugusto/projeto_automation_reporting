"""
Módulo de Detecção de Anomalias - Versão Pandas
Responsável por identificar padrões anômalos nos dados
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from scipy import stats
import matplotlib.pyplot as plt
import seaborn as sns

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnomalyDetector:
    """Classe para detecção de anomalias usando pandas e scikit-learn"""
    
    def __init__(self):
        """Inicializa o detector de anomalias"""
        self.scaler = StandardScaler()
        self.isolation_forest = IsolationForest(contamination=0.1, random_state=42)
    
    def detect_statistical_outliers(self, df: pd.DataFrame, 
                                  columns: List[str],
                                  method: str = 'iqr',
                                  threshold: float = 1.5) -> pd.DataFrame:
        """
        Detecta outliers usando métodos estatísticos
        
        Args:
            df (pd.DataFrame): DataFrame para análise
            columns (List[str]): Colunas para análise
            method (str): Método ('iqr', 'zscore', 'modified_zscore')
            threshold (float): Threshold para detecção
            
        Returns:
            pd.DataFrame: DataFrame com flag de outliers
        """
        df_result = df.copy()
        
        for column in columns:
            if column not in df_result.columns:
                continue
                
            if method == 'iqr':
                Q1 = df_result[column].quantile(0.25)
                Q3 = df_result[column].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR
                
                df_result[f'{column}_outlier'] = (
                    (df_result[column] < lower_bound) | 
                    (df_result[column] > upper_bound)
                )
                
            elif method == 'zscore':
                z_scores = np.abs(stats.zscore(df_result[column].dropna()))
                df_result[f'{column}_outlier'] = z_scores > threshold
                
            elif method == 'modified_zscore':
                median = df_result[column].median()
                mad = np.median(np.abs(df_result[column] - median))
                modified_z_scores = 0.6745 * (df_result[column] - median) / mad
                df_result[f'{column}_outlier'] = np.abs(modified_z_scores) > threshold
        
        outlier_columns = [col for col in df_result.columns if col.endswith('_outlier')]
        df_result['is_outlier'] = df_result[outlier_columns].any(axis=1)
        
        outlier_count = df_result['is_outlier'].sum()
        logger.info(f"Detectados {outlier_count} outliers usando método {method}")
        
        return df_result
    
    def detect_isolation_forest_anomalies(self, df: pd.DataFrame,
                                        feature_columns: List[str]) -> pd.DataFrame:
        """
        Detecta anomalias usando Isolation Forest
        
        Args:
            df (pd.DataFrame): DataFrame para análise
            feature_columns (List[str]): Colunas para usar como features
            
        Returns:
            pd.DataFrame: DataFrame com flag de anomalias
        """
        df_result = df.copy()
        
        # Prepara os dados
        features = df_result[feature_columns].dropna()
        
        if len(features) == 0:
            logger.warning("Nenhuma feature válida encontrada")
            return df_result
        
        # Normaliza os dados
        features_scaled = self.scaler.fit_transform(features)
        
        # Treina o modelo
        self.isolation_forest.fit(features_scaled)
        
        # Prediz anomalias
        predictions = self.isolation_forest.predict(features_scaled)
        anomaly_scores = self.isolation_forest.decision_function(features_scaled)
        
        # Adiciona resultados ao DataFrame
        df_result['isolation_forest_anomaly'] = False
        df_result['anomaly_score'] = 0.0
        
        valid_indices = features.index
        df_result.loc[valid_indices, 'isolation_forest_anomaly'] = predictions == -1
        df_result.loc[valid_indices, 'anomaly_score'] = anomaly_scores
        
        anomaly_count = df_result['isolation_forest_anomaly'].sum()
        logger.info(f"Detectadas {anomaly_count} anomalias usando Isolation Forest")
        
        return df_result
    
    def detect_temporal_anomalies(self, df: pd.DataFrame,
                                date_column: str,
                                value_column: str,
                                window: int = 7) -> pd.DataFrame:
        """
        Detecta anomalias temporais usando média móvel
        
        Args:
            df (pd.DataFrame): DataFrame com dados temporais
            date_column (str): Coluna de data
            value_column (str): Coluna de valores
            window (int): Janela para média móvel
            
        Returns:
            pd.DataFrame: DataFrame com flag de anomalias temporais
        """
        df_result = df.copy()
        
        # Ordena por data
        df_result = df_result.sort_values(date_column)
        
        # Calcula média móvel e desvio padrão
        df_result['rolling_mean'] = df_result[value_column].rolling(window=window).mean()
        df_result['rolling_std'] = df_result[value_column].rolling(window=window).std()
        
        # Detecta anomalias (valores fora de 2 desvios padrão)
        df_result['temporal_anomaly'] = (
            np.abs(df_result[value_column] - df_result['rolling_mean']) > 
            2 * df_result['rolling_std']
        )
        
        anomaly_count = df_result['temporal_anomaly'].sum()
        logger.info(f"Detectadas {anomaly_count} anomalias temporais")
        
        return df_result
    
    def detect_pattern_anomalies(self, df: pd.DataFrame,
                               group_columns: List[str],
                               value_column: str) -> pd.DataFrame:
        """
        Detecta anomalias em padrões de grupos
        
        Args:
            df (pd.DataFrame): DataFrame para análise
            group_columns (List[str]): Colunas para agrupamento
            value_column (str): Coluna de valores para análise
            
        Returns:
            pd.DataFrame: DataFrame com flag de anomalias de padrão
        """
        df_result = df.copy()
        
        # Calcula estatísticas por grupo
        group_stats = df_result.groupby(group_columns)[value_column].agg([
            'mean', 'std', 'count'
        ]).reset_index()
        
        # Merge com dados originais
        df_result = df_result.merge(
            group_stats, 
            on=group_columns, 
            suffixes=('', '_group')
        )
        
        # Detecta anomalias (valores fora de 2 desvios padrão do grupo)
        df_result['pattern_anomaly'] = (
            np.abs(df_result[value_column] - df_result['mean']) > 
            2 * df_result['std']
        )
        
        anomaly_count = df_result['pattern_anomaly'].sum()
        logger.info(f"Detectadas {anomaly_count} anomalias de padrão")
        
        return df_result
    
    def generate_anomaly_report(self, df: pd.DataFrame) -> Dict:
        """
        Gera relatório de anomalias detectadas
        
        Args:
            df (pd.DataFrame): DataFrame com flags de anomalias
            
        Returns:
            Dict: Relatório de anomalias
        """
        report = {}
        
        # Conta diferentes tipos de anomalias
        anomaly_columns = [col for col in df.columns if 'anomaly' in col or 'outlier' in col]
        
        for col in anomaly_columns:
            if col in df.columns:
                count = df[col].sum()
                percentage = (count / len(df)) * 100
                report[col] = {
                    'count': int(count),
                    'percentage': round(percentage, 2)
                }
        
        # Anomalias combinadas
        if len(anomaly_columns) > 1:
            combined_anomalies = df[anomaly_columns].any(axis=1)
            report['combined_anomalies'] = {
                'count': int(combined_anomalies.sum()),
                'percentage': round((combined_anomalies.sum() / len(df)) * 100, 2)
            }
        
        logger.info("Relatório de anomalias gerado")
        return report
    
    def plot_anomalies(self, df: pd.DataFrame,
                      x_column: str,
                      y_column: str,
                      anomaly_column: str,
                      title: str = "Detecção de Anomalias"):
        """
        Plota gráfico com anomalias destacadas
        
        Args:
            df (pd.DataFrame): DataFrame com dados
            x_column (str): Coluna para eixo X
            y_column (str): Coluna para eixo Y
            anomaly_column (str): Coluna com flag de anomalias
            title (str): Título do gráfico
        """
        plt.figure(figsize=(12, 8))
        
        # Dados normais
        normal_data = df[~df[anomaly_column]]
        plt.scatter(normal_data[x_column], normal_data[y_column], 
                   alpha=0.6, label='Normal', color='blue')
        
        # Dados anômalos
        anomaly_data = df[df[anomaly_column]]
        plt.scatter(anomaly_data[x_column], anomaly_data[y_column], 
                   alpha=0.8, label='Anomalia', color='red', s=100)
        
        plt.xlabel(x_column)
        plt.ylabel(y_column)
        plt.title(title)
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()

def main():
    """Função principal para teste do módulo"""
    detector = AnomalyDetector()
    
    try:
        # Carrega dados
        df = pd.read_csv('data/Coffe_sales.csv')
        print(f"Dados carregados. Shape: {df.shape}")
        
        # Detecta outliers estatísticos
        df_with_outliers = detector.detect_statistical_outliers(
            df, 
            ['money', 'hour_of_day'],
            method='iqr'
        )
        
        # Detecta anomalias com Isolation Forest
        feature_columns = ['money', 'hour_of_day', 'Weekdaysort', 'Monthsort']
        df_with_anomalies = detector.detect_isolation_forest_anomalies(
            df_with_outliers,
            feature_columns
        )
        
        # Detecta anomalias temporais (se coluna Date existir)
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df_with_anomalies = detector.detect_temporal_anomalies(
                df_with_anomalies,
                'Date',
                'money',
                window=7
            )
        
        # Detecta anomalias de padrão por tipo de café
        if 'coffee_name' in df.columns:
            df_with_anomalies = detector.detect_pattern_anomalies(
                df_with_anomalies,
                ['coffee_name'],
                'money'
            )
        
        # Gera relatório
        report = detector.generate_anomaly_report(df_with_anomalies)
        print("Relatório de Anomalias:")
        for anomaly_type, stats in report.items():
            print(f"  {anomaly_type}: {stats['count']} ({stats['percentage']}%)")
        
        # Mostra exemplos de anomalias detectadas
        print("\nExemplos de anomalias detectadas:")
        anomaly_columns = [col for col in df_with_anomalies.columns if 'anomaly' in col or 'outlier' in col]
        if anomaly_columns:
            # Mostra linhas com qualquer tipo de anomalia
            any_anomaly = df_with_anomalies[anomaly_columns].any(axis=1)
            anomaly_examples = df_with_anomalies[any_anomaly][['coffee_name', 'money', 'hour_of_day', 'Date'] + anomaly_columns].head(10)
            print(anomaly_examples.to_string(index=False))
        
        # Salva dados com anomalias detectadas
        df_with_anomalies.to_csv('data/Coffe_sales_with_anomalies.csv', index=False)
        print("\nDados com anomalias salvos em: data/Coffe_sales_with_anomalies.csv")
        
        # Exemplo de visualização (opcional - descomente para ver gráficos)
        # if 'money_outlier' in df_with_anomalies.columns:
        #     detector.plot_anomalies(
        #         df_with_anomalies,
        #         'hour_of_day',
        #         'money',
        #         'money_outlier',
        #         'Anomalias de Vendas por Hora do Dia'
        #     )
        
    except FileNotFoundError:
        print("Arquivo de dados não encontrado. Execute primeiro o extract.py")

if __name__ == "__main__":
    main()
