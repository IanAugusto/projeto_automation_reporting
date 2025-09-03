"""
Módulo de Extração de Dados - Versão Pandas
Responsável por extrair dados de diferentes fontes (CSV, banco de dados, APIs)
"""

import pandas as pd
import os
from typing import Optional, Dict, Any
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataExtractor:
    """Classe para extração de dados usando pandas"""
    
    def __init__(self, data_path: str = "data/raw/"):
        """
        Inicializa o extrator de dados
        
        Args:
            data_path (str): Caminho para o diretório de dados raw
        """
        self.data_path = data_path
        self.ensure_data_directory()
    
    def ensure_data_directory(self):
        """Garante que o diretório de dados existe"""
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)
            logger.info(f"Diretório de dados criado: {self.data_path}")
    
    def extract_from_csv(self, filename: str, **kwargs) -> pd.DataFrame:
        """
        Extrai dados de um arquivo CSV
        
        Args:
            filename (str): Nome do arquivo CSV
            **kwargs: Argumentos adicionais para pd.read_csv()
            
        Returns:
            pd.DataFrame: DataFrame com os dados extraídos
        """
        filepath = os.path.join(self.data_path, filename)
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Arquivo não encontrado: {filepath}")
        
        try:
            df = pd.read_csv(filepath, **kwargs)
            logger.info(f"Dados extraídos com sucesso de {filename}. Shape: {df.shape}")
            return df
        except Exception as e:
            logger.error(f"Erro ao extrair dados de {filename}: {str(e)}")
            raise
    
    def extract_from_database(self, connection_string: str, query: str) -> pd.DataFrame:
        """
        Extrai dados de um banco de dados
        
        Args:
            connection_string (str): String de conexão com o banco
            query (str): Query SQL para extração
            
        Returns:
            pd.DataFrame: DataFrame com os dados extraídos
        """
        try:
            df = pd.read_sql(query, connection_string)
            logger.info(f"Dados extraídos do banco de dados. Shape: {df.shape}")
            return df
        except Exception as e:
            logger.error(f"Erro ao extrair dados do banco: {str(e)}")
            raise
    
    def extract_sales_data(self, filename: str = "Coffe_sales.csv") -> pd.DataFrame:
        """
        Extrai dados de vendas de café (exemplo específico)
        
        Args:
            filename (str): Nome do arquivo de vendas de café
            
        Returns:
            pd.DataFrame: DataFrame com dados de vendas de café
        """
        return self.extract_from_csv(
            filename,
            parse_dates=['Date'],
            index_col=None
        )
    
    def get_data_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Retorna informações sobre o DataFrame
        
        Args:
            df (pd.DataFrame): DataFrame para análise
            
        Returns:
            Dict[str, Any]: Informações do DataFrame
        """
        return {
            'shape': df.shape,
            'columns': list(df.columns),
            'dtypes': df.dtypes.to_dict(),
            'null_counts': df.isnull().sum().to_dict(),
            'memory_usage': df.memory_usage(deep=True).sum()
        }

def main():
    """Função principal para teste do módulo"""
    extractor = DataExtractor()
    
    # Exemplo de uso
    try:
        # Tenta extrair dados de vendas
        sales_df = extractor.extract_sales_data()
        print("Dados de vendas extraídos com sucesso!")
        print(f"Shape: {sales_df.shape}")
        print(f"Colunas: {list(sales_df.columns)}")
        
        # Informações do DataFrame
        info = extractor.get_data_info(sales_df)
        print(f"Informações do DataFrame: {info}")
        
    except FileNotFoundError:
        print("Arquivo de dados não encontrado. Criando arquivo de exemplo...")
        # Criar dados de exemplo de café
        sample_data = pd.DataFrame({
            'hour_of_day': [10, 12, 13, 15, 16] * 20,
            'cash_type': ['card', 'cash'] * 50,
            'money': [38.7, 28.9, 33.8, 25.5, 42.0] * 20,
            'coffee_name': ['Latte', 'Americano', 'Hot Chocolate', 'Cappuccino', 'Espresso'] * 20,
            'Time_of_Day': ['Morning', 'Afternoon', 'Evening'] * 33 + ['Morning'],
            'Weekday': ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'] * 20,
            'Month_name': ['Jan', 'Feb', 'Mar', 'Apr', 'May'] * 20,
            'Weekdaysort': [1, 2, 3, 4, 5] * 20,
            'Monthsort': [1, 2, 3, 4, 5] * 20,
            'Date': pd.date_range('2024-01-01', periods=100),
            'Time': pd.date_range('2024-01-01 08:00:00', periods=100, freq='H').strftime('%H:%M:%S')
        })
        sample_data.to_csv('data/raw/Coffe_sales.csv', index=False)
        print("Arquivo de exemplo criado: data/raw/Coffe_sales.csv")

if __name__ == "__main__":
    main()
