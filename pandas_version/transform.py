"""
Módulo de Transformação de Dados - Versão Pandas
Responsável por limpar, transformar e preparar dados para análise
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime, timedelta

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataTransformer:
    """Classe para transformação de dados usando pandas"""
    
    def __init__(self):
        """Inicializa o transformador de dados"""
        pass
    
    def clean_data(self, df: pd.DataFrame, 
                   remove_duplicates: bool = True,
                   handle_missing: str = 'drop') -> pd.DataFrame:
        """
        Limpa os dados removendo duplicatas e tratando valores ausentes
        
        Args:
            df (pd.DataFrame): DataFrame para limpeza
            remove_duplicates (bool): Se deve remover duplicatas
            handle_missing (str): Como tratar valores ausentes ('drop', 'fill', 'interpolate')
            
        Returns:
            pd.DataFrame: DataFrame limpo
        """
        df_clean = df.copy()
        
        # Remove duplicatas
        if remove_duplicates:
            initial_rows = len(df_clean)
            df_clean = df_clean.drop_duplicates()
            removed_rows = initial_rows - len(df_clean)
            if removed_rows > 0:
                logger.info(f"Removidas {removed_rows} linhas duplicadas")
        
        # Trata valores ausentes
        if handle_missing == 'drop':
            df_clean = df_clean.dropna()
        elif handle_missing == 'fill':
            # Preenche com valores apropriados baseado no tipo
            for col in df_clean.columns:
                if df_clean[col].dtype in ['int64', 'float64']:
                    df_clean[col] = df_clean[col].fillna(df_clean[col].median())
                else:
                    df_clean[col] = df_clean[col].fillna(df_clean[col].mode()[0] if not df_clean[col].mode().empty else 'Unknown')
        elif handle_missing == 'interpolate':
            df_clean = df_clean.interpolate()
        
        logger.info(f"Dados limpos. Shape original: {df.shape}, Shape final: {df_clean.shape}")
        return df_clean
    
    def add_calculated_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adiciona colunas calculadas ao DataFrame
        
        Args:
            df (pd.DataFrame): DataFrame base
            
        Returns:
            pd.DataFrame: DataFrame com colunas adicionais
        """
        df_transformed = df.copy()
        
        # Adiciona colunas de data/hora se existir coluna de data
        if 'Date' in df_transformed.columns:
            df_transformed['year'] = pd.to_datetime(df_transformed['Date']).dt.year
            df_transformed['month'] = pd.to_datetime(df_transformed['Date']).dt.month
            df_transformed['day_of_week'] = pd.to_datetime(df_transformed['Date']).dt.day_name()
            df_transformed['quarter'] = pd.to_datetime(df_transformed['Date']).dt.quarter
        
        # Adiciona colunas de vendas se existirem
        if 'money' in df_transformed.columns:
            # Categorização de vendas baseada no valor
            df_transformed['sales_category'] = pd.cut(
                df_transformed['money'],
                bins=[0, 30, 40, 50, float('inf')],
                labels=['Baixa', 'Média', 'Alta', 'Muito Alta']
            )
        
        logger.info("Colunas calculadas adicionadas com sucesso")
        return df_transformed
    
    def aggregate_data(self, df: pd.DataFrame, 
                      group_columns: List[str],
                      agg_functions: Dict[str, List[str]]) -> pd.DataFrame:
        """
        Agrega dados por grupos especificados
        
        Args:
            df (pd.DataFrame): DataFrame para agregação
            group_columns (List[str]): Colunas para agrupamento
            agg_functions (Dict[str, List[str]]): Funções de agregação por coluna
            
        Returns:
            pd.DataFrame: DataFrame agregado
        """
        try:
            df_agg = df.groupby(group_columns).agg(agg_functions).reset_index()
            
            # Flatten column names
            df_agg.columns = ['_'.join(col).strip() if col[1] else col[0] 
                             for col in df_agg.columns.values]
            
            logger.info(f"Dados agregados com sucesso. Shape: {df_agg.shape}")
            return df_agg
        except Exception as e:
            logger.error(f"Erro na agregação: {str(e)}")
            raise
    
    def pivot_data(self, df: pd.DataFrame,
                   index: str,
                   columns: str,
                   values: str,
                   fill_value: float = 0) -> pd.DataFrame:
        """
        Cria tabela pivot dos dados
        
        Args:
            df (pd.DataFrame): DataFrame base
            index (str): Coluna para índice
            columns (str): Coluna para colunas
            values (str): Coluna para valores
            fill_value (float): Valor para preencher células vazias
            
        Returns:
            pd.DataFrame: DataFrame pivotado
        """
        try:
            df_pivot = df.pivot_table(
                index=index,
                columns=columns,
                values=values,
                fill_value=fill_value,
                aggfunc='sum'
            )
            logger.info(f"Dados pivotados com sucesso. Shape: {df_pivot.shape}")
            return df_pivot
        except Exception as e:
            logger.error(f"Erro no pivot: {str(e)}")
            raise
    
    def filter_data(self, df: pd.DataFrame, 
                   filters: Dict[str, any]) -> pd.DataFrame:
        """
        Filtra dados baseado em condições
        
        Args:
            df (pd.DataFrame): DataFrame base
            filters (Dict[str, any]): Dicionário com filtros {coluna: valor}
            
        Returns:
            pd.DataFrame: DataFrame filtrado
        """
        df_filtered = df.copy()
        
        for column, value in filters.items():
            if column in df_filtered.columns:
                if isinstance(value, tuple) and len(value) == 2:
                    # Range filter (min, max)
                    df_filtered = df_filtered[
                        (df_filtered[column] >= value[0]) & 
                        (df_filtered[column] <= value[1])
                    ]
                elif isinstance(value, list):
                    # List filter
                    df_filtered = df_filtered[df_filtered[column].isin(value)]
                else:
                    # Exact match
                    df_filtered = df_filtered[df_filtered[column] == value]
        
        logger.info(f"Dados filtrados. Shape original: {df.shape}, Shape final: {df_filtered.shape}")
        return df_filtered
    
    def transform_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transformação específica para dados de vendas
        
        Args:
            df (pd.DataFrame): DataFrame de vendas
            
        Returns:
            pd.DataFrame: DataFrame transformado
        """
        # Limpa os dados
        df_clean = self.clean_data(df)
        
        # Adiciona colunas calculadas
        df_transformed = self.add_calculated_columns(df_clean)
        
        # Remove outliers usando IQR
        if 'money' in df_transformed.columns:
            Q1 = df_transformed['money'].quantile(0.25)
            Q3 = df_transformed['money'].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            initial_rows = len(df_transformed)
            df_transformed = df_transformed[
                (df_transformed['money'] >= lower_bound) & 
                (df_transformed['money'] <= upper_bound)
            ]
            removed_outliers = initial_rows - len(df_transformed)
            if removed_outliers > 0:
                logger.info(f"Removidos {removed_outliers} outliers")
        
        logger.info("Transformação de dados de vendas concluída")
        return df_transformed

def main():
    """Função principal para teste do módulo"""
    transformer = DataTransformer()
    
    # Exemplo de uso
    try:
        # Carrega dados de exemplo
        df = pd.read_csv('data/raw/Coffe_sales.csv')
        print(f"Dados carregados. Shape: {df.shape}")
        
        # Transforma os dados
        df_transformed = transformer.transform_sales_data(df)
        print(f"Dados transformados. Shape: {df_transformed.shape}")
        print(f"Colunas: {list(df_transformed.columns)}")
        
        # Exemplo de agregação
        if 'coffee_name' in df_transformed.columns and 'money' in df_transformed.columns:
            agg_functions = {
                'money': ['sum', 'mean', 'count']
            }
            df_agg = transformer.aggregate_data(
                df_transformed, 
                ['coffee_name'], 
                agg_functions
            )
            print(f"Dados agregados. Shape: {df_agg.shape}")
            print(df_agg.head())
            
            # Exemplo de pivot
            print("\n=== EXEMPLO DE PIVOT ===")
            try:
                # Pivot: coffee_name vs Time_of_Day, valores = money
                df_pivot = transformer.pivot_data(
                    df_transformed,
                    index='coffee_name',
                    columns='Time_of_Day', 
                    values='money'
                )
                print(f"Pivot criado com sucesso. Shape: {df_pivot.shape}")
                print("Pivot (coffee_name vs Time_of_Day):")
                print(df_pivot)
                
                # Outro exemplo de pivot: Weekday vs coffee_name
                df_pivot2 = transformer.pivot_data(
                    df_transformed,
                    index='Weekday',
                    columns='coffee_name',
                    values='money'
                )
                print(f"\nPivot 2 criado com sucesso. Shape: {df_pivot2.shape}")
                print("Pivot (Weekday vs coffee_name):")
                print(df_pivot2)
                
            except Exception as e:
                print(f"Erro ao criar pivot: {str(e)}")
                print("Verifique se as colunas existem no DataFrame")
        
    except FileNotFoundError:
        print("Arquivo de dados não encontrado. Execute primeiro o extract.py")

if __name__ == "__main__":
    main()
