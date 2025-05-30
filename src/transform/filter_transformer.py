"""
Transformer for filtering DataFrame rows based on conditions.
"""

from common.base import BaseTransformer
from pyspark.sql import DataFrame
from typing import Dict, Any

class FilterTransformer(BaseTransformer):
    """Transformer for filtering DataFrame rows."""
    
    def __init__(self, options: Dict[str, Any]):
        """
        Initialize transformer.
        
        Args:
            options: Configuration options including:
                - condition: SQL condition to filter rows
        """
        super().__init__(options)
        
        if 'condition' not in self.options:
            raise ValueError("'condition' is required in options")
            
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Filter DataFrame rows based on condition.
        
        Args:
            df: Input DataFrame
            kwargs: Additional arguments (not used)
            
        Returns:
            DataFrame with filtered rows
        """
        try:
            return df.filter(self.options['condition'])
        except Exception as e:
            raise Exception(f"Failed to filter DataFrame: {str(e)}")
            
    @staticmethod
    def name() -> str:
        return "filter" 