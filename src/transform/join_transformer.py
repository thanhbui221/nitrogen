"""
Transformer for joining multiple DataFrames.
"""

from typing import Dict, Any, List
from pyspark.sql import DataFrame
from common.base import BaseTransformer

class JoinTransformer(BaseTransformer):
    """Transformer for joining multiple DataFrames in sequence."""
    
    def __init__(self, options: Dict[str, Any]):
        """
        Initialize transformer.
        
        Args:
            options: Configuration options including:
                - joins: List of join configurations, each containing:
                    - right_df: Name of the right DataFrame to join with
                    - join_type: Type of join (inner, left, right, full)
                    - join_conditions: List of join conditions, each with:
                        - left: Column name from left DataFrame
                        - right: Column name from right DataFrame
                    - select: Optional list of columns to keep after join
        """
        super().__init__(options)
        
        # Validate required options
        if 'joins' not in self.options:
            raise ValueError("'joins' list is required in options")
            
        # Validate each join configuration
        for i, join_config in enumerate(self.options['joins']):
            required = ['right_df', 'join_type', 'join_conditions']
            for opt in required:
                if opt not in join_config:
                    raise ValueError(f"'{opt}' is required in join configuration {i}")
            
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Apply multiple joins in sequence according to configuration.
        
        Args:
            df: Initial DataFrame
            kwargs: Additional arguments including all dependency DataFrames
            
        Returns:
            Final joined DataFrame
            
        Raises:
            Exception: If transformation fails
        """
        try:
            result_df = df
            
            # Apply each join in sequence
            for join_config in self.options['joins']:
                right_df_name = join_config['right_df']
                if right_df_name not in kwargs:
                    raise ValueError(f"Right DataFrame '{right_df_name}' not found in kwargs")
                    
                right_df = kwargs[right_df_name]
                join_conditions = []
                
                for cond in join_config['join_conditions']:
                    join_conditions.append(
                        result_df[cond['left']] == right_df[cond['right']]
                    )
                    
                result_df = result_df.join(
                    right_df,
                    join_conditions,
                    join_config['join_type']
                )
                
                # Optionally select specific columns after join
                if 'select' in join_config:
                    result_df = result_df.select(*join_config['select'])
            
            return result_df
            
        except Exception as e:
            raise Exception(f"Failed to join DataFrames: {str(e)}")
            
    @staticmethod
    def name() -> str:
        """Get transformer name."""
        return "join" 