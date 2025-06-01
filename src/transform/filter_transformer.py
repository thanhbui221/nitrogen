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
                - condition: SQL condition to filter rows. Supports complex conditions using:
                    - AND: for logical AND
                    - OR: for logical OR
                    - Parentheses () for grouping
                  Examples:
                    - "age > 25"
                    - "age > 25 AND salary > 50000"
                    - "(age > 25 OR experience > 5) AND salary > 50000"
        """
        super().__init__(options)
        if "condition" not in options:
            raise ValueError("Filter condition must be specified")
        self.condition = options["condition"]
    
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Filter DataFrame rows based on the specified condition.
        
        Args:
            df: Input DataFrame
            kwargs: Additional keyword arguments (not used by this transformer)
            
        Returns:
            Filtered DataFrame
        """
        return df.filter(self.condition)
    
    @property
    def name(self) -> str:
        """Get transformer name."""
        return "filter" 