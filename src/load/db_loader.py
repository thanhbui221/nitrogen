"""
Loader for writing data to databases.
"""

from typing import Dict, Any
from pyspark.sql import DataFrame
from common.base import BaseLoader

class DbLoader(BaseLoader):
    """Loader for writing data to databases."""
    
    def __init__(self, options: Dict[str, Any]):
        """
        Initialize DbLoader.
        
        Args:
            options: Configuration options including:
                - url: Database connection URL
                - table: Target table name
                - mode: Write mode (overwrite/append/error/ignore)
                - properties: Optional JDBC properties
        """
        super().__init__(options)
        
        # Validate required options
        if 'url' not in self.options:
            raise ValueError("'url' is required in options")
        if 'table' not in self.options:
            raise ValueError("'table' is required in options")
        
    def load(self, df: DataFrame) -> None:
        """
        Write DataFrame to database.
        
        Args:
            df: DataFrame to write
            
        Raises:
            Exception: If writing fails
        """
        try:
            writer = df.write \
                .format("jdbc") \
                .option("url", self.options['url']) \
                .option("dbtable", self.options['table']) \
                .mode(self.options.get('mode', 'overwrite'))
                
            # Add any additional properties
            for key, value in self.options.get('properties', {}).items():
                writer = writer.option(key, value)
                
            writer.save()
            
        except Exception as e:
            raise Exception(f"Failed to write to database table {self.options['table']}: {str(e)}")
            
    @staticmethod
    def name() -> str:
        """Get the name of the loader."""
        return "db" 