"""
Extractor for reading data from databases.
"""

from typing import Dict, Any
from pyspark.sql import DataFrame, SparkSession
from common.base import BaseExtractor

class DbExtractor(BaseExtractor):
    """Extractor for reading data from databases."""
    
    def __init__(self, options: Dict[str, Any]):
        """
        Initialize DbExtractor.
        
        Args:
            options: Configuration options including:
                - url: Database connection URL
                - table: Source table name
                - properties: Optional JDBC properties
        """
        super().__init__(options)
        
        # Validate required options
        if 'url' not in self.options:
            raise ValueError("'url' is required in options")
        if 'table' not in self.options:
            raise ValueError("'table' is required in options")
        
    def extract(self, spark: SparkSession) -> DataFrame:
        """
        Read data from database.
        
        Args:
            spark: Active SparkSession to use
            
        Returns:
            DataFrame containing the read data
            
        Raises:
            Exception: If reading fails
        """
        try:
            reader = spark.read \
                .format("jdbc") \
                .option("url", self.options['url']) \
                .option("dbtable", self.options['table'])
                
            # Add any additional properties
            for key, value in self.options.get('properties', {}).items():
                reader = reader.option(key, value)
                
            return reader.load()
            
        except Exception as e:
            raise Exception(f"Failed to read from database table {self.options['table']}: {str(e)}")
            
    @staticmethod
    def name() -> str:
        """Get the name of the extractor."""
        return "db" 