"""
Extractor for reading data from Parquet files.
"""

from pyspark.sql import DataFrame, SparkSession
from common.base import BaseExtractor
from typing import Dict, Any, Optional, List, Union

class ParquetExtractor(BaseExtractor):
    """Extractor for reading data from Parquet files."""
    
    def __init__(self, options: Dict[str, Any]):
        """
        Initialize ParquetExtractor.
        
        Args:
            options: Configuration options including:
                - path: Path to Parquet file or directory
                - schema: Optional schema name or definition
                - columns: Optional list of columns to select
        """
        super().__init__(options)
        
        # Validate required options
        if 'path' not in self.options:
            raise ValueError("'path' is required in options")
        
    def extract(self, spark: SparkSession) -> DataFrame:
        """
        Read data from Parquet file(s).
        
        Args:
            spark: Active SparkSession to use
            
        Returns:
            DataFrame containing the read data
        
        Raises:
            ValueError: If path is not provided
            Exception: If reading fails
        """
        try:
            # Start with reader
            reader = spark.read.format("parquet")
            
            # Apply schema if provided
            schema = self.options.get('schema')
            if schema:
                if isinstance(schema, str):
                    # TODO: Load schema from registry by name
                    pass
                else:
                    reader = reader.schema(schema)
                    
            # Read the data
            df = reader.load(self.options['path'])
            
            # Select columns if specified
            columns = self.options.get('columns', [])
            if columns:
                df = df.select(*columns)
                
            return df
            
        except Exception as e:
            raise Exception(f"Failed to read Parquet file from {self.options['path']}: {str(e)}")
            
    @staticmethod
    def name() -> str:
        """Get the name of the extractor."""
        return "parquet" 