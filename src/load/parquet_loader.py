"""
Loader for writing data to Parquet files.
"""

from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame
from common.base import BaseLoader

class ParquetLoader(BaseLoader):
    """Loader for writing data to Parquet files."""
    
    def __init__(self, options: Dict[str, Any]):
        """
        Initialize ParquetLoader.
        
        Args:
            options: Dictionary containing:
                - path: Output path for Parquet files
                - mode: Write mode (overwrite/append/error/ignore)
                - partition_by: Optional list of partition columns
                - compression: Compression codec (none/snappy/gzip/lzo)
                - partition_overwrite_mode: How to handle partition overwrites (dynamic/static)
        """
        super().__init__(options)
        self.path = options.get('path')
        if not self.path:
            raise ValueError("'path' is required in options")
            
        self.mode = options.get('mode', 'overwrite')
        if self.mode not in ['overwrite', 'append', 'error', 'ignore']:
            raise ValueError(
                f"Invalid mode '{self.mode}'. Must be one of: "
                "overwrite, append, error, ignore"
            )
            
        self.partition_by = options.get('partition_by', [])
        self.compression = options.get('compression', 'snappy')
        self.partition_overwrite_mode = options.get(
            'partition_overwrite_mode',
            'dynamic'
        )
        
    def load(self, df: DataFrame) -> None:
        """
        Write DataFrame to Parquet file(s).
        
        Args:
            df: DataFrame to write
            
        Raises:
            ValueError: If path is not provided
            Exception: If writing fails
        """
        try:
            # Configure writer
            writer = df.write \
                .format("parquet") \
                .mode(self.mode) \
                .option("compression", self.compression)
                
            # Set partition overwrite mode if partitioning
            if self.partition_by:
                writer = writer.option(
                    "partitionOverwriteMode",
                    self.partition_overwrite_mode
                )
                
            # Write with partitioning if specified
            if self.partition_by:
                writer.partitionBy(*self.partition_by) \
                    .save(self.path)
            else:
                writer.save(self.path)
                
        except Exception as e:
            raise Exception(
                f"Failed to write Parquet file to {self.path}: {str(e)}"
            )
            
    @staticmethod
    def name() -> str:
        """Get the name of the loader."""
        return "parquet" 