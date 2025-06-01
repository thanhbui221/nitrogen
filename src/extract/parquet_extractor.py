"""
Extractor for reading data from Parquet files.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import to_timestamp, col
from common.base import BaseExtractor
from typing import Dict, Any, Optional, List, Union
from common.schema_utils import SchemaRegistry
from common.utils.logging_utils import get_logger

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
        self.logger = get_logger(__name__)
        
        # Validate required options
        if 'path' not in self.options:
            raise ValueError("'path' is required in options")
        
    def _convert_timestamp_columns(self, df: DataFrame, schema_def: Dict[str, Any]) -> DataFrame:
        """
        Convert string timestamp columns to proper timestamp type.
        
        Args:
            df: Input DataFrame
            schema_def: Schema definition containing field types
            
        Returns:
            DataFrame with converted timestamp columns
        """
        if not schema_def or 'fields' not in schema_def:
            return df
            
        for field in schema_def['fields']:
            if field['type'] == 'timestamp' and field['name'] in df.columns:
                # Check if the column is string/binary type
                if str(df.schema[field['name']].dataType) in ('StringType', 'BinaryType'):
                    self.logger.info(f"Converting column {field['name']} to timestamp")
                    df = df.withColumn(
                        field['name'],
                        to_timestamp(col(field['name']))
                    )
        return df
        
    def extract(self, spark: SparkSession) -> DataFrame:
        """
        Read data from Parquet file(s).
        
        Args:
            spark: Active SparkSession to use
            
        Returns:
            DataFrame containing the read data
        
        Raises:
            ValueError: If path is not provided or schema is invalid
            Exception: If reading fails
        """
        try:
            # Start with reader
            reader = spark.read.format("parquet")
            
            # Get schema if provided
            schema = self.options.get('schema')
            schema_def = None
            if schema and isinstance(schema, str):
                # Load schema from registry by name
                schema_registry = SchemaRegistry()
                schema_def = schema_registry.get_schema(schema)
                if schema_def is None:
                    raise ValueError(f"Schema '{schema}' not found in registry")
                self.logger.info(f"Loaded schema '{schema}' from registry")
                    
            # Read the data without schema first
            df = reader.load(self.options['path'])
            
            # Convert timestamp columns if needed
            if schema_def:
                df = self._convert_timestamp_columns(df, schema_def)
                
                # Now validate against the schema
                schema_registry = SchemaRegistry()
                if not schema_registry.validate_schema_compatibility(schema, df):
                    self.logger.warning(f"DataFrame schema does not match expected schema '{schema}'")
            
            # Select columns if specified
            columns = self.options.get('columns', [])
            if columns:
                df = df.select(*columns)
                
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read Parquet file from {self.options['path']}: {str(e)}")
            raise Exception(f"Failed to read Parquet file from {self.options['path']}: {str(e)}")
            
    @staticmethod
    def name() -> str:
        """Get the name of the extractor."""
        return "parquet" 