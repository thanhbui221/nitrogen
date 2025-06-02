"""
Unified transformer for all column operations.
"""

from common.base import BaseTransformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    expr, from_unixtime, to_timestamp, 
    split, regexp_replace, when, col,
    date_format, to_date
)
from pyspark.sql.types import TimestampType, LongType, StringType
from typing import Dict, Any, List
from common.utils.spark_utils import to_plain_decimal
import logging

logger = logging.getLogger(__name__)

class ColumnTransformer(BaseTransformer):
    """Unified transformer for all column operations."""
    
    VALID_TYPES = ['select', 'drop', 'rename', 'add_column', 'array', 'timestamp', 'plain_decimal']
    
    def __init__(self, options: Dict[str, Any]):
        """
        Initialize transformer.
        
        Args:
            options: Configuration options including:
                - type: Operation type (select/drop/rename/add_column/array/timestamp/plain_decimal)
                - columns: List/Dict of columns (for select/drop/rename/array/timestamp)
                - columns_to_cast: List of columns to convert to plain decimal
                - column_name: Name of new column (for add_column)
                - expression: SQL expression (for add_column)
                - precision: Timestamp precision (for timestamp)
                - format: Optional timestamp format (for timestamp)
                - input_type: Optional input type for timestamp ('unix' or 'timestamp', defaults to 'unix')
        """
        super().__init__(options)
        
        if 'type' not in self.options:
            raise ValueError("'type' is required in options")
            
        if self.options['type'] not in self.VALID_TYPES:
            raise ValueError(f"'type' must be one of: {', '.join(self.VALID_TYPES)}")
            
        # Validate type-specific required options
        operation_type = self.options['type']
        
        if operation_type in ['select', 'drop', 'array', 'timestamp']:
            if 'columns' not in self.options:
                raise ValueError("'columns' is required for this operation type")
                
        elif operation_type == 'rename':
            if 'columns' not in self.options:
                raise ValueError("'columns' mapping is required for rename operation")
                
        elif operation_type == 'add_column':
            if 'column_name' not in self.options:
                raise ValueError("'column_name' is required for add_column operation")
            if 'expression' not in self.options:
                raise ValueError("'expression' is required for add_column operation")
                
        elif operation_type == 'plain_decimal':
            if 'columns_to_cast' not in self.options:
                raise ValueError("'columns_to_cast' is required for plain_decimal operation")
                
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform DataFrame based on the specified operation type.
        
        Args:
            df: Input DataFrame
            kwargs: Additional arguments (not used)
            
        Returns:
            Transformed DataFrame
        """
        try:
            operation_type = self.options['type']
            
            if operation_type == 'select':
                return df.select(*self.options['columns'])
                
            elif operation_type == 'drop':
                return df.drop(*self.options['columns'])
                
            elif operation_type == 'rename':
                result_df = df
                for old_name, new_name in self.options['columns'].items():
                    result_df = result_df.withColumnRenamed(old_name, new_name)
                return result_df
                
            elif operation_type == 'add_column':
                return df.withColumn(
                    self.options['column_name'],
                    expr(self.options['expression'])
                )
                
            elif operation_type == 'array':
                result_df = df
                for column in self.options['columns']:
                    # Handle both bracketed and non-bracketed strings
                    cleaned = regexp_replace(col(column), r'[\[\]]', '')
                    # Split on comma and trim whitespace
                    result_df = result_df.withColumn(
                        column,
                        split(cleaned, ',\s*')
                    )
                return result_df
                
            elif operation_type == 'timestamp':
                result_df = df
                precision = self.options.get('precision', 'seconds')
                input_type = self.options.get('input_type', 'unix')
                output_format = self.options.get('format')
                
                for column in self.options['columns']:
                    # Get the column's data type
                    col_type = df.schema[column].dataType
                    
                    # Convert to timestamp based on input type
                    if input_type == 'unix' and isinstance(col_type, (LongType, StringType)):
                        # Input is a Unix timestamp
                        if precision == 'seconds':
                            result_df = result_df.withColumn(
                                column,
                                from_unixtime(col(column))
                            )
                        else:  # milliseconds
                            result_df = result_df.withColumn(
                                column,
                                from_unixtime(col(column) / 1000)
                            )
                    
                    # If output format is specified, format the timestamp
                    if output_format:
                        result_df = result_df.withColumn(
                            column,
                            date_format(col(column), output_format)
                        )
                        
                return result_df
                
            elif operation_type == 'plain_decimal':
                result_df = df
                logger.info("Converting decimal columns to normalized string format")
                for column in self.options['columns']:
                    if column in df.columns:
                        result_df = result_df.withColumn(
                            column,
                            to_plain_decimal(col(column))
                        )
                        null_count = result_df.filter(col(column).isNull()).count()
                        if null_count > 0:
                            logger.warning(f"Found {null_count} null values in column {column}")
                return result_df

            elif operation_type == 'to_date_format':
                result_df = df
                logger.info("Converting date columns to date type")
                format = self.options.get('format', "yyyy-MM-dd")
                for column in self.options['columns']:
                    result_df = result_df.withColumn(
                        column,
                        to_date(split(col(column), ' ')[0], format)
                    )
                return result_df

            
        except Exception as e:
            raise Exception(f"Failed to apply {self.options['type']} operation: {str(e)}")
            
    @property
    def name(self) -> str:
        """Get transformer name."""
        return "column" 