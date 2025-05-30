"""
Spark-related utility functions for session management and DataFrame operations.
"""

from typing import Optional, Dict, Any
from decimal import Decimal, InvalidOperation
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, udf, split, regexp_replace, array
from pyspark.sql.types import StringType

_spark_session = None

def create_spark_session(
    app_name: str,
    config: Optional[Dict[str, Any]] = None
) -> SparkSession:
    """
    Create a new Spark session with the given configuration.
    
    Args:
        app_name: Name of the Spark application
        config: Optional dictionary of Spark configuration options
        
    Returns:
        SparkSession: Configured Spark session
    """
    global _spark_session
    
    if _spark_session:
        return _spark_session
        
    builder = SparkSession.builder.appName(app_name)
    
    # Apply custom configurations
    if config:
        for key, value in config.items():
            builder = builder.config(key, value)
            
    _spark_session = builder.getOrCreate()
    return _spark_session

def get_spark_session() -> Optional[SparkSession]:
    """
    Get the current Spark session if it exists.
    
    Returns:
        Optional[SparkSession]: Current Spark session or None if not initialized
    """
    return _spark_session

def dataframe_empty(df) -> bool:
    """
    Check if a DataFrame is empty.
    
    Args:
        df: Spark DataFrame to check
        
    Returns:
        bool: True if DataFrame is empty, False otherwise
    """
    return df.rdd.isEmpty()

def count_by_partition(df) -> Dict[int, int]:
    """
    Count records in each partition of a DataFrame.
    
    Args:
        df: Spark DataFrame to analyze
        
    Returns:
        Dict[int, int]: Dictionary mapping partition index to record count
    """
    def count_partition(iterator):
        count = 0
        for _ in iterator:
            count += 1
        yield count

    counts = df.rdd.mapPartitionsWithIndex(
        lambda idx, it: [(idx, sum(1 for _ in it))]
    ).collect()
    
    return dict(counts)

def add_audit_columns(df, process_id: str):
    """
    Add audit columns to a DataFrame.
    
    Args:
        df: Spark DataFrame to modify
        process_id: Unique identifier for the current process
        
    Returns:
        DataFrame with audit columns added
    """
    from pyspark.sql.functions import current_timestamp, lit
    
    return df.withColumn("process_id", lit(process_id)) \
            .withColumn("processed_at", current_timestamp())

def repartition_df(df, partition_cols=None, num_partitions=None):
    """
    Repartition a DataFrame optimally.
    
    Args:
        df: Spark DataFrame to repartition
        partition_cols: Optional list of columns to partition by
        num_partitions: Optional number of partitions
        
    Returns:
        Repartitioned DataFrame
    """
    if partition_cols and num_partitions:
        return df.repartition(num_partitions, *partition_cols)
    elif partition_cols:
        return df.repartition(*partition_cols)
    elif num_partitions:
        return df.repartition(num_partitions)
    return df 

def to_plain_decimal(column):
    """
    Convert decimal column to plain string format.
    
    Args:
        column: Column to convert to decimal string format
        
    Returns:
        Column: Spark Column with decimal values converted to normalized string format
    """
    @udf(returnType=StringType())
    def normalize_decimal(value):
        if value is None:
            return None
        try:
            return format(Decimal(str(value)).normalize(), 'f')
        except (InvalidOperation, ValueError, TypeError):
            return None
            
    return when(column.isNotNull(), normalize_decimal(column)).otherwise(None) 

def string_to_array(column, delimiter=","):
    """
    Convert a string column to an array type.
    Handles:
    - Removing brackets
    - Normalizing spacing around delimiters
    - Converting nulls to empty arrays
    
    Args:
        column: Column to convert
        delimiter: Delimiter used in the string (default: comma)
        
    Returns:
        Column: Array type column
    """
    return when(
        column.isNotNull(),
        split(
            # First remove brackets
            regexp_replace(
                # Then normalize spacing around delimiters
                regexp_replace(
                    regexp_replace(column, r"^\[|\]$", ""),  # remove brackets
                    r"\\s*,\\s*", ","  # normalize spacing around commas
                ),
                # Remove any remaining whitespace
                r"^\\s+|\\s+$", ""
            ),
            delimiter
        )
    ).otherwise(array())  # return empty array if null 