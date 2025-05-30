"""
Utility functions for data validation and quality checks.
"""

from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull

def validate_dataframe_schema(df: DataFrame, expected_schema: Dict[str, str]) -> bool:
    """
    Validate if DataFrame schema matches expected schema.
    
    Args:
        df: DataFrame to validate
        expected_schema: Dictionary mapping column names to expected data types
        
    Returns:
        bool: True if schema matches, False otherwise
    """
    actual_schema = dict(df.dtypes)
    
    # Check if all expected columns exist with correct types
    for col_name, expected_type in expected_schema.items():
        if col_name not in actual_schema:
            return False
        if actual_schema[col_name].lower() != expected_type.lower():
            return False
            
    return True

def validate_column_values(
    df: DataFrame,
    column: str,
    valid_values: List[Any]
) -> DataFrame:
    """
    Validate that column values are within an allowed set.
    
    Args:
        df: DataFrame to validate
        column: Column name to check
        valid_values: List of allowed values
        
    Returns:
        DataFrame with invalid rows
    """
    return df.filter(~col(column).isin(valid_values))

def check_null_values(df: DataFrame, columns: Optional[List[str]] = None) -> Dict[str, int]:
    """
    Check for null values in specified columns.
    
    Args:
        df: DataFrame to check
        columns: Optional list of columns to check. If None, checks all columns.
        
    Returns:
        Dict mapping column names to count of null values
    """
    if columns is None:
        columns = df.columns
        
    null_counts = {}
    for column in columns:
        null_count = df.filter(
            isnull(col(column)) | isnan(col(column))
        ).count()
        if null_count > 0:
            null_counts[column] = null_count
            
    return null_counts

def validate_numeric_range(
    df: DataFrame,
    column: str,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None
) -> DataFrame:
    """
    Validate numeric values are within specified range.
    
    Args:
        df: DataFrame to validate
        column: Column name to check
        min_value: Optional minimum allowed value
        max_value: Optional maximum allowed value
        
    Returns:
        DataFrame with invalid rows
    """
    conditions = []
    
    if min_value is not None:
        conditions.append(col(column) < min_value)
        
    if max_value is not None:
        conditions.append(col(column) > max_value)
        
    if not conditions:
        return df.limit(0)  # Return empty DataFrame if no conditions
        
    from functools import reduce
    from pyspark.sql.functions import expr
    
    invalid_condition = reduce(lambda a, b: a | b, conditions)
    return df.filter(invalid_condition)

def validate_string_length(
    df: DataFrame,
    column: str,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None
) -> DataFrame:
    """
    Validate string length is within specified range.
    
    Args:
        df: DataFrame to validate
        column: Column name to check
        min_length: Optional minimum string length
        max_length: Optional maximum string length
        
    Returns:
        DataFrame with invalid rows
    """
    from pyspark.sql.functions import length
    
    conditions = []
    
    if min_length is not None:
        conditions.append(length(col(column)) < min_length)
        
    if max_length is not None:
        conditions.append(length(col(column)) > max_length)
        
    if not conditions:
        return df.limit(0)
        
    from functools import reduce
    invalid_condition = reduce(lambda a, b: a | b, conditions)
    return df.filter(invalid_condition) 