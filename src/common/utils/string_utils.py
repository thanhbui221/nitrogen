"""
Utility functions for string manipulation and cleaning.
"""

import re
from typing import List, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, lower, trim, when, length, concat, lit

def clean_column_name(name: str) -> str:
    """
    Clean and normalize column names.
    
    Args:
        name: Column name to clean
        
    Returns:
        Cleaned column name
    """
    # Convert to lowercase
    name = name.lower()
    
    # Replace spaces and special characters with underscore
    name = re.sub(r'[^a-z0-9]+', '_', name)
    
    # Remove leading/trailing underscores
    name = name.strip('_')
    
    # Ensure name starts with letter (prepend 'col_' if not)
    if not name[0].isalpha():
        name = f'col_{name}'
        
    return name

def normalize_string(text: str) -> str:
    """
    Normalize string by removing special characters and extra whitespace.
    
    Args:
        text: String to normalize
        
    Returns:
        Normalized string
    """
    # Remove special characters
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    
    # Convert multiple spaces to single space
    text = re.sub(r'\s+', ' ', text)
    
    # Strip leading/trailing whitespace
    return text.strip()

def remove_special_chars(text: str, keep_chars: str = '') -> str:
    """
    Remove special characters from string.
    
    Args:
        text: String to clean
        keep_chars: String of characters to preserve
        
    Returns:
        Cleaned string
    """
    pattern = f'[^a-zA-Z0-9{re.escape(keep_chars)}]'
    return re.sub(pattern, '', text)

def clean_string_columns(
    df: DataFrame,
    columns: List[str],
    remove_special: bool = True,
    to_lower: bool = True
) -> DataFrame:
    """
    Clean specified string columns in DataFrame.
    
    Args:
        df: DataFrame to clean
        columns: List of column names to clean
        remove_special: Whether to remove special characters
        to_lower: Whether to convert to lowercase
        
    Returns:
        DataFrame with cleaned columns
    """
    for column in columns:
        # Start with trimmed column
        cleaned = trim(col(column))
        
        # Remove special characters if requested
        if remove_special:
            cleaned = regexp_replace(cleaned, '[^a-zA-Z0-9\\s]', '')
            
        # Convert to lowercase if requested
        if to_lower:
            cleaned = lower(cleaned)
            
        df = df.withColumn(column, cleaned)
        
    return df

def standardize_phone_numbers(
    df: DataFrame,
    phone_columns: List[str],
    country_code: str = '+1'
) -> DataFrame:
    """
    Standardize phone number format in specified columns.
    
    Args:
        df: DataFrame containing phone numbers
        phone_columns: List of column names containing phone numbers
        country_code: Default country code to prepend
        
    Returns:
        DataFrame with standardized phone numbers
    """
    def standardize_phone(col_name: str) -> col:
        # Remove all non-digit characters
        cleaned = regexp_replace(col(col_name), '[^0-9]', '')
        
        # Add country code if not present
        return when(
            length(cleaned) == 10,
            concat(lit(country_code), cleaned)
        ).otherwise(
            when(length(cleaned) > 10, cleaned)
            .otherwise(None)
        )
    
    for column in phone_columns:
        df = df.withColumn(column, standardize_phone(column))
        
    return df 