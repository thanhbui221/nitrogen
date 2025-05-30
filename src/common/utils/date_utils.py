"""
Utility functions for date and time handling.
"""

from datetime import datetime, date, timedelta
from typing import List, Union, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, date_format

def parse_date(
    date_str: str,
    format: str = "%Y-%m-%d"
) -> Optional[date]:
    """
    Parse date string to date object.
    
    Args:
        date_str: Date string to parse
        format: Date format string
        
    Returns:
        Parsed date object or None if parsing fails
    """
    try:
        return datetime.strptime(date_str, format).date()
    except ValueError:
        return None

def format_date(
    dt: Union[date, datetime],
    format: str = "%Y-%m-%d"
) -> str:
    """
    Format date object to string.
    
    Args:
        dt: Date or datetime object to format
        format: Date format string
        
    Returns:
        Formatted date string
    """
    if isinstance(dt, datetime):
        dt = dt.date()
    return dt.strftime(format)

def get_date_range(
    start_date: Union[str, date],
    end_date: Union[str, date],
    format: str = "%Y-%m-%d"
) -> List[date]:
    """
    Get list of dates between start and end dates.
    
    Args:
        start_date: Start date (string or date object)
        end_date: End date (string or date object)
        format: Date format string for parsing if strings provided
        
    Returns:
        List of dates between start and end (inclusive)
    """
    if isinstance(start_date, str):
        start_date = parse_date(start_date, format)
    if isinstance(end_date, str):
        end_date = parse_date(end_date, format)
        
    if not (start_date and end_date):
        return []
        
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
        
    return dates

def add_processing_date(df: DataFrame) -> DataFrame:
    """
    Add processing date column to DataFrame.
    
    Args:
        df: DataFrame to modify
        
    Returns:
        DataFrame with processing_date column added
    """
    from pyspark.sql.functions import current_date
    
    return df.withColumn("processing_date", current_date())

def parse_timestamp_column(
    df: DataFrame,
    column: str,
    format: str = "yyyy-MM-dd HH:mm:ss"
) -> DataFrame:
    """
    Parse string timestamp column to timestamp type.
    
    Args:
        df: DataFrame containing timestamp column
        column: Column name to parse
        format: Timestamp format string
        
    Returns:
        DataFrame with parsed timestamp column
    """
    from pyspark.sql.functions import to_timestamp
    
    return df.withColumn(
        column,
        to_timestamp(col(column), format)
    )

def get_fiscal_year_dates(
    fiscal_year: int,
    fiscal_start_month: int = 4
) -> Tuple[date, date]:
    """
    Get start and end dates for a fiscal year.
    
    Args:
        fiscal_year: Fiscal year number
        fiscal_start_month: Month when fiscal year starts (default: 4 for April)
        
    Returns:
        Tuple of (start_date, end_date) for the fiscal year
    """
    if fiscal_start_month < 1 or fiscal_start_month > 12:
        raise ValueError("fiscal_start_month must be between 1 and 12")
        
    # Start date is fiscal_start_month of the fiscal year
    start_date = date(fiscal_year, fiscal_start_month, 1)
    
    # End date is day before fiscal_start_month of next fiscal year
    if fiscal_start_month == 1:
        end_date = date(fiscal_year, 12, 31)
    else:
        end_date = date(fiscal_year + 1, fiscal_start_month - 1, 1)
        # Get last day of the month
        next_month = end_date.replace(day=28) + timedelta(days=4)
        end_date = next_month - timedelta(days=next_month.day)
        
    return start_date, end_date 