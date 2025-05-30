"""
Common utility functions for the ETL framework.
Import commonly used functions here for easy access.
"""

from .spark_utils import (
    create_spark_session,
    get_spark_session,
    dataframe_empty,
    count_by_partition
)

from .validation_utils import (
    validate_dataframe_schema,
    validate_column_values,
    check_null_values
)

from .date_utils import (
    parse_date,
    format_date,
    get_date_range,
    add_processing_date
)

from .string_utils import (
    clean_column_name,
    normalize_string,
    remove_special_chars
)

from .logging_utils import (
    setup_logging,
    get_logger
)

__all__ = [
    'create_spark_session',
    'get_spark_session',
    'dataframe_empty',
    'count_by_partition',
    'validate_dataframe_schema',
    'validate_column_values',
    'check_null_values',
    'parse_date',
    'format_date',
    'get_date_range',
    'add_processing_date',
    'clean_column_name',
    'normalize_string',
    'remove_special_chars',
    'setup_logging',
    'get_logger'
] 