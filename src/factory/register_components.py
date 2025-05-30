"""
Register all available components with the factories
"""

from factory.component_factory import extractor_factory, transformer_factory, loader_factory
from extract.parquet_extractor import ParquetExtractor
from extract.csv_extractor import CsvExtractor
from extract.db_extractor import DbExtractor
from transform.ca_account_parameter_value_transformer import ParameterValueTransformer
from transform.column_transformer import ColumnTransformer
from transform.filter_transformer import FilterTransformer
from transform.join_transformer import JoinTransformer
from load.parquet_loader import ParquetLoader
from load.db_loader import DbLoader

def register_components():
    """Register all available components with their respective factories"""
    
    # Register extractors
    extractor_factory.register("parquet", ParquetExtractor)
    extractor_factory.register("csv", CsvExtractor)
    extractor_factory.register("db", DbExtractor)
    
    # Register transformers
    transformer_factory.register("column", ColumnTransformer)  # For column operations
    transformer_factory.register("filter", FilterTransformer)  # For filtering rows
    transformer_factory.register("join", JoinTransformer)  # For joining DataFrames
    transformer_factory.register("parameter_value", ParameterValueTransformer)
    
    # Register loaders
    loader_factory.register("parquet", ParquetLoader)
    loader_factory.register("db", DbLoader) 