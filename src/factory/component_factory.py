from typing import Dict, Type, Any
from common.base import BaseExtractor, BaseTransformer, BaseLoader
from extract.parquet_extractor import ParquetExtractor
from transform.column_transformer import ColumnTransformer
from transform.filter_transformer import FilterTransformer
from transform.join_transformer import JoinTransformer
from transform.ca_account_parameter_value_transformer import CAAccountParameterValueTransformer
from load.parquet_loader import ParquetLoader

class ExtractorFactory:
    """Factory for creating extractor components."""
    
    def __init__(self):
        """Initialize the factory with empty registry."""
        self._extractors: Dict[str, Type[BaseExtractor]] = {}
        
    def register(self, name: str, extractor: Type[BaseExtractor]) -> None:
        """Register an extractor component."""
        self._extractors[name] = extractor
        
    def create(self, type_name: str, config: Dict[str, Any]) -> BaseExtractor:
        """Create an extractor instance."""
        if type_name not in self._extractors:
            raise ValueError(f"Unknown extractor type: {type_name}")
        return self._extractors[type_name](**config)

class TransformerFactory:
    """Factory for creating transformer components."""
    
    def __init__(self):
        """Initialize the factory with empty registry."""
        self._transformers: Dict[str, Type[BaseTransformer]] = {}
        
    def register(self, name: str, transformer: Type[BaseTransformer]) -> None:
        """Register a transformer component."""
        self._transformers[name] = transformer
        
    def create(self, type_name: str, config: Dict[str, Any]) -> BaseTransformer:
        """Create a transformer instance."""
        if type_name not in self._transformers:
            raise ValueError(f"Unknown transformer type: {type_name}")
        return self._transformers[type_name](**config)

class LoaderFactory:
    """Factory for creating loader components."""
    
    def __init__(self):
        """Initialize the factory with empty registry."""
        self._loaders: Dict[str, Type[BaseLoader]] = {}
        
    def register(self, name: str, loader: Type[BaseLoader]) -> None:
        """Register a loader component."""
        self._loaders[name] = loader
        
    def create(self, type_name: str, config: Dict[str, Any]) -> BaseLoader:
        """Create a loader instance."""
        if type_name not in self._loaders:
            raise ValueError(f"Unknown loader type: {type_name}")
        return self._loaders[type_name](**config)

# Create factory instances
extractor_factory = ExtractorFactory()
transformer_factory = TransformerFactory()
loader_factory = LoaderFactory()

# Register extractors
extractor_factory.register("parquet", ParquetExtractor)  # Register with name "parquet"

# Register transformers
transformer_factory.register("column", ColumnTransformer)  # For column operations
transformer_factory.register("filter", FilterTransformer)  # For filtering rows
transformer_factory.register("join", JoinTransformer)  # For joining DataFrames
transformer_factory.register("ca_account_parameter_value", CAAccountParameterValueTransformer)

# Register loaders
loader_factory.register("parquet", ParquetLoader)  # Register with name "parquet" 