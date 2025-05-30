from abc import ABC, abstractmethod
from typing import Dict, Any
from pyspark.sql import DataFrame, SparkSession

class BaseComponent(ABC):
    """Base class for all ETL components."""
    
    def __init__(self, options: Dict[str, Any]):
        """Initialize with options dictionary."""
        self.options = options

class BaseExtractor(BaseComponent):
    """Base class for data extractors."""
    
    @abstractmethod
    def extract(self, spark: SparkSession) -> DataFrame:
        """
        Extract data from source.
        
        Args:
            spark: Active SparkSession to use
            
        Returns:
            DataFrame containing the extracted data
        """
        pass

class BaseTransformer(BaseComponent):
    """Base class for data transformers."""
    
    @abstractmethod
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Transform the input DataFrame.
        
        Args:
            df: Input DataFrame to transform
            **kwargs: Additional arguments (e.g. dependency DataFrames)
            
        Returns:
            Transformed DataFrame
        """
        pass

class BaseLoader(BaseComponent):
    """Base class for data loaders."""
    
    @abstractmethod
    def load(self, df: DataFrame) -> None:
        """
        Load data to target.
        
        Args:
            df: DataFrame to load
        """
        pass 