from abc import ABC, abstractmethod
from typing import Dict, Any
from pyspark.sql import DataFrame, SparkSession

class BaseComponent(ABC):
    def __init__(self, options: Dict[str, Any]):
        self.options = options

class BaseExtractor(BaseComponent):
    @abstractmethod
    def extract(self, spark: SparkSession) -> DataFrame:
        """Extract data from source"""
        pass

class BaseTransformer(BaseComponent):
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform the input DataFrame"""
        pass

class BaseLoader(BaseComponent):
    @abstractmethod
    def load(self, df: DataFrame) -> None:
        """Load data to target"""
        pass 