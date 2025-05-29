from src.common.base import BaseExtractor
from src.factory.component_factory import extractor_factory
from pyspark.sql import SparkSession, DataFrame

class CsvExtractor(BaseExtractor):
    def extract(self, spark: SparkSession) -> DataFrame:
        return spark.read.options(**self.options).csv(self.options["path"])

# Register the extractor
extractor_factory.register("csv", CsvExtractor) 