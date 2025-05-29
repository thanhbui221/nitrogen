from pyspark.sql import SparkSession
from src.common.config import JobConfig
from src.factory.component_factory import (
    extractor_factory,
    transformer_factory,
    loader_factory
)

class JobRunner:
    def __init__(self, config: JobConfig):
        self.config = config
        self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """Create a SparkSession with the configured settings"""
        builder = SparkSession.builder \
            .appName(self.config.spark_config.app_name) \
            .master(self.config.spark_config.master)

        # Apply additional configurations
        for key, value in self.config.spark_config.configs.items():
            builder = builder.config(key, value)

        return builder.getOrCreate()

    def run(self) -> None:
        """Execute the ETL job"""
        try:
            # Extract
            extractor = extractor_factory.create(
                self.config.source.type,
                self.config.source.options
            )
            df = extractor.extract(self.spark)

            # Transform
            for transform_config in self.config.transformations:
                transformer = transformer_factory.create(
                    transform_config.type,
                    transform_config.options
                )
                df = transformer.transform(df)

            # Load
            loader = loader_factory.create(
                self.config.target.type,
                self.config.target.options
            )
            loader.load(df)

        finally:
            self.spark.stop() 