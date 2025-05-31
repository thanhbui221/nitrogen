from pyspark.sql import SparkSession
from common.config import JobConfig, ComponentConfig
from factory.component_factory import (
    extractor_factory,
    transformer_factory,
    loader_factory
)
import logging
import os

logger = logging.getLogger(__name__)

class JobRunner:
    def __init__(self, config: JobConfig):
        self.config = config
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self) -> SparkSession:
        """Create a Spark session with the specified configuration"""
        builder = SparkSession.builder.appName(self.config.name)
        
        # Add default configurations for Windows
        builder = builder.config("spark.driver.host", "localhost")
        
        # Configure temporary directory using workspace root
        temp_dir = os.path.join(os.getcwd(), "data", "temp")
        builder = builder.config("spark.local.dir", temp_dir)
        
        # Use built-in Java implementation instead of native one
        builder = builder.config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        
        # Add any additional configurations from the config file
        if self.config.spark_config and self.config.spark_config.configs:
            for key, value in self.config.spark_config.configs.items():
                builder = builder.config(key, value)
        
        return builder.getOrCreate()
        
    def _create_component(self, config: ComponentConfig, factory):
        """Create a component using its factory."""
        return factory.create(config.type, {"options": config.options})
        
    def run(self) -> None:
        """Run the ETL job"""
        try:
            # Extract from source
            logger.info(f"Extracting data from source: {self.config.extract.type}")
            extractor = self._create_component(self.config.extract, extractor_factory)
            df = extractor.extract(self.spark)
            
            # Load dependencies
            dependency_dfs = {}
            for dep_name, dep_config in self.config.dependencies.items():
                logger.info(f"Loading dependency: {dep_name}")
                dep_extractor = self._create_component(dep_config, extractor_factory)
                dependency_dfs[dep_name] = dep_extractor.extract(self.spark)
                
                # Apply any transformations specified in dependency config
                if 'transform' in dep_config.options:
                    for transform_config in dep_config.options['transform']:
                        # Get required dependencies for this transformation
                        transform_kwargs = {}
                        if 'requires' in transform_config['options']:
                            for required_dep in transform_config['options']['requires']:
                                if required_dep not in dependency_dfs:
                                    raise ValueError(f"Required dependency {required_dep} not found")
                                # Pass the dependency with its original name
                                transform_kwargs[required_dep] = dependency_dfs[required_dep]
                        
                        transformer = transformer_factory.create(
                            transform_config['type'],
                            {"options": transform_config['options']}
                        )
                        dependency_dfs[dep_name] = transformer.transform(
                            dependency_dfs[dep_name],
                            **transform_kwargs
                        )
            
            # Apply transformations
            for transform_config in self.config.transform:
                logger.info(f"Applying transformation: {transform_config.type}")
                transformer = self._create_component(transform_config, transformer_factory)
                
                # Pass all dependencies to every transformer
                # Each transformer can decide which ones it needs
                transform_kwargs = dependency_dfs
                
                df = transformer.transform(df, **transform_kwargs)
            
            # Load to target
            logger.info(f"Loading data to target: {self.config.load.type}")
            loader = self._create_component(self.config.load, loader_factory)
            loader.load(df)
            
            logger.info("Job completed successfully")
            
        except Exception as e:
            logger.error(f"Error in job execution: {str(e)}")
            raise
        finally:
            self.spark.stop() 