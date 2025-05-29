from src.common.base import BaseLoader
from src.factory.component_factory import loader_factory
from pyspark.sql import DataFrame

class ParquetLoader(BaseLoader):
    def load(self, df: DataFrame) -> None:
        writer = df.write.mode(self.options.get("mode", "overwrite"))
        
        partition_by = self.options.get("partitionBy", [])
        if partition_by:
            writer = writer.partitionBy(*partition_by)
            
        writer.parquet(self.options["path"])

# Register the loader
loader_factory.register("parquet", ParquetLoader) 