from src.common.base import BaseTransformer
from src.factory.component_factory import transformer_factory
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, col

class ColumnRenameTransformer(BaseTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        mappings = self.options.get("mappings", {})
        for old_name, new_name in mappings.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df

class FilterTransformer(BaseTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        condition = self.options.get("condition")
        if condition:
            return df.filter(condition)
        return df

class AddColumnTransformer(BaseTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        column_name = self.options.get("column_name")
        expression = self.options.get("expression")
        if column_name and expression:
            return df.withColumn(column_name, expr(expression))
        return df

# Register transformers
transformer_factory.register("column_rename", ColumnRenameTransformer)
transformer_factory.register("filter", FilterTransformer)
transformer_factory.register("add_column", AddColumnTransformer) 