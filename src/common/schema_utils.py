import json
from typing import Dict, Any, Optional
from pathlib import Path
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DateType, DecimalType, BooleanType
)

class SchemaRegistry:
    """Registry for managing and accessing data schemas"""
    
    def __init__(self, schema_dir: str = "schemas"):
        self.schema_dir = Path(schema_dir)
        self._schemas: Dict[str, Dict[str, Any]] = {}
        self._spark_schemas: Dict[str, StructType] = {}
        self._load_schemas()

    def _load_schemas(self) -> None:
        """Load all schema definitions from the schema directory"""
        if not self.schema_dir.exists():
            raise ValueError(f"Schema directory not found: {self.schema_dir}")
            
        for schema_file in self.schema_dir.glob("*.json"):
            with open(schema_file) as f:
                schema = json.load(f)
                self._schemas[schema["name"]] = schema

    def _convert_field_type(self, field_type: str) -> Any:
        """Convert JSON schema type to Spark SQL type"""
        type_mapping = {
            "string": StringType(),
            "timestamp": TimestampType(),
            "date": DateType(),
            "boolean": BooleanType()
        }
        
        if field_type in type_mapping:
            return type_mapping[field_type]
        elif field_type.startswith("decimal"):
            precision, scale = map(int, field_type[8:-1].split(","))
            return DecimalType(precision, scale)
        elif field_type == "struct":
            return None  # Handled separately
        else:
            raise ValueError(f"Unsupported field type: {field_type}")

    def _create_struct_type(self, fields: list) -> StructType:
        """Create a StructType from field definitions"""
        spark_fields = []
        for field in fields:
            if field["type"] == "struct":
                field_type = self._create_struct_type(field["fields"])
            else:
                field_type = self._convert_field_type(field["type"])
            
            spark_fields.append(
                StructField(
                    field["name"],
                    field_type,
                    field.get("nullable", True)
                )
            )
        return StructType(spark_fields)

    def get_schema(self, name: str) -> Optional[Dict[str, Any]]:
        """Get the raw schema definition"""
        return self._schemas.get(name)

    def get_spark_schema(self, name: str) -> Optional[StructType]:
        """Get the Spark SQL schema for a given schema name"""
        if name not in self._spark_schemas:
            schema_def = self.get_schema(name)
            if not schema_def:
                return None
            self._spark_schemas[name] = self._create_struct_type(schema_def["fields"])
        return self._spark_schemas[name]

    def get_partition_columns(self, name: str) -> list:
        """Get partition columns for a schema"""
        schema = self.get_schema(name)
        return schema.get("partitionColumns", []) if schema else []

    def validate_schema_compatibility(self, name: str, df) -> bool:
        """Validate if a DataFrame is compatible with a schema"""
        expected_schema = self.get_spark_schema(name)
        if not expected_schema:
            return False
            
        df_fields = {f.name: f for f in df.schema.fields}
        expected_fields = {f.name: f for f in expected_schema.fields}
        
        # Check if all required fields are present with correct types
        for name, field in expected_fields.items():
            if name not in df_fields:
                return False
            if not field.nullable and df_fields[name].nullable:
                return False
            if field.dataType != df_fields[name].dataType:
                return False
                
        return True

# Example usage in a job:
"""
from src.common.schema_utils import SchemaRegistry

# Initialize schema registry
schema_registry = SchemaRegistry()

# Reading with schema validation
df = spark.read.format("csv") \
    .schema(schema_registry.get_spark_schema("users")) \
    .load("path/to/data.csv")

# Get partition columns for writing
partition_by = schema_registry.get_partition_columns("users")
df.write.partitionBy(*partition_by).parquet("output/path")

# Validate schema compatibility
if schema_registry.validate_schema_compatibility("users", df):
    print("DataFrame matches expected schema")
else:
    print("Schema validation failed")
""" 