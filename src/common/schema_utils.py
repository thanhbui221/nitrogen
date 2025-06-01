import json
from typing import Dict, Any, Optional, List
from pathlib import Path
import os
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DateType, DecimalType, BooleanType, ArrayType
)
from pyspark.sql.functions import col, length
from common.utils.logging_utils import get_logger

class SchemaRegistry:
    """Registry for managing and accessing data schemas"""
    
    def __init__(self, schema_dir: str = None):
        self.logger = get_logger(__name__)
        # Default schema directory is 'schemas' in the same directory as this module
        if schema_dir is None:
            current_dir = Path(os.path.dirname(os.path.abspath(__file__)))
            self.schema_dir = current_dir.parent.parent / 'schemas'
        else:
            self.schema_dir = Path(schema_dir)
            
        self._schemas: Dict[str, Dict[str, Any]] = {}
        self._spark_schemas: Dict[str, StructType] = {}
        self._load_schemas()
        self.logger.info(f"Initialized schema registry with directory: {self.schema_dir}")

    def _load_schemas(self) -> None:
        """Load all schema definitions from the schema directory"""
        if not self.schema_dir.exists():
            raise ValueError(f"Schema directory not found: {self.schema_dir}")
            
        for schema_file in self.schema_dir.glob("*.json"):
            with open(schema_file) as f:
                schema = json.load(f)
                self._schemas[schema["name"]] = schema
                self.logger.debug(f"Loaded schema: {schema['name']} from {schema_file}")

    def _convert_field_type(self, field_type: str, element_type: str = None) -> Any:
        """
        Convert JSON schema type to Spark SQL type.
        
        Args:
            field_type: The main field type
            element_type: For array types, the type of elements in the array
            
        Returns:
            Spark SQL type
        """
        type_mapping = {
            "string": StringType(),
            "timestamp": TimestampType(),
            "date": DateType(),
            "boolean": BooleanType()
        }
        
        if field_type == "array" and element_type:
            element_spark_type = self._convert_field_type(element_type)
            return ArrayType(element_spark_type, True)  # Arrays allow null elements by default
        elif field_type in type_mapping:
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
                field_type = self._convert_field_type(
                    field["type"],
                    field.get("elementType")  # Get elementType for arrays
                )
            
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

    def _validate_field_constraints(self, field_def: Dict[str, Any], field_name: str, df) -> List[str]:
        """
        Validate field constraints against DataFrame values.
        
        Args:
            field_def: Field definition from schema
            field_name: Name of the field being validated
            df: DataFrame to validate
            
        Returns:
            List of validation error messages
        """
        errors = []
        constraints = field_def.get("constraints", {})
        
        # Check enum constraints
        if "enum" in constraints:
            valid_values = set(constraints["enum"])
            invalid_values = df.select(field_name).distinct().where(
                ~col(field_name).isin(list(valid_values)) & col(field_name).isNotNull()
            )
            if invalid_values.count() > 0:
                bad_values = [row[0] for row in invalid_values.collect()]
                errors.append(
                    f"Field '{field_name}' contains invalid values {bad_values}. "
                    f"Allowed values are {list(valid_values)}"
                )
        
        # Check length constraints for string fields
        if "length" in constraints and field_def["type"] == "string":
            required_length = constraints["length"]
            invalid_length = df.select(field_name).where(
                (col(field_name).isNotNull()) & 
                (length(col(field_name)) != required_length)
            )
            if invalid_length.count() > 0:
                errors.append(
                    f"Field '{field_name}' contains values with length != {required_length}"
                )
                
        return errors

    def validate_schema_compatibility(self, name: str, df) -> bool:
        """
        Validate if a DataFrame is compatible with a schema.
        Checks both structural compatibility and data constraints.
        
        Args:
            name: Schema name
            df: DataFrame to validate
            
        Returns:
            bool: True if compatible, False otherwise
        """
        schema_def = self.get_schema(name)
        expected_schema = self.get_spark_schema(name)
        if not schema_def or not expected_schema:
            self.logger.error(f"Schema '{name}' not found")
            return False
            
        # Check structural compatibility
        df_fields = {f.name: f for f in df.schema.fields}
        expected_fields = {f.name: f for f in expected_schema.fields}
        
        # Check if all required fields are present with correct types
        for field_name, field in expected_fields.items():
            if field_name not in df_fields:
                self.logger.error(f"Required field '{field_name}' missing from DataFrame")
                return False
            if not field.nullable and df_fields[field_name].nullable:
                self.logger.error(f"Field '{field_name}' must not be nullable")
                return False
            if field.dataType != df_fields[field_name].dataType:
                self.logger.error(
                    f"Field '{field_name}' has incorrect type. "
                    f"Expected {field.dataType}, got {df_fields[field_name].dataType}"
                )
                return False
        
        # Check data constraints
        validation_errors = []
        for field_def in schema_def["fields"]:
            field_name = field_def["name"]
            if "constraints" in field_def:
                errors = self._validate_field_constraints(field_def, field_name, df)
                validation_errors.extend(errors)
        
        if validation_errors:
            self.logger.error("Schema validation failed:")
            for error in validation_errors:
                self.logger.error(f"  - {error}")
            return False
                
        return True

