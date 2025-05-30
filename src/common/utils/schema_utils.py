"""
Utilities for schema handling and conversion.
"""

import json
from typing import Dict, Any
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    ArrayType, BooleanType, DecimalType, IntegerType
)

def load_json_schema(schema_name: str) -> Dict[str, Any]:
    """
    Load a JSON schema from file.
    
    Args:
        schema_name: Name of the schema file (without .json extension)
        
    Returns:
        Dictionary containing the schema definition
    """
    with open(f"schemas/{schema_name}.json", 'r') as f:
        return json.load(f)

def get_spark_type(field_type: str, element_type: str = None):
    """
    Convert JSON schema type to Spark type.
    
    Args:
        field_type: Type from JSON schema
        element_type: Optional element type for arrays
        
    Returns:
        Spark SQL type
    """
    type_map = {
        "string": StringType(),
        "timestamp": TimestampType(),
        "boolean": BooleanType(),
        "integer": IntegerType(),
        "decimal": DecimalType(38, 18)
    }
    
    if field_type == "array" and element_type:
        return ArrayType(get_spark_type(element_type))
    
    return type_map.get(field_type, StringType())

def json_schema_to_spark(schema_def: Dict[str, Any]) -> StructType:
    """
    Convert JSON schema definition to Spark schema.
    
    Args:
        schema_def: Dictionary containing JSON schema definition
        
    Returns:
        Spark StructType schema
    """
    fields = []
    
    for field in schema_def["fields"]:
        spark_type = get_spark_type(
            field["type"],
            field.get("elementType")
        )
        
        fields.append(StructField(
            name=field["name"],
            dataType=spark_type,
            nullable=field.get("nullable", True)
        ))
    
    return StructType(fields) 