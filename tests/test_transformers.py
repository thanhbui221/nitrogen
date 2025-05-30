"""
Tests for transformer components.
"""

from transform.column_transformer import (
    ColumnTransformer,
    ColumnRenameTransformer,
    ColumnCastTransformer,
    ColumnMapTransformer
)
from transform.array_field_transformer import ArrayFieldTransformer
from transform.timestamp_transformer import TimestampTransformer
from transform.ca_account_parameter_value_transformer import ParameterValueTransformer
from pyspark.sql import SparkSession
import pytest

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder \
        .appName("transformer_tests") \
        .master("local[*]") \
        .getOrCreate()

def test_column_transformer(spark):
    """Test ColumnTransformer functionality."""
    # Create test data
    data = [("1", "A"), ("2", "B"), ("3", "C")]
    df = spark.createDataFrame(data, ["id", "value"])
    
    # Configure transformer
    config = {
        "columns": {
            "id": [{"type": "cast", "to": "int"}],
            "value": [{"type": "map", "mapping": {"A": "X", "B": "Y"}}]
        }
    }
    
    transformer = ColumnTransformer(config)
    result = transformer.transform(df)
    
    # Verify results
    assert result.schema["id"].dataType.simpleString() == "integer"
    assert result.collect()[0]["value"] == "X"
    
def test_array_field_transformer(spark):
    """Test ArrayFieldTransformer functionality."""
    # Create test data
    data = [("[1,2,3]",), ("[4,5,6]",)]
    df = spark.createDataFrame(data, ["numbers"])
    
    # Configure transformer
    transformer = ArrayFieldTransformer("numbers")
    result = transformer.transform(df)
    
    # Verify results
    assert len(result.collect()) == 6  # Each array element becomes a row
    
def test_timestamp_transformer(spark):
    """Test TimestampTransformer functionality."""
    # Create test data
    data = [("2024-01-01 12:00:00",), ("2024-01-02 13:00:00",)]
    df = spark.createDataFrame(data, ["timestamp"])
    
    # Configure transformer
    config = {
        "columns": ["timestamp"],
        "format": "yyyy-MM-dd HH:mm:ss"
    }
    
    transformer = TimestampTransformer(config)
    result = transformer.transform(df)
    
    # Verify results
    assert result.schema["timestamp"].dataType.simpleString() == "timestamp"
    
def test_parameter_value_transformer(spark):
    """Test ParameterValueTransformer functionality."""
    # Create test data
    data = [
        ("123.45", "DECIMAL"),
        ("true", "BOOLEAN"),
        ("text", "STRING")
    ]
    df = spark.createDataFrame(data, ["value", "type"])
    
    # Configure transformer
    config = {
        "param1": {
            "value_column": "value",
            "type_column": "type",
            "type_transforms": {
                "BOOLEAN": {"cast": "boolean"},
                "STRING": {"map": {"text": "mapped_text"}}
            }
        }
    }
    
    transformer = ParameterValueTransformer(config)
    result = transformer.transform(df)
    
    # Verify results
    rows = result.collect()
    assert float(rows[0]["param1"]) == 123.45
    assert rows[1]["param1"] == True
    assert rows[2]["param1"] == "mapped_text"

def test_column_rename_transformer(spark):
    # Create test data
    data = [("John", 30), ("Jane", 25)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    # Configure and apply transformer
    transformer = ColumnRenameTransformer({
        "mappings": {"name": "full_name"}
    })
    result = transformer.transform(df)
    
    # Verify results
    assert "full_name" in result.columns
    assert "name" not in result.columns

def test_filter_transformer(spark):
    # Create test data
    data = [("John", 30), ("Jane", 25)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    # Configure and apply transformer
    transformer = FilterTransformer({
        "condition": "age > 25"
    })
    result = transformer.transform(df)
    
    # Verify results
    assert result.count() == 1
    assert result.first()["age"] == 30

def test_add_column_transformer(spark):
    # Create test data
    data = [("John", 30), ("Jane", 25)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    # Configure and apply transformer
    transformer = AddColumnTransformer({
        "column_name": "is_adult",
        "expression": "age >= 18"
    })
    result = transformer.transform(df)
    
    # Verify results
    assert "is_adult" in result.columns
    assert result.filter("name = 'John'").first()["is_adult"] == True 