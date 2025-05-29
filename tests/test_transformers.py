import pytest
from pyspark.sql import SparkSession
from src.transform.column_transformer import (
    ColumnRenameTransformer,
    FilterTransformer,
    AddColumnTransformer
)

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("test") \
        .master("local[1]") \
        .getOrCreate()

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