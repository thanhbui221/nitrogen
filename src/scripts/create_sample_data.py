"""
Script to create sample parquet data for testing.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    ArrayType
)
import datetime

def create_sample_data():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("create_sample_data") \
        .master("local[*]") \
        .getOrCreate()
        
    # Create schema
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("smart_contract_version_id", StringType(), True),
        StructField("stakeholder_ids", ArrayType(StringType()), True),
        StructField("status", StringType(), False),
        StructField("source_create_timestamp", TimestampType(), False),
        StructField("source_open_timestamp", TimestampType(), True),
        StructField("permitted_denominations", ArrayType(StringType()), True),
        StructField("acct_type", StringType(), False)
    ])
    
    # Create sample data
    data = [
        (
            "ACC001",
            "SC001",
            ["STK001", "STK002"],
            "active",
            datetime.datetime.now(),
            datetime.datetime.now(),
            ["USD", "EUR"],
            "CA"
        ),
        (
            "ACC002",
            "SC002",
            ["STK003"],
            "active",
            datetime.datetime.now(),
            None,
            ["USD"],
            "CA"
        )
    ]
    
    # Create DataFrame
    df = spark.createDataFrame(data, schema)
    
    # Write to parquet
    df.write.mode("overwrite").parquet("data/input/raw/account")
    
    print("Sample data created successfully")
    spark.stop()

if __name__ == "__main__":
    create_sample_data() 