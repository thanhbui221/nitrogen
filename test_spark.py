import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
print("Loading environment variables from .env file...")
load_dotenv()

# Print Python environment info
print(f"Python executable: {sys.executable}")
print(f"Python version: {sys.version}")

# Print environment variables for debugging
print("\nEnvironment Variables:")
print(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")
print(f"SPARK_HOME: {os.environ.get('SPARK_HOME')}")
print(f"HADOOP_HOME: {os.environ.get('HADOOP_HOME')}")
print(f"PYTHONPATH: {os.environ.get('PYTHONPATH')}")
print(f"PATH: {os.environ.get('PATH')}")

try:
    import pyspark
    print(f"\nPySpark version: {pyspark.__version__}")
    print(f"PySpark location: {pyspark.__file__}")
    
    # Try to create a SparkSession
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("test") \
        .config("spark.driver.host", "localhost") \
        .getOrCreate()
    print("\nSuccessfully created SparkSession!")
    
except Exception as e:
    print(f"\nError: {str(e)}")
    print(f"Type: {type(e)}") 