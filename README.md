# PySpark ETL Framework

A modular and extensible ETL framework built with PySpark, using the Factory pattern for dynamic job handling and configuration-as-code approach.

[![Python Version](https://img.shields.io/badge/python-3.x-blue.svg)](https://www.python.org/downloads/)
[![PySpark Version](https://img.shields.io/badge/pyspark-3.5.0-orange.svg)](https://spark.apache.org/docs/3.5.0/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Version History

See [CHANGELOG.md](CHANGELOG.md) for a list of changes and versions.

## Project Structure

```
├── config/
│   └── jobs/              # ETL job configurations
├── schemas/              # Data schema definitions
│   ├── users.json       # User data schema
│   └── orders.json      # Order data schema
├── src/
│   ├── extract/          # Source extractors
│   ├── transform/        # Transformation logic
│   ├── load/            # Data loaders
│   ├── common/          # Shared utilities
│   │   └── schema_utils.py  # Schema management utilities
│   ├── factory/         # Factory pattern implementations
│   └── main.py          # Main Spark application entry point
├── tests/               # Unit tests
├── create_deps_zip.py    # Script to create dependencies package
├── Dockerfile          # Docker image definition
├── docker-entrypoint.sh # Docker entry point script
└── requirements.txt     # Python dependencies
```

## Environment Setup

### 1. Configure Environment Variables

First, copy the environment template:

```bash
# On Windows
copy .env.template .env

# On Linux/MacOS
cp .env.template .env
```

Then edit the `.env` file with your specific paths:

```bash
# Java Configuration
JAVA_HOME=C:\Program Files\Java\jdk-11  # Windows example
# JAVA_HOME=/usr/lib/jvm/java-11        # Linux example

# Spark Configuration
SPARK_HOME=D:\spark-3.5.0-bin-hadoop3   # Windows example
# SPARK_HOME=/opt/spark                  # Linux example

# Hadoop Configuration (for Windows)
HADOOP_HOME=D:\hadoop-3.3.6

# Path Updates
# Windows: Will be handled by setup_env.ps1
# Linux: Add to PATH in .env
PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH

# Python Configuration
PYTHONPATH=$PYTHONPATH:${PWD}/src
PYSPARK_PYTHON=python
PYSPARK_DRIVER_PYTHON=python

# Optional: Spark Configurations
SPARK_CONF_DIR=${PWD}/conf
SPARK_LOCAL_IP=localhost
SPARK_LOCAL_DIRS=/tmp
```

### 2. Apply Environment Configuration

#### On Windows:
Run the PowerShell setup script:
```powershell
.\setup_env.ps1
```

#### On Linux/MacOS:
Source the environment file:
```bash
source .env
```

### 3. Verify Setup
```bash
# Verify Java
java -version

# Verify Spark
spark-submit --version

# Verify Python
python -c "import pyspark; print(pyspark.__version__)"
```

## Local Development

### 1. Environment Setup

First, set up your Python virtual environment and install dependencies:

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
.\venv\Scripts\activate
# On Unix/MacOS:
source venv/bin/activate

# Install required packages
pip install -r requirements.txt
```

### 2. Package Dependencies

After installing the required packages, create the dependencies package for Spark:

```bash
python create_deps_zip.py
```

This creates `deps.zip` containing all necessary dependencies for Spark workers.

### 3. Download JDBC Drivers (Optional)

If you need database connectivity, download the required JDBC drivers:

```bash
mkdir -p drivers
# PostgreSQL
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -O drivers/postgresql-42.6.0.jar
# MySQL
wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j-8.0.33.jar -O drivers/mysql-connector-j-8.0.33.jar
```

### 4. Running Jobs Locally

Now you can run ETL jobs using `spark-submit`. Here are some common scenarios:

#### Basic ETL Job
```bash
spark-submit \
    --master local[4] \
    --driver-memory 4g \
    --py-files deps.zip \
    src/main.py config/jobs/your_job.yaml
```

#### Database ETL Job
```bash
spark-submit \
    --master local[4] \
    --driver-memory 4g \
    --jars drivers/postgresql-42.6.0.jar \
    --driver-class-path drivers/postgresql-42.6.0.jar \
    --py-files deps.zip \
    src/main.py config/jobs/db_job.yaml
```

#### Common spark-submit Options

| Option | Description | Example |
|--------|-------------|---------|
| --master | Spark master URL | local[4], spark://host:7077 |
| --driver-memory | Memory for driver process | 4g, 8g |
| --executor-memory | Memory per executor | 2g, 4g |
| --executor-cores | Cores per executor | 2, 4 |
| --py-files | Additional Python files | deps.zip |
| --jars | Additional JAR files | drivers/postgresql-42.6.0.jar |
| --packages | Maven coordinates | org.postgresql:postgresql:42.6.0 |

### Example Use Cases

1. **Process a local CSV file**:
```bash
spark-submit \
    --master local[2] \
    --py-files deps.zip \
    src/main.py config/jobs/process_csv.yaml
```

2. **PostgreSQL to Parquet**:
```bash
spark-submit \
    --master local[4] \
    --driver-memory 4g \
    --jars drivers/postgresql-42.6.0.jar \
    --driver-class-path drivers/postgresql-42.6.0.jar \
    --py-files deps.zip \
    src/main.py config/jobs/postgres_extract.yaml
```

3. **Large Dataset Processing**:
```bash
spark-submit \
    --master local[8] \
    --driver-memory 8g \
    --executor-memory 4g \
    --executor-cores 4 \
    --py-files deps.zip \
    src/main.py config/jobs/large_dataset.yaml
```

4. **Database Migration (PostgreSQL to MySQL)**:
```bash
spark-submit \
    --master local[4] \
    --driver-memory 4g \
    --jars drivers/postgresql-42.6.0.jar,drivers/mysql-connector-j-8.0.33.jar \
    --driver-class-path drivers/postgresql-42.6.0.jar:drivers/mysql-connector-j-8.0.33.jar \
    --py-files deps.zip \
    src/main.py config/jobs/db_migration.yaml
```

### Debugging Tips

1. **Enable Debug Logging**:
```bash
spark-submit \
    --master local[*] \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j-debug.properties" \
    --py-files deps.zip \
    src/main.py config/jobs/your_job.yaml
```

2. **Interactive Development**:
```bash
# Start PySpark shell with your code
pyspark \
    --driver-memory 4g \
    --py-files deps.zip
```

## Usage

### Preparing for Execution

Before running jobs, especially in distributed mode, you need to package the dependencies:

```bash
# Create the dependencies package
python create_deps_zip.py
```

This creates `deps.zip` containing all the Python dependencies, which Spark will distribute to worker nodes.

### Running ETL Jobs

Use `spark-submit` to run ETL jobs. The `--py-files` option is crucial for distributed execution as it ensures all worker nodes have access to the complete codebase and dependencies:

```bash
spark-submit \
    --master <spark-master-url> \
    --name "ETL Job" \
    --py-files deps.zip \
    src/main.py config/jobs/your_job.yaml
```

The `--py-files` option serves several purposes:
- Distributes the dependencies to all Spark worker nodes
- Maintains the proper Python package structure
- Ensures all required packages are available everywhere

Common execution scenarios:

```bash
# Local mode with 4 cores
spark-submit \
    --master local[4] \
    --py-files deps.zip \
    src/main.py config/jobs/your_job.yaml

# Cluster mode with specific configs
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 2 \
    --num-executors 3 \
    --py-files deps.zip \
    src/main.py config/jobs/your_job.yaml
```

### Database Jobs

For jobs involving database operations, include both the dependencies and JDBC driver:

```bash
# PostgreSQL example
spark-submit \
    --master local[4] \
    --py-files deps.zip \
    --jars ./drivers/postgresql-42.6.0.jar \
    --driver-class-path ./drivers/postgresql-42.6.0.jar \
    src/main.py config/jobs/postgres_job.yaml

# MySQL example
spark-submit \
    --master local[4] \
    --py-files deps.zip \
    --jars ./drivers/mysql-connector-j-8.0.33.jar \
    --driver-class-path ./drivers/mysql-connector-j-8.0.33.jar \
    src/main.py config/jobs/mysql_job.yaml
```

## Adding New Components

### New Extractor
1. Create a new class in `src/extract/`
2. Inherit from `BaseExtractor`
3. Register in the extractor factory

### New Transformer
1. Create a new class in `src/transform/`
2. Inherit from `BaseTransformer`
3. Register in the transformer factory

### New Loader
1. Create a new class in `src/load/`
2. Inherit from `BaseLoader`
3. Register in the loader factory

## Database Support

### Supported Databases
- PostgreSQL
- MySQL
- Microsoft SQL Server
- Oracle

### Configuration Examples

1. Database Extraction:
```yaml
source:
  type: database
  options:
    db_type: postgresql
    host: localhost
    port: 5432
    database: mydb
    table: users
    # Or use custom query
    # query: "SELECT * FROM users WHERE created_at >= current_date - interval '1 day'"
```

2. Database Loading:
```yaml
target:
  type: database
  options:
    db_type: mysql
    host: localhost
    port: 3306
    database: analytics_db
    table: processed_users
    mode: append  # append/overwrite/error/ignore
    jdbc_options:
      batchsize: 1000
      numPartitions: 4
```

### Performance Tuning
For database operations, consider these configuration options:
- `batchsize`: Number of rows per batch for bulk operations
- `numPartitions`: Number of parallel connections
- `fetchsize`: Number of rows to fetch per network round trip
- Database-specific options (e.g., `rewriteBatchedStatements` for MySQL)

## Testing

### Setting Up Test Environment

1. Make sure you're in your virtual environment:
```bash
# On Windows:
.\venv\Scripts\activate
# On Unix/MacOS:
source venv/bin/activate
```

2. Install test dependencies:
```bash
pip install pytest pytest-cov
```

3. Run tests:
```bash
# Run all tests
pytest tests/

# Run with coverage report
pytest --cov=src tests/

# Run specific test file
pytest tests/test_transformers.py
```

## Schema Management

The framework includes a schema registry for managing data schemas across your ETL jobs.

### Schema Definition

Schemas are defined in JSON format in the `schemas/` directory. Example:

```json
{
  "name": "users",
  "description": "User profile data schema",
  "version": "1.0",
  "fields": [
    {
      "name": "user_id",
      "type": "string",
      "nullable": false,
      "description": "Unique identifier for the user"
    }
  ],
  "partitionColumns": ["created_at"]
}
```

### Using Schemas in Jobs

```python
from src.common.schema_utils import SchemaRegistry

# Initialize schema registry
schema_registry = SchemaRegistry()

# Reading with schema enforcement
df = spark.read.format("csv") \
    .schema(schema_registry.get_spark_schema("users")) \
    .load("path/to/data.csv")

# Get partition columns for writing
partition_by = schema_registry.get_partition_columns("users")
df.write.partitionBy(*partition_by).parquet("output/path")

# Validate schema compatibility
if not schema_registry.validate_schema_compatibility("users", df):
    raise ValueError("DataFrame does not match expected schema")
```

### Schema Features

- **Type Safety**: Enforce data types and nullability
- **Documentation**: Field descriptions and metadata
- **Partition Management**: Define partition columns
- **Validation**: Schema compatibility checking
- **Evolution**: Version tracking for schema changes
- **Reusability**: Share schemas across jobs 

## Configuration Examples

### Join Transformer Configuration

The join transformer supports multiple sequential joins in a single transformation step. Here's an example configuration:

```yaml
transform:
  - type: join
    options:
      joins:
        # First join
        - right_df: customers
          join_type: left
          join_conditions:
            - left: customer_id
              right: id
          select:  # Optional: select specific columns after join
            - order_id
            - customer_name
            - email

        # Second join
        - right_df: products
          join_type: inner
          join_conditions:
            - left: product_id
              right: id
          select:
            - order_id
            - customer_name
            - product_name
            - price
```