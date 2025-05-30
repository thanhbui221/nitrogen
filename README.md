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
├── create_package.py    # Script to create source distribution
├── Dockerfile          # Docker image definition
├── docker-entrypoint.sh # Docker entry point script
└── requirements.txt     # Python dependencies
```

## Local Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Docker Deployment

### Building the Image

Build the Docker image with all dependencies included:

```bash
docker build -t etl-framework .
```

### Running ETL Jobs

1. **Standard ETL Job**:
```bash
docker run --rm \
    -v /path/to/your/jobs:/app/jobs \
    -v /path/to/your/data:/app/data \
    -e SPARK_MASTER=local[4] \
    -e SPARK_DRIVER_MEMORY=4g \
    -e SPARK_EXECUTOR_MEMORY=2g \
    -e SPARK_EXECUTOR_CORES=2 \
    etl-framework /app/jobs/your_job.yaml
```

2. **Database ETL Job**:
```bash
docker run --rm \
    -v /path/to/your/jobs:/app/jobs \
    -v /path/to/your/data:/app/data \
    -v /path/to/your/.env:/app/.env \
    --network host \
    -e SPARK_MASTER=local[4] \
    -e SPARK_DRIVER_MEMORY=4g \
    etl-framework /app/jobs/db_job.yaml
```

All Spark configuration is done through environment variables, with sensible defaults if not specified:

| Variable | Default | Description |
|----------|---------|-------------|
| SPARK_MASTER | local[*] | Spark master URL |
| SPARK_DRIVER_MEMORY | 1g | Driver process memory |
| SPARK_EXECUTOR_MEMORY | 1g | Executor process memory |
| SPARK_EXECUTOR_CORES | 1 | Number of cores per executor |

### Volume Mounts

The container expects these volume mounts:
- `-v /path/to/your/jobs:/app/jobs` : Job configuration files
- `-v /path/to/your/data:/app/data` : Data files for processing
- `-v /path/to/your/.env:/app/.env` : Environment file (for database credentials)

### Network Configuration

For database connections:
- Use `--network host` to connect to databases on the host machine
- Use Docker networks (e.g., `--network my-network`) to connect to databases in other containers

### Included Components

The Docker image includes:
- PySpark 3.5.0
- Python 3
- Common JDBC drivers:
  - PostgreSQL (postgresql-42.6.0.jar)
  - MySQL (mysql-connector-j-8.0.33.jar)
- All required Python dependencies

### Examples

**Processing local CSV files**:
```bash
docker run --rm \
    -v ./jobs:/app/jobs \
    -v ./data:/app/data \
    etl-framework /app/jobs/process_csv.yaml
```

**Loading data from PostgreSQL to MySQL**:
```bash
docker run --rm \
    -v ./jobs:/app/jobs \
    -v ./.env:/app/.env \
    --network host \
    etl-framework /app/jobs/db_migration.yaml
```

**Processing large datasets**:
```bash
docker run --rm \
    -v ./jobs:/app/jobs \
    -v ./data:/app/data \
    -e SPARK_DRIVER_MEMORY=8g \
    -e SPARK_EXECUTOR_MEMORY=4g \
    -e SPARK_EXECUTOR_CORES=4 \
    etl-framework /app/jobs/large_dataset.yaml
```

## Local Development

### 1. Environment Setup

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Package the source code
python create_package.py
```

### 2. Download JDBC Drivers (if needed)

Create a `drivers` directory and download the required JDBC drivers:

```bash
mkdir -p drivers
# PostgreSQL
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -O drivers/postgresql-42.6.0.jar
# MySQL
wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j-8.0.33.jar -O drivers/mysql-connector-j-8.0.33.jar
```

### 3. Running Jobs Locally

#### Basic ETL Job
```bash
spark-submit \
    --master local[4] \
    --driver-memory 4g \
    --py-files src.zip \
    src/main.py config/jobs/your_job.yaml
```

#### Database ETL Job
```bash
spark-submit \
    --master local[4] \
    --driver-memory 4g \
    --jars drivers/postgresql-42.6.0.jar \
    --driver-class-path drivers/postgresql-42.6.0.jar \
    --py-files src.zip \
    src/main.py config/jobs/db_job.yaml
```

#### Common spark-submit Options

| Option | Description | Example |
|--------|-------------|---------|
| --master | Spark master URL | local[4], spark://host:7077 |
| --driver-memory | Memory for driver process | 4g, 8g |
| --executor-memory | Memory per executor | 2g, 4g |
| --executor-cores | Cores per executor | 2, 4 |
| --py-files | Additional Python files | src.zip |
| --jars | Additional JAR files | drivers/postgresql-42.6.0.jar |
| --packages | Maven coordinates | org.postgresql:postgresql:42.6.0 |

### Example Use Cases

1. **Process a local CSV file**:
```bash
spark-submit \
    --master local[2] \
    --py-files src.zip \
    src/main.py config/jobs/process_csv.yaml
```

2. **PostgreSQL to Parquet**:
```bash
spark-submit \
    --master local[4] \
    --driver-memory 4g \
    --jars drivers/postgresql-42.6.0.jar \
    --driver-class-path drivers/postgresql-42.6.0.jar \
    --py-files src.zip \
    src/main.py config/jobs/postgres_extract.yaml
```

3. **Large Dataset Processing**:
```bash
spark-submit \
    --master local[8] \
    --driver-memory 8g \
    --executor-memory 4g \
    --executor-cores 4 \
    --py-files src.zip \
    src/main.py config/jobs/large_dataset.yaml
```

4. **Database Migration (PostgreSQL to MySQL)**:
```bash
spark-submit \
    --master local[4] \
    --driver-memory 4g \
    --jars drivers/postgresql-42.6.0.jar,drivers/mysql-connector-j-8.0.33.jar \
    --driver-class-path drivers/postgresql-42.6.0.jar:drivers/mysql-connector-j-8.0.33.jar \
    --py-files src.zip \
    src/main.py config/jobs/db_migration.yaml
```

### Debugging Tips

1. **Enable Debug Logging**:
```bash
spark-submit \
    --master local[*] \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j-debug.properties" \
    --py-files src.zip \
    src/main.py config/jobs/your_job.yaml
```

2. **Interactive Development**:
```bash
# Start PySpark shell with your code
pyspark \
    --driver-memory 4g \
    --py-files src.zip
```

## Usage

### Preparing for Execution

Before running jobs, especially in distributed mode, you need to package the source code:

```bash
# Create the source distribution package
python create_package.py
```

This creates `src.zip` containing all the Python source code, which Spark will distribute to worker nodes.

### Running ETL Jobs

Use `spark-submit` to run ETL jobs. The `--py-files` option is crucial for distributed execution as it ensures all worker nodes have access to the complete codebase:

```bash
spark-submit \
    --master <spark-master-url> \
    --name "ETL Job" \
    --py-files src.zip \
    src/main.py config/jobs/your_job.yaml
```

The `--py-files` option serves several purposes:
- Distributes the source code to all Spark worker nodes
- Maintains the proper Python package structure
- Ensures all custom classes (extractors, transformers, loaders) are available everywhere

Common execution scenarios:

```bash
# Local mode with 4 cores
spark-submit \
    --master local[4] \
    --py-files src.zip \
    src/main.py config/jobs/your_job.yaml

# Cluster mode with specific configs
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 2 \
    --num-executors 3 \
    --py-files src.zip \
    src/main.py config/jobs/your_job.yaml
```

### Database Jobs

For jobs involving database operations, include both the source distribution and JDBC driver:

```bash
# PostgreSQL example
spark-submit \
    --master local[4] \
    --py-files src.zip \
    --jars ./drivers/postgresql-42.6.0.jar \
    --driver-class-path ./drivers/postgresql-42.6.0.jar \
    src/main.py config/jobs/postgres_job.yaml

# MySQL example
spark-submit \
    --master local[4] \
    --py-files src.zip \
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

Run tests using:
```bash
pytest tests/
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

Each join configuration supports:
- `right_df`: Name of the dependency DataFrame to join with
- `join_type`: Type of join (inner, left, right, full)
- `join_conditions`: List of column pairs to join on
- `select`: Optional list of columns to keep after the join

The joins are executed in sequence, allowing for complex data merging operations in a single transformer. 