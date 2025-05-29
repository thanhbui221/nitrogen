#!/bin/bash
set -e

# Default values
SPARK_MASTER=${SPARK_MASTER:-"local[*]"}
SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-"1g"}
SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-"1g"}
SPARK_EXECUTOR_CORES=${SPARK_EXECUTOR_CORES:-"1"}

# Function to add JDBC driver if needed
add_jdbc_options() {
    local job_config=$1
    local jdbc_options=""
    
    # Check if job uses database connections
    if grep -q "db_type.*postgresql" "$job_config"; then
        jdbc_options="--jars /opt/jdbc-drivers/postgresql-42.6.0.jar --driver-class-path /opt/jdbc-drivers/postgresql-42.6.0.jar"
    elif grep -q "db_type.*mysql" "$job_config"; then
        jdbc_options="--jars /opt/jdbc-drivers/mysql-connector-j-8.0.33.jar --driver-class-path /opt/jdbc-drivers/mysql-connector-j-8.0.33.jar"
    fi
    
    echo "$jdbc_options"
}

# Check if a job configuration file is provided
if [ "$#" -eq 0 ]; then
    echo "Error: Job configuration file not provided"
    echo "Usage: docker run [docker-options] etl-framework /path/to/job.yaml"
    exit 1
fi

JOB_CONFIG=$1
JDBC_OPTIONS=$(add_jdbc_options "$JOB_CONFIG")

# Execute spark-submit with the provided configuration
exec spark-submit \
    --master "$SPARK_MASTER" \
    --driver-memory "$SPARK_DRIVER_MEMORY" \
    --executor-memory "$SPARK_EXECUTOR_MEMORY" \
    --executor-cores "$SPARK_EXECUTOR_CORES" \
    --py-files /app/src.zip \
    $JDBC_OPTIONS \
    /app/src/main.py "$JOB_CONFIG" 