# Use a base image with Java (required for Spark)
FROM openjdk:11-slim

# Set environment variables
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Install Python and required system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Download and setup Spark
RUN wget -q https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Create directory for JDBC drivers
RUN mkdir -p /opt/jdbc-drivers

# Download common JDBC drivers
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -O /opt/jdbc-drivers/postgresql-42.6.0.jar \
    && wget -q https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j-8.0.33.jar -O /opt/jdbc-drivers/mysql-connector-j-8.0.33.jar

# Set up working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the ETL framework code
COPY src/ src/
COPY config/ config/
COPY create_package.py .

# Create the source distribution package
RUN python3 create_package.py

# Create directory for job configurations
RUN mkdir -p /app/jobs

# Create an entrypoint script
COPY docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"] 