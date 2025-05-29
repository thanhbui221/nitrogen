import os
from typing import Dict, Any
from src.common.base import BaseLoader
from src.factory.component_factory import loader_factory
from pyspark.sql import DataFrame
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

class DatabaseLoader(BaseLoader):
    # Mapping of database types to their JDBC drivers
    JDBC_DRIVERS = {
        'postgresql': 'org.postgresql.Driver',
        'mysql': 'com.mysql.cj.jdbc.Driver',
        'mssql': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
        'oracle': 'oracle.jdbc.driver.OracleDriver'
    }

    def __init__(self, options: Dict[str, Any]):
        super().__init__(options)
        self.db_type = options.get('db_type', '').lower()
        if self.db_type not in self.JDBC_DRIVERS:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def _get_jdbc_url(self) -> str:
        """Construct JDBC URL based on database type and configuration"""
        host = self.options.get('host', 'localhost')
        port = self.options.get('port')
        database = self.options.get('database')
        
        if not all([host, port, database]):
            raise ValueError("Missing required database connection parameters")

        if self.db_type == 'postgresql':
            return f"jdbc:postgresql://{host}:{port}/{database}"
        elif self.db_type == 'mysql':
            return f"jdbc:mysql://{host}:{port}/{database}"
        elif self.db_type == 'mssql':
            return f"jdbc:sqlserver://{host}:{port};databaseName={database}"
        elif self.db_type == 'oracle':
            return f"jdbc:oracle:thin:@{host}:{port}:{database}"
        
        raise ValueError(f"JDBC URL construction not implemented for {self.db_type}")

    def load(self, df: DataFrame) -> None:
        """Load DataFrame to database table using JDBC"""
        # Get credentials from environment variables or options
        username = self.options.get('username') or os.getenv(f"{self.db_type.upper()}_USER")
        password = self.options.get('password') or os.getenv(f"{self.db_type.upper()}_PASSWORD")
        
        if not all([username, password]):
            raise ValueError("Database credentials not provided")

        # Get target table name
        table = self.options.get('table')
        if not table:
            raise ValueError("Target table name must be provided")

        # Basic JDBC options
        jdbc_options = {
            "driver": self.JDBC_DRIVERS[self.db_type],
            "url": self._get_jdbc_url(),
            "user": username,
            "password": password,
            "dbtable": table
        }

        # Add any additional JDBC options from configuration
        jdbc_options.update(self.options.get('jdbc_options', {}))

        # Configure write mode (append/overwrite)
        mode = self.options.get('mode', 'append')
        if mode not in ['append', 'overwrite', 'error', 'ignore']:
            raise ValueError(f"Invalid write mode: {mode}")

        # Handle schema evolution options
        if self.options.get('merge_schema', False):
            jdbc_options['mergeSchema'] = 'true'

        # Configure batch size for writing
        if 'batchsize' in self.options:
            jdbc_options['batchsize'] = str(self.options['batchsize'])

        # Write DataFrame to database
        df.write \
            .format("jdbc") \
            .mode(mode) \
            .options(**jdbc_options) \
            .save()

# Register the loader
loader_factory.register("database", DatabaseLoader) 