"""
PostgreSQL data source implementation.
"""
from typing import Dict, Any, Optional, List, Union
from pyspark.sql import SparkSession, DataFrame
import logging
from .base_source import BaseDataSource
from config.source_configs.postgres_config import PostgresSourceConfig

class PostgresDataSource(BaseDataSource):
    """Data source for reading from PostgreSQL with vector support."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the PostgreSQL data source.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for the PostgreSQL source (optional)
        """
        if config is None:
            config = PostgresSourceConfig().get('postgres', {})
        super().__init__(spark, config)
        
        # JDBC connection properties
        self.jdbc_url = f"jdbc:postgresql://{self.config.get('host', 'localhost')}:{self.config.get('port', 5432)}/{self.config.get('database', 'postgres')}"
        self.connection_properties = {
            "user": self.config.get('username', ''),
            "password": self.config.get('password', ''),
            "driver": "org.postgresql.Driver"
        }
        
        # Add SSL properties if needed
        ssl_mode = self.config.get('ssl_mode')
        if ssl_mode and ssl_mode != 'disable':
            self.connection_properties['ssl'] = 'true'
            self.connection_properties['sslmode'] = ssl_mode
            
            ssl_root_cert = self.config.get('ssl_root_cert')
            if ssl_root_cert:
                self.connection_properties['sslrootcert'] = ssl_root_cert
                
            ssl_cert = self.config.get('ssl_cert')
            if ssl_cert:
                self.connection_properties['sslcert'] = ssl_cert
                
            ssl_key = self.config.get('ssl_key')
            if ssl_key:
                self.connection_properties['sslkey'] = ssl_key
        
    def read(self, table: Optional[str] = None, schema: Optional[str] = None,
             query: Optional[str] = None, fetch_size: Optional[int] = None,
             partition_info: Optional[Dict[str, Any]] = None, **kwargs) -> DataFrame:
        """
        Read data from a PostgreSQL table or query.
        
        Args:
            table: Table name (overrides config)
            schema: Schema name (overrides config)
            query: SQL query (overrides config)
            fetch_size: Number of rows to fetch per round trip (overrides config)
            partition_info: Partitioning information (overrides config)
            **kwargs: Additional read parameters
            
        Returns:
            DataFrame: PySpark DataFrame containing the data
        """
        try:
            # Get parameters from config if not provided
            table_name = table or self.config.get('table', '')
            schema_name = schema or self.config.get('schema', 'public')
            sql_query = query or self.config.get('query', '')
            batch_size = fetch_size or self.config.get('fetch_size', 1000)
            
            if not table_name and not sql_query:
                raise ValueError("Either table name or SQL query must be provided")
                
            # Set up read options
            read_options = {
                'fetchsize': str(batch_size)
            }
            
            # Add any additional options from kwargs
            for key, value in kwargs.items():
                if key not in ['table', 'schema', 'query', 'fetch_size', 'partition_info']:
                    read_options[key] = value
                    
            # Create the JDBC reader
            jdbc_reader = self.spark.read.format('jdbc') \
                .option('url', self.jdbc_url) \
                .option('user', self.connection_properties['user']) \
                .option('password', self.connection_properties['password']) \
                .option('driver', self.connection_properties['driver'])
                
            # Add any additional connection properties
            for key, value in self.connection_properties.items():
                if key not in ['user', 'password', 'driver']:
                    jdbc_reader = jdbc_reader.option(key, value)
                    
            # Add read options
            for key, value in read_options.items():
                jdbc_reader = jdbc_reader.option(key, value)
                
            # Set up partitioning if provided
            partition_config = partition_info or self._get_partition_info()
            if partition_config and partition_config['partition_column'] and \
               partition_config['lower_bound'] and partition_config['upper_bound'] and \
               partition_config['num_partitions']:
                jdbc_reader = jdbc_reader \
                    .option('partitionColumn', partition_config['partition_column']) \
                    .option('lowerBound', partition_config['lower_bound']) \
                    .option('upperBound', partition_config['upper_bound']) \
                    .option('numPartitions', partition_config['num_partitions'])
                    
            # Read the data
            if sql_query:
                self.logger.info(f"Executing SQL query: {sql_query}")
                df = jdbc_reader.option('query', sql_query).load()
            else:
                full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
                self.logger.info(f"Reading from table: {full_table_name}")
                df = jdbc_reader.option('dbtable', full_table_name).load()
                
            # Log the operation
            row_count = df.count()
            source_details = f"SQL query: {sql_query}" if sql_query else f"table: {table_name}"
            self._log_read_operation(f"PostgreSQL {source_details}", row_count)
            
            return df
            
        except Exception as e:
            source_details = f"SQL query: {query or self.config.get('query', '')}" if query or self.config.get('query', '') else f"table: {table or self.config.get('table', '')}"
            self.logger.error(f"Error reading from PostgreSQL {source_details}: {str(e)}")
            raise
            
    def _get_partition_info(self) -> Dict[str, Any]:
        """Get partitioning information from config."""
        return {
            'partition_column': self.config.get('partition_column', ''),
            'lower_bound': self.config.get('lower_bound', ''),
            'upper_bound': self.config.get('upper_bound', ''),
            'num_partitions': self.config.get('num_partitions', 10)
        }
            
    def validate(self) -> bool:
        """
        Validate the PostgreSQL connection.
        
        Returns:
            bool: True if validation is successful, False otherwise
        """
        try:
            # Try to execute a simple query to validate the connection
            test_df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("query", "SELECT 1 as test") \
                .option("user", self.connection_properties["user"]) \
                .option("password", self.connection_properties["password"]) \
                .option("driver", self.connection_properties["driver"]) \
                .load()
                
            test_df.collect()
            self.logger.info("PostgreSQL connection validated successfully")
            return True
        except Exception as e:
            self.logger.error(f"PostgreSQL connection validation failed: {str(e)}")
            return False