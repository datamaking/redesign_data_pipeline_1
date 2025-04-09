"""
RDBMS data source implementation.
"""
from typing import Dict, Any, Optional, List, Union
from pyspark.sql import SparkSession, DataFrame
import logging
from .base_source import BaseDataSource
from config.source_configs.rdbms_config import RdbmsSourceConfig

class RdbmsDataSource(BaseDataSource):
    """Data source for reading from relational databases."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the RDBMS data source.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for the RDBMS source (optional)
        """
        if config is None:
            config = RdbmsSourceConfig().get('rdbms', {})
        super().__init__(spark, config)
        
        # JDBC connection properties
        self.connection_params = self._get_connection_params()
        
    def _get_connection_params(self) -> Dict[str, str]:
        """Get JDBC connection parameters."""
        url = self.config.get('url')
        if not url:
            driver_prefix = self.config.get('driver', 'com.mysql.jdbc.Driver')
            if 'mysql' in driver_prefix.lower():
                jdbc_prefix = 'mysql'
            elif 'postgresql' in driver_prefix.lower():
                jdbc_prefix = 'postgresql'
            elif 'sqlserver' in driver_prefix.lower():
                jdbc_prefix = 'sqlserver'
            elif 'oracle' in driver_prefix.lower():
                jdbc_prefix = 'oracle'
            else:
                jdbc_prefix = 'jdbc'
                
            host = self.config.get('host', 'localhost')
            port = self.config.get('port', 3306)
            database = self.config.get('database', 'database')
            url = f"jdbc:{jdbc_prefix}://{host}:{port}/{database}"
            
        connection_properties = {
            'driver': self.config.get('driver', 'com.mysql.jdbc.Driver'),
            'url': url,
            'user': self.config.get('username', ''),
            'password': self.config.get('password', '')
        }
        
        # Add any additional properties
        for key, value in self.config.get('properties', {}).items():
            connection_properties[key] = value
            
        return connection_properties
        
    def read(self, table: Optional[str] = None, query: Optional[str] = None, 
             fetch_size: Optional[int] = None, partition_info: Optional[Dict[str, Any]] = None, 
             **kwargs) -> DataFrame:
        """
        Read data from a relational database.
        
        Args:
            table: Table name (overrides config)
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
                if key not in ['table', 'query', 'fetch_size', 'partition_info']:
                    read_options[key] = value
                    
            # Create the JDBC reader
            jdbc_reader = self.spark.read.format('jdbc') \
                .option('driver', self.connection_params['driver']) \
                .option('url', self.connection_params['url']) \
                .option('user', self.connection_params['user']) \
                .option('password', self.connection_params['password'])
                
            # Add any additional connection properties
            for key, value in self.connection_params.items():
                if key not in ['driver', 'url', 'user', 'password']:
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
                self.logger.info(f"Reading from table: {table_name}")
                df = jdbc_reader.option('dbtable', table_name).load()
                
            # Log the operation
            row_count = df.count()
            source_details = f"SQL query: {sql_query}" if sql_query else f"table: {table_name}"
            self._log_read_operation(f"RDBMS {source_details}", row_count)
            
            return df
            
        except Exception as e:
            source_details = f"SQL query: {query or self.config.get('query', '')}" if query or self.config.get('query', '') else f"table: {table or self.config.get('table', '')}"
            self.logger.error(f"Error reading from RDBMS {source_details}: {str(e)}")
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
        Validate the RDBMS connection.
        
        Returns:
            bool: True if validation is successful, False otherwise
        """
        try:
            # Try to execute a simple query to validate the connection
            test_df = self.spark.read.format('jdbc') \
                .option('driver', self.connection_params['driver']) \
                .option('url', self.connection_params['url']) \
                .option('user', self.connection_params['user']) \
                .option('password', self.connection_params['password']) \
                .option('query', 'SELECT 1 as test') \
                .load()
                
            test_df.collect()
            self.logger.info("RDBMS connection validated successfully")
            return True
        except Exception as e:
            self.logger.error(f"RDBMS connection validation failed: {str(e)}")
            return False