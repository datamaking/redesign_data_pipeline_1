"""
Hive data source implementation.
"""
from typing import Dict, Any, Optional, List, Union
from pyspark.sql import SparkSession, DataFrame
import logging
from .base_source import BaseDataSource
from config.source_configs.hive_config import HiveSourceConfig

class HiveDataSource(BaseDataSource):
    """Data source for reading from Hive tables."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the Hive data source.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for the Hive source (optional)
        """
        if config is None:
            config = HiveSourceConfig().get('hive', {})
        super().__init__(spark, config)
        
    def read(self, table_name: str, columns: Optional[List[str]] = None, 
             where_clause: Optional[str] = None, limit: Optional[int] = None) -> DataFrame:
        """
        Read data from a Hive table.
        
        Args:
            table_name: Name of the Hive table
            columns: List of columns to select (None for all columns)
            where_clause: WHERE clause for filtering data
            limit: Maximum number of rows to read
            
        Returns:
            DataFrame: PySpark DataFrame containing the data
        """
        try:
            # Construct the SQL query
            column_str = "*" if columns is None else ", ".join(columns)
            query = f"SELECT {column_str} FROM {table_name}"
            
            if where_clause:
                query += f" WHERE {where_clause}"
                
            if limit:
                query += f" LIMIT {limit}"
                
            # Execute the query
            self.logger.info(f"Executing Hive query: {query}")
            df = self.spark.sql(query)
            
            # Log the operation
            row_count = df.count() if limit is None or limit > 100 else None
            self._log_read_operation(f"Hive table {table_name}", row_count)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading from Hive table {table_name}: {str(e)}")
            raise
            
    def validate(self) -> bool:
        """
        Validate the Hive connection.
        
        Returns:
            bool: True if validation is successful, False otherwise
        """
        try:
            # Try to execute a simple query to validate the connection
            self.spark.sql("SHOW DATABASES").collect()
            self.logger.info("Hive connection validated successfully")
            return True
        except Exception as e:
            self.logger.error(f"Hive connection validation failed: {str(e)}")
            return False
            
    def list_tables(self, database: Optional[str] = None) -> List[str]:
        """
        List tables in a Hive database.
        
        Args:
            database: Name of the database (None for current database)
            
        Returns:
            List[str]: List of table names
        """
        try:
            query = f"SHOW TABLES"
            if database:
                query += f" IN {database}"
                
            tables_df = self.spark.sql(query)
            tables = [row[0] for row in tables_df.collect()]
            
            self.logger.info(f"Listed {len(tables)} tables in database {database or 'current'}")
            return tables
        except Exception as e:
            self.logger.error(f"Error listing tables in database {database or 'current'}: {str(e)}")
            raise