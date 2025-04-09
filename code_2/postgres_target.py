"""
PostgreSQL data target implementation with vector support.
"""
from typing import Dict, Any, Optional, List, Union
from pyspark.sql import SparkSession, DataFrame
import logging
from .base_target import BaseDataTarget
from config.target_configs.postgres_config import PostgresTargetConfig

class PostgresDataTarget(BaseDataTarget):
    """Data target for writing to PostgreSQL with vector support."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the PostgreSQL data target.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for the PostgreSQL target (optional)
        """
        if config is None:
            config = PostgresTargetConfig().get('postgres', {})
        super().__init__(spark, config)
        
        # JDBC connection properties
        self.jdbc_url = f"jdbc:postgresql://{self.config.get('host', 'localhost')}:{self.config.get('port', 5432)}/{self.config.get('database', 'postgres')}"
        self.connection_properties = {
            "user": self.config.get('username', ''),
            "password": self.config.get('password', ''),
            "driver": "org.postgresql.Driver"
        }
        
    def write(self, df: DataFrame, table_name: str, mode: str = "append", 
              batch_size: Optional[int] = None, vector_column: Optional[str] = None) -> None:
        """
        Write data to a PostgreSQL table.
        
        Args:
            df: DataFrame to write
            table_name: Name of the target table
            mode: Write mode (append, overwrite, ignore, error)
            batch_size: Number of rows per batch
            vector_column: Name of the column containing vector embeddings
        """
        try:
            # Handle vector column if specified
            write_df = df
            if vector_column is not None:
                # Convert vector column to PostgreSQL vector format
                # This requires pgvector extension to be installed in the database
                self.logger.info(f"Writing vector data to column '{vector_column}'")
                
                # Ensure the table has the vector column with the right type
                self._ensure_vector_column(table_name, vector_column, df.select(vector_column).first()[0])
            
            # Set batch size if specified
            if batch_size is None:
                batch_size = self.config.get('batch_size', 1000)
                
            # Write the data
            write_df.write \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", self.connection_properties["user"]) \
                .option("password", self.connection_properties["password"]) \
                .option("driver", self.connection_properties["driver"]) \
                .option("batchsize", batch_size) \
                .mode(mode) \
                .save()
                
            # Log the operation
            self._log_write_operation(f"PostgreSQL table {table_name}", write_df.count())
            
        except Exception as e:
            self.logger.error(f"Error writing to PostgreSQL table {table_name}: {str(e)}")
            raise
            
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
            
    def _ensure_vector_column(self, table_name: str, vector_column: str, sample_vector: List[float]) -> None:
        """
        Ensure that the table has a vector column with the right type.
        
        Args:
            table_name: Name of the target table
            vector_column: Name of the vector column
            sample_vector: Sample vector to determine the dimension
        """
        vector_dim = len(sample_vector)
        
        # Create a temporary DataFrame to execute SQL
        sql = f"""
        DO $$
        BEGIN
            -- Create the extension if it doesn't exist
            CREATE EXTENSION IF NOT EXISTS vector;
            
            -- Check if the table exists
            IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}') THEN
                -- Check if the column exists
                IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_name = '{table_name}' AND column_name = '{vector_column}') THEN
                    -- Add the vector column
                    EXECUTE 'ALTER TABLE {table_name} ADD COLUMN {vector_column} vector({vector_dim})';
                END IF;
            ELSE
                -- Create the table with the vector column
                EXECUTE 'CREATE TABLE {table_name} ({vector_column} vector({vector_dim}))';
            END IF;
        END $$;
        """
        
        try:
            # Execute the SQL
            self.spark.sql(sql)
            self.logger.info(f"Ensured vector column '{vector_column}' in table '{table_name}'")
        except Exception as e:
            self.logger.error(f"Error ensuring vector column: {str(e)}")
            raise