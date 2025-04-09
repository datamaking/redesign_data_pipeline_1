"""
Neo4j data source implementation.
"""
from typing import Dict, Any, Optional, List, Union
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
import logging
import pandas as pd
from neo4j import GraphDatabase
from .base_source import BaseDataSource
from config.source_configs.neo4j_config import Neo4jSourceConfig

class Neo4jDataSource(BaseDataSource):
    """Data source for reading from Neo4j with vector support."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the Neo4j data source.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for the Neo4j source (optional)
        """
        if config is None:
            config = Neo4jSourceConfig().get('neo4j', {})
        super().__init__(spark, config)
        
        # Neo4j connection parameters
        self.connection_params = self._get_connection_params()
        self.driver = None
        
    def _get_connection_params(self) -> Dict[str, Any]:
        """Get Neo4j connection parameters."""
        url = self.config.get('url')
        if not url:
            protocol = self.config.get('protocol', 'bolt')
            host = self.config.get('host', 'localhost')
            port = self.config.get('port', 7687)
            url = f"{protocol}://{host}:{port}"
            
        return {
            'url': url,
            'database': self.config.get('database', 'neo4j'),
            'auth': (
                self.config.get('username', 'neo4j'),
                self.config.get('password', '')
            ),
            'encryption': self.config.get('encryption', False),
            'trust': self.config.get('trust_strategy', 'TRUST_SYSTEM_CA_SIGNED_CERTIFICATES'),
            'trusted_certificates': self.config.get('trusted_certificates', ''),
            'connection_timeout': self.config.get('connection_timeout', 30),
            'max_connection_lifetime': self.config.get('max_connection_lifetime', 3600),
            'max_connection_pool_size': self.config.get('max_connection_pool_size', 100),
            'connection_acquisition_timeout': self.config.get('connection_acquisition_timeout', 60)
        }
        
    def _get_driver(self):
        """Get or create Neo4j driver."""
        if self.driver is None:
            self.driver = GraphDatabase.driver(
                self.connection_params['url'],
                auth=self.connection_params['auth'],
                encrypted=self.connection_params['encryption'],
                trust=self.connection_params['trust'],
                trusted_certificates=self.connection_params['trusted_certificates'],
                connection_timeout=self.connection_params['connection_timeout'],
                max_connection_lifetime=self.connection_params['max_connection_lifetime'],
                max_connection_pool_size=self.connection_params['max_connection_pool_size'],
                connection_acquisition_timeout=self.connection_params['connection_acquisition_timeout']
            )
        return self.driver
        
    def read(self, query: Optional[str] = None, params: Optional[Dict[str, Any]] = None,
             batch_size: Optional[int] = None, **kwargs) -> DataFrame:
        """
        Read data from Neo4j using a Cypher query.
        
        Args:
            query: Cypher query (overrides config)
            params: Query parameters (overrides config)
            batch_size: Number of records per batch (overrides config)
            **kwargs: Additional read parameters
            
        Returns:
            DataFrame: PySpark DataFrame containing the data
        """
        try:
            # Get parameters from config if not provided
            cypher_query = query or self.config.get('query', '')
            query_params = params or self.config.get('params', {})
            records_per_batch = batch_size or self.config.get('batch_size', 1000)
            
            if not cypher_query:
                raise ValueError("Cypher query must be provided")
                
            # Get the driver
            driver = self._get_driver()
            
            # Execute the query
            self.logger.info(f"Executing Cypher query: {cypher_query}")
            with driver.session(database=self.connection_params['database']) as session:
                result = session.run(cypher_query, query_params)
                
                # Convert to pandas DataFrame
                records = []
                keys = result.keys()
                
                # Process in batches
                batch = result.fetch(records_per_batch)
                while batch:
                    for record in batch:
                        record_dict = {}
                        for key in keys:
                            value = record[key]
                            # Handle Neo4j types
                            if hasattr(value, 'items'):  # Node or Relationship
                                for prop_key, prop_value in value.items():
                                    record_dict[f"{key}_{prop_key}"] = prop_value
                            else:
                                record_dict[key] = value
                        records.append(record_dict)
                    
                    batch = result.fetch(records_per_batch)
                
                # Create pandas DataFrame
                pdf = pd.DataFrame(records)
                
                # Convert pandas DataFrame to Spark DataFrame
                if not pdf.empty:
                    schema = self._infer_schema(pdf)
                    df = self.spark.createDataFrame(pdf, schema=schema)
                else:
                    # Create empty DataFrame with schema
                    schema = StructType([StructField(key, StringType(), True) for key in keys])
                    df = self.spark.createDataFrame([], schema=schema)
                
            # Log the operation
            row_count = df.count()
            self._log_read_operation(f"Neo4j query: {cypher_query}", row_count)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error executing Neo4j query: {str(e)}")
            raise
            
    def _infer_schema(self, pdf: pd.DataFrame) -> StructType:
        """Infer Spark schema from pandas DataFrame."""
        schema = StructType()
        
        for column, dtype in pdf.dtypes.items():
            if 'object' in str(dtype):
                # Check if this is a vector column (list of floats)
                if not pdf[column].empty and isinstance(pdf[column].iloc[0], list):
                    # Check if all elements are floats
                    if all(isinstance(x, (int, float)) for x in pdf[column].iloc[0]):
                        schema.add(StructField(column, ArrayType(FloatType()), True))
                    else:
                        schema.add(StructField(column, StringType(), True))
                else:
                    schema.add(StructField(column, StringType(), True))
            elif 'float' in str(dtype):
                schema.add(StructField(column, FloatType(), True))
            else:
                schema.add(StructField(column, StringType(), True))
                
        return schema
            
    def validate(self) -> bool:
        """
        Validate the Neo4j connection.
        
        Returns:
            bool: True if validation is successful, False otherwise
        """
        try:
            # Try to execute a simple query to validate the connection
            driver = self._get_driver()
            with driver.session(database=self.connection_params['database']) as session:
                result = session.run("RETURN 1 as test")
                result.single()
                
            self.logger.info("Neo4j connection validated successfully")
            return True
        except Exception as e:
            self.logger.error(f"Neo4j connection validation failed: {str(e)}")
            return False
            
    def __del__(self):
        """Close the driver when the object is destroyed."""
        if self.driver is not None:
            self.driver.close()
            self.logger.info("Closed Neo4j driver")