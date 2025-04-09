"""
MongoDB data source implementation.
"""
from typing import Dict, Any, Optional, List, Union
from pyspark.sql import SparkSession, DataFrame
import logging
import json
from .base_source import BaseDataSource
from config.source_configs.mongodb_config import MongoDBSourceConfig

class MongoDBDataSource(BaseDataSource):
    """Data source for reading from MongoDB."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the MongoDB data source.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for the MongoDB source (optional)
        """
        if config is None:
            config = MongoDBSourceConfig().get('mongodb', {})
        super().__init__(spark, config)
        
    def read(self, database: Optional[str] = None, collection: Optional[str] = None, 
             query: Optional[Dict[str, Any]] = None, projection: Optional[Dict[str, Any]] = None, 
             batch_size: Optional[int] = None, **kwargs) -> DataFrame:
        """
        Read data from a MongoDB collection.
        
        Args:
            database: Database name (overrides config)
            collection: Collection name (overrides config)
            query: Query filter (overrides config)
            projection: Field projection (overrides config)
            batch_size: Number of documents per batch (overrides config)
            **kwargs: Additional read parameters
            
        Returns:
            DataFrame: PySpark DataFrame containing the data
        """
        try:
            # Get parameters from config if not provided
            db_name = database or self.config.get('database', '')
            coll_name = collection or self.config.get('collection', '')
            query_filter = query or self.config.get('query', {})
            field_projection = projection or self.config.get('projection', {})
            docs_per_batch = batch_size or self.config.get('batch_size', 1000)
            
            if not db_name or not coll_name:
                raise ValueError("Both database and collection names must be provided")
                
            # Set up connection URI
            uri = self.config.get('uri')
            if not uri:
                host = self.config.get('host', 'localhost')
                port = self.config.get('port', 27017)
                username = self.config.get('username', '')
                password = self.config.get('password', '')
                auth_source = self.config.get('auth_source', 'admin')
                
                if username and password:
                    uri = f"mongodb://{username}:{password}@{host}:{port}/{db_name}?authSource={auth_source}"
                else:
                    uri = f"mongodb://{host}:{port}/{db_name}"
                    
            # Set up read options
            read_options = {
                'uri': uri,
                'database': db_name,
                'collection': coll_name,
                'batchSize': str(docs_per_batch)
            }
            
            # Add query filter if provided
            if query_filter:
                read_options['pipeline'] = json.dumps([{'$match': query_filter}])
                
            # Add projection if provided
            if field_projection:
                if 'pipeline' in read_options:
                    pipeline = json.loads(read_options['pipeline'])
                    pipeline.append({'$project': field_projection})
                    read_options['pipeline'] = json.dumps(pipeline)
                else:
                    read_options['pipeline'] = json.dumps([{'$project': field_projection}])
                    
            # Add any additional options from kwargs
            for key, value in kwargs.items():
                if key not in ['database', 'collection', 'query', 'projection', 'batch_size']:
                    read_options[key] = value
                    
            # Read the data
            self.logger.info(f"Reading from MongoDB collection: {db_name}.{coll_name}")
            df = self.spark.read.format('mongo').options(**read_options).load()
            
            # Log the operation
            row_count = df.count()
            self._log_read_operation(f"MongoDB collection {db_name}.{coll_name}", row_count)
            
            return df
            
        except Exception as e:
            db_name = database or self.config.get('database', '')
            coll_name = collection or self.config.get('collection', '')
            self.logger.error(f"Error reading from MongoDB collection {db_name}.{coll_name}: {str(e)}")
            raise
            
    def validate(self) -> bool:
        """
        Validate the MongoDB connection.
        
        Returns:
            bool: True if validation is successful, False otherwise
        """
        try:
            # Get connection parameters
            db_name = self.config.get('database', '')
            coll_name = self.config.get('collection', '')
            
            if not db_name:
                self.logger.warning("Database name not specified")
                return False
                
            # Set up connection URI
            uri = self.config.get('uri')
            if not uri:
                host = self.config.get('host', 'localhost')
                port = self.config.get('port', 27017)
                username = self.config.get('username', '')
                password = self.config.get('password', '')
                auth_source = self.config.get('auth_source', 'admin')
                
                if username and password:
                    uri = f"mongodb://{username}:{password}@{host}:{port}/{db_name}?authSource={auth_source}"
                else:
                    uri = f"mongodb://{host}:{port}/{db_name}"
                    
            # Try to execute a simple query to validate the connection
            read_options = {
                'uri': uri,
                'database': db_name,
                'collection': coll_name if coll_name else 'system.namespaces',
                'pipeline': '[{"$limit": 1}]'
            }
            
            test_df = self.spark.read.format('mongo').options(**read_options).load()
            test_df.collect()
            
            self.logger.info("MongoDB connection validated successfully")
            return True
        except Exception as e:
            self.logger.error(f"MongoDB connection validation failed: {str(e)}")
            return False