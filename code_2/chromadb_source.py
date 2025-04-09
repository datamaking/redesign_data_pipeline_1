"""
ChromaDB data source implementation.
"""
from typing import Dict, Any, Optional, List, Union
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType, MapType
import logging
import chromadb
from chromadb.config import Settings
from chromadb.utils import embedding_functions
import pandas as pd
from .base_source import BaseDataSource
from config.source_configs.chromadb_config import ChromaDBSourceConfig

class ChromaDBDataSource(BaseDataSource):
    """Data source for reading from ChromaDB."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the ChromaDB data source.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for the ChromaDB source (optional)
        """
        if config is None:
            config = ChromaDBSourceConfig().get('chromadb', {})
        super().__init__(spark, config)
        
        # Initialize ChromaDB client
        self.client = self._init_client()
        
    def _init_client(self) -> chromadb.Client:
        """Initialize ChromaDB client."""
        client_type = self.config.get('client_type', 'http')
        
        if client_type == 'http':
            host = self.config.get('host', 'localhost')
            port = self.config.get('port', 8000)
            return chromadb.HttpClient(host=host, port=port)
        else:  # persistent
            persist_directory = self.config.get('persist_directory', '.chromadb')
            return chromadb.PersistentClient(path=persist_directory)
            
    def _get_embedding_function(self):
        """Get embedding function based on configuration."""
        embedding_func_type = self.config.get('embedding_function', 'default')
        
        if embedding_func_type == 'openai':
            api_key = self.config.get('openai_api_key', '')
            return embedding_functions.OpenAIEmbeddingFunction(
                api_key=api_key,
                model_name="text-embedding-ada-002"
            )
        elif embedding_func_type == 'sentence_transformers':
            model_name = self.config.get('embedding_model', 'all-MiniLM-L6-v2')
            return embedding_functions.SentenceTransformerEmbeddingFunction(
                model_name=model_name
            )
        else:
            return None  # Default embedding function
            
    def read(self, collection_name: Optional[str] = None, where_filter: Optional[Dict[str, Any]] = None, 
             batch_size: Optional[int] = None, include_config: Optional[Dict[str, bool]] = None, 
             **kwargs) -> DataFrame:
        """
        Read data from a ChromaDB collection.
        
        Args:
            collection_name: Collection name (overrides config)
            where_filter: Where filter for querying (overrides config)
            batch_size: Number of items per batch (overrides config)
            include_config: Configuration for what to include (overrides config)
            **kwargs: Additional read parameters
            
        Returns:
            DataFrame: PySpark DataFrame containing the data
        """
        try:
            # Get parameters from config if not provided
            coll_name = collection_name or self.config.get('collection_name', '')
            filter_dict = where_filter or self.config.get('where_filter', {})
            items_per_batch = batch_size or self.config.get('batch_size', 100)
            
            if not coll_name:
                raise ValueError("Collection name must be provided")
                
            # Get include configuration
            if include_config is None:
                include_config = {
                    'include_embeddings': self.config.get('include_embeddings', True),
                    'include_documents': self.config.get('include_documents', True),
                    'include_metadatas': self.config.get('include_metadatas', True)
                }
                
            # Get the collection
            collection = self.client.get_collection(name=coll_name)
            
            # Query the collection
            self.logger.info(f"Reading from ChromaDB collection: {coll_name}")
            result = collection.get(
                where=filter_dict if filter_dict else None,
                limit=items_per_batch,
                include=include_config
            )
            
            # Convert to pandas DataFrame
            pdf = pd.DataFrame({
                'id': result['ids']
            })
            
            if include_config.get('include_documents', True) and 'documents' in result:
                pdf['document'] = result['documents']
                
            if include_config.get('include_metadatas', True) and 'metadatas' in result:
                for i, metadata in enumerate(result['metadatas']):
                    if metadata:
                        for key, value in metadata.items():
                            if key not in pdf.columns:
                                pdf[key] = None
                            pdf.at[i, key] = value
                            
            if include_config.get('include_embeddings', True) and 'embeddings' in result:
                pdf['embedding'] = result['embeddings']
                
            # Convert pandas DataFrame to Spark DataFrame
            schema = self._infer_schema(pdf)
            df = self.spark.createDataFrame(pdf, schema=schema)
            
            # Log the operation
            row_count = df.count()
            self._log_read_operation(f"ChromaDB collection {coll_name}", row_count)
            
            return df
            
        except Exception as e:
            coll_name = collection_name or self.config.get('collection_name', '')
            self.logger.error(f"Error reading from ChromaDB collection {coll_name}: {str(e)}")
            raise
            
    def _infer_schema(self, pdf: pd.DataFrame) -> StructType:
        """Infer Spark schema from pandas DataFrame."""
        schema = StructType()
        
        for column, dtype in pdf.dtypes.items():
            if column == 'id':
                schema.add(StructField(column, StringType(), False))
            elif column == 'document':
                schema.add(StructField(column, StringType(), True))
            elif column == 'embedding':
                schema.add(StructField(column, ArrayType(FloatType()), True))
            elif 'object' in str(dtype):
                # For metadata fields, use string type for simplicity
                schema.add(StructField(column, StringType(), True))
            elif 'float' in str(dtype):
                schema.add(StructField(column, FloatType(), True))
            else:
                schema.add(StructField(column, StringType(), True))
                
        return schema
            
    def validate(self) -> bool:
        """
        Validate the ChromaDB connection.
        
        Returns:
            bool: True if validation is successful, False otherwise
        """
        try:
            # Try to list collections to validate the connection
            self.client.list_collections()
            
            # Check if the specified collection exists
            coll_name = self.config.get('collection_name', '')
            if coll_name:
                collections = self.client.list_collections()
                collection_names = [coll.name for coll in collections]
                
                if coll_name not in collection_names:
                    self.logger.warning(f"Collection {coll_name} does not exist")
                    return False
            
            self.logger.info("ChromaDB connection validated successfully")
            return True
        except Exception as e:
            self.logger.error(f"ChromaDB connection validation failed: {str(e)}")
            return False