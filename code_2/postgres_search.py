"""
PostgreSQL vector search implementation.
"""
from typing import List, Dict, Any, Optional, Union
from pyspark.sql import SparkSession
import logging
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from .base_search import BaseVectorSearch

class PostgresVectorSearch(BaseVectorSearch):
    """Vector search implementation for PostgreSQL with pgvector."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the PostgreSQL vector search.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for vector search
        """
        super().__init__(spark, config)
        
        # Connection parameters
        self.host = self.config.get('host', 'localhost')
        self.port = self.config.get('port', 5432)
        self.database = self.config.get('database', 'postgres')
        self.username = self.config.get('username', '')
        self.password = self.config.get('password', '')
        self.table_name = self.config.get('table_name', '')
        self.vector_column = self.config.get('vector_column', 'embedding')
        self.id_column = self.config.get('id_column', 'id')
        self.content_column = self.config.get('content_column', 'content')
        self.metadata_columns = self.config.get('metadata_columns', [])
        
        # Connection
        self.conn = None
        
    def _connect(self) -> None:
        """Establish a connection to PostgreSQL."""
        if self.conn is None or self.conn.closed:
            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.username,
                    password=self.password
                )
                self.logger.info(f"Connected to PostgreSQL database: {self.database}")
            except Exception as e:
                self.logger.error(f"Error connecting to PostgreSQL: {str(e)}")
                raise
                
    def search(self, query_vector: Union[List[float], np.ndarray], top_k: int = 10) -> List[Dict[str, Any]]:
        """
        Search for similar vectors in PostgreSQL.
        
        Args:
            query_vector: Query vector
            top_k: Number of results to return
            
        Returns:
            List[Dict[str, Any]]: List of search results
        """
        self._connect()
        
        try:
            # Convert numpy array to list if needed
            if isinstance(query_vector, np.ndarray):
                query_vector = query_vector.tolist()
                
            # Construct the query
            columns = [self.id_column, self.content_column] + self.metadata_columns
            columns_str = ', '.join(columns)
            
            query = f"""
            SELECT {columns_str}, 
                   1 - ({self.vector_column} <=> %s::vector) as similarity
            FROM {self.table_name}
            ORDER BY {self.vector_column} <=> %s::vector
            LIMIT %s
            """
            
            # Execute the query
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (query_vector, query_vector, top_k))
                results = cursor.fetchall()
                
            self.logger.info(f"Found {len(results)} similar vectors in {self.table_name}")
            return [dict(result) for result in results]
            
        except Exception as e:
            self.logger.error(f"Error searching for similar vectors: {str(e)}")
            raise
            
    def validate(self) -> bool:
        """
        Validate the PostgreSQL vector search configuration.
        
        Returns:
            bool: True if validation is successful, False otherwise
        """
        try:
            self._connect()
            
            # Check if pgvector extension is installed
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT * FROM pg_extension WHERE extname = 'vector'")
                if cursor.fetchone() is None:
                    self.logger.error("pgvector extension is not installed in the database")
                    return False
                    
                # Check if the table exists
                cursor.execute(f"SELECT to_regclass('{self.table_name}')")
                if cursor.fetchone()[0] is None:
                    self.logger.error(f"Table {self.table_name} does not exist")
                    return False
                    
                # Check if the vector column exists
                cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{self.table_name}' AND column_name = '{self.vector_column}'")
                if cursor.fetchone() is None:
                    self.logger.error(f"Vector column {self.vector_column} does not exist in table {self.table_name}")
                    return False
                    
            self.logger.info("PostgreSQL vector search configuration validated successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"PostgreSQL vector search validation failed: {str(e)}")
            return False
            
    def __del__(self):
        """Close the connection when the object is destroyed."""
        if self.conn is not None and not self.conn.closed:
            self.conn.close()
            self.logger.info("Closed PostgreSQL connection")