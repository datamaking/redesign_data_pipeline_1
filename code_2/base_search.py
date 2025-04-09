"""
Base vector search module for the PySpark NLP pipeline.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
from pyspark.sql import SparkSession
import logging
import numpy as np

class BaseVectorSearch(ABC):
    """Abstract base class for vector search implementations."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the vector search.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for vector search
        """
        self.spark = spark
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        
    @abstractmethod
    def search(self, query_vector: Union[List[float], np.ndarray], top_k: int = 10) -> List[Dict[str, Any]]:
        """
        Search for similar vectors.
        
        Args:
            query_vector: Query vector
            top_k: Number of results to return
            
        Returns:
            List[Dict[str, Any]]: List of search results
        """
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        """
        Validate the vector search configuration and connectivity.
        
        Returns:
            bool: True if validation is successful, False otherwise
        """
        pass