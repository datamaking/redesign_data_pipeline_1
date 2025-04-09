"""
Base embedding module for the PySpark NLP pipeline.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
from pyspark.sql import DataFrame, SparkSession
import logging
import numpy as np

class BaseEmbedder(ABC):
    """Abstract base class for text embedding generators."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the embedder.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for embedding generation
        """
        self.spark = spark
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        
    @abstractmethod
    def generate_embeddings(self, df: DataFrame, text_column: str, output_column: Optional[str] = None) -> DataFrame:
        """
        Generate embeddings for text data in a DataFrame.
        
        Args:
            df: Input DataFrame
            text_column: Name of the column containing text data
            output_column: Name of the output column (defaults to embeddings)
            
        Returns:
            DataFrame: DataFrame with text embeddings
        """
        pass
    
    def _validate_embeddings(self, embeddings: List[np.ndarray]) -> bool:
        """
        Validate that embeddings have consistent dimensions.
        
        Args:
            embeddings: List of embedding vectors
            
        Returns:
            bool: True if embeddings are valid, False otherwise
        """
        if not embeddings:
            return False
            
        # Check that all embeddings have the same dimension
        dim = embeddings[0].shape[0]
        return all(emb.shape[0] == dim for emb in embeddings)