"""
Hugging Face embedding implementation.
"""
from typing import List, Dict, Any, Optional, Union
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, FloatType
import logging
import numpy as np
from sentence_transformers import SentenceTransformer
from .base_embedder import BaseEmbedder

class HuggingFaceEmbedder(BaseEmbedder):
    """Embedder that uses Hugging Face models to generate embeddings."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the Hugging Face embedder.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for embedding generation
        """
        super().__init__(spark, config)
        
        # Load the model
        model_name = self.config.get('model_name', 'all-MiniLM-L6-v2')
        try:
            self.model = SentenceTransformer(model_name)
            self.logger.info(f"Loaded Hugging Face model: {model_name}")
        except Exception as e:
            self.logger.error(f"Failed to load Hugging Face model: {str(e)}")
            raise
            
    def generate_embeddings(self, df: DataFrame, text_column: str, output_column: Optional[str] = None) -> DataFrame:
        """
        Generate embeddings for text data using a Hugging Face model.
        
        Args:
            df: Input DataFrame
            text_column: Name of the column containing text data
            output_column: Name of the output column (defaults to embeddings)
            
        Returns:
            DataFrame: DataFrame with text embeddings
        """
        if output_column is None:
            output_column = "embeddings"
            
        # Create a broadcast variable for the model
        model_broadcast = self.spark.sparkContext.broadcast(self.model)
        
        # Define the embedding UDF
        @udf(ArrayType(FloatType()))
        def embed_text(text):
            if text is None:
                return None
                
            model = model_broadcast.value
            embedding = model.encode(text)
            return embedding.tolist()
            
        # Apply the embedding
        result_df = df.withColumn(output_column, embed_text(col(text_column)))
        
        self.logger.info(f"Generated embeddings for text in column '{text_column}' to '{output_column}'")
        return result_df
        
    def batch_generate_embeddings(self, texts: List[str]) -> List[np.ndarray]:
        """
        Generate embeddings for a batch of texts.
        
        Args:
            texts: List of text strings
            
        Returns:
            List[np.ndarray]: List of embedding vectors
        """
        try:
            embeddings = self.model.encode(texts)
            return embeddings
        except Exception as e:
            self.logger.error(f"Error generating batch embeddings: {str(e)}")
            raise