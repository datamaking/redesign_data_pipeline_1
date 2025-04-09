"""
Base chunking module for the PySpark NLP pipeline.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame, SparkSession
import logging

class BaseChunker(ABC):
    """Abstract base class for text chunking strategies."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the chunker.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for chunking
        """
        self.spark = spark
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def chunk(self, df: DataFrame, text_column: str, id_column: Optional[str] = None) -> DataFrame:
        """
        Chunk text data in a DataFrame.
        
        Args:
            df: Input DataFrame
            text_column: Name of the column containing text data
            id_column: Name of the column containing unique identifiers (optional)
            
        Returns:
            DataFrame: DataFrame with chunked text
        """
        pass
    
    def smooth_chunks(self, chunks: List[str], overlap: int = 0) -> List[str]:
        """
        Apply smoothing to chunks by adding overlap between consecutive chunks.
        
        Args:
            chunks: List of text chunks
            overlap: Number of words to overlap between chunks
            
        Returns:
            List[str]: List of smoothed chunks
        """
        if overlap <= 0 or len(chunks) <= 1:
            return chunks
            
        smoothed_chunks = []
        for i in range(len(chunks)):
            if i == 0:
                # First chunk remains the same
                smoothed_chunks.append(chunks[i])
            else:
                # Get words from the end of the previous chunk
                prev_chunk_words = chunks[i-1].split()
                overlap_words = prev_chunk_words[-min(overlap, len(prev_chunk_words)):]
                
                # Add overlap words to the beginning of the current chunk
                current_chunk = ' '.join(overlap_words) + ' ' + chunks[i]
                smoothed_chunks.append(current_chunk)
                
        return smoothed_chunks