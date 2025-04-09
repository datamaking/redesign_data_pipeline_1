"""
Semantic chunking implementation.
"""
from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.functions import col, udf, monotonically_increasing_id
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType
import logging
import nltk
from nltk.tokenize import sent_tokenize
import numpy as np
from .base_chunker import BaseChunker

# Download required NLTK resources
try:
    nltk.download('punkt', quiet=True)
except Exception as e:
    logging.warning(f"Failed to download NLTK resources: {str(e)}")

class SemanticChunker(BaseChunker):
    """Chunker that splits text based on semantic similarity."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the semantic chunker.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for chunking
        """
        super().__init__(spark, config)
        self.chunk_size = self.config.get('chunk_size', 500)  # Target chunk size in characters
        self.chunk_overlap = self.config.get('chunk_overlap', 50)  # Overlap in characters
        self.similarity_threshold = self.config.get('similarity_threshold', 0.7)  # Similarity threshold for merging
        
        # Initialize sentence embedder if available
        self.embedder = None
        if self.config.get('use_embeddings', False):
            try:
                from sentence_transformers import SentenceTransformer
                model_name = self.config.get('embedding_model', 'all-MiniLM-L6-v2')
                self.embedder = SentenceTransformer(model_name)
                self.logger.info(f"Loaded sentence transformer model: {model_name}")
            except Exception as e:
                self.logger.error(f"Failed to load sentence transformer model: {str(e)}")
        
    def chunk(self, df: DataFrame, text_column: str, id_column: Optional[str] = None) -> DataFrame:
        """
        Chunk text data in a DataFrame based on semantic similarity.
        
        Args:
            df: Input DataFrame
            text_column: Name of the column containing text data
            id_column: Name of the column containing unique identifiers (optional)
            
        Returns:
            DataFrame: DataFrame with chunked text
        """
        # If no ID column is provided, create one
        if id_column is None:
            df = df.withColumn("_id", monotonically_increasing_id())
            id_column = "_id"
            
        # Define the chunking UDF
        @udf(ArrayType(StringType()))
        def chunk_semantically(text):
            if text is None:
                return []
                
            # Split text into sentences
            sentences = sent_tokenize(text)
            
            if not sentences:
                return []
                
            # If embedder is available, use semantic chunking
            if self.embedder is not None:
                chunks = self._semantic_chunking(sentences)
            else:
                # Fall back to size-based chunking
                chunks = self._size_based_chunking(sentences)
                
            # Apply smoothing if overlap is specified
            if self.chunk_overlap > 0 and len(chunks) > 1:
                chunks = self.smooth_chunks(chunks, self.chunk_overlap)
                
            return chunks
            
        # Apply the chunking
        chunked_df = df.withColumn("chunks", chunk_semantically(col(text_column)))
        
        # Explode the chunks into separate rows
        schema = StructType([
            StructField(id_column, df.schema[id_column].dataType, True),
            StructField("chunk_id", IntegerType(), False),
            StructField("chunk_text", StringType(), True)
        ])
        
        # Convert to RDD and then back to DataFrame to explode the chunks
        exploded_rdd = chunked_df.rdd.flatMap(
            lambda row: [(row[id_column], i, chunk) for i, chunk in enumerate(row["chunks"])]
        )
        
        result_df = self.spark.createDataFrame(exploded_rdd, schema)
        
        self.logger.info(f"Chunked text in column '{text_column}' into {result_df.count()} semantic chunks")
        return result_df
        
    def _semantic_chunking(self, sentences: List[str]) -> List[str]:
        """
        Chunk sentences based on semantic similarity.
        
        Args:
            sentences: List of sentences
            
        Returns:
            List[str]: List of chunks
        """
        if not sentences:
            return []
            
        # Compute sentence embeddings
        embeddings = self.embedder.encode(sentences)
        
        # Compute similarity matrix
        similarity_matrix = np.inner(embeddings, embeddings)
        
        # Group sentences into chunks
        chunks = []
        current_chunk = []
        current_length = 0
        
        for i, sentence in enumerate(sentences):
            # If this is the first sentence or it's similar to the previous one
            if i == 0 or similarity_matrix[i, i-1] >= self.similarity_threshold:
                current_chunk.append(sentence)
                current_length += len(sentence)
            else:
                # If the chunk is getting too large, start a new one
                if current_length >= self.chunk_size:
                    chunks.append(' '.join(current_chunk))
                    current_chunk = [sentence]
                    current_length = len(sentence)
                else:
                    current_chunk.append(sentence)
                    current_length += len(sentence)
                    
        # Add the last chunk if it's not empty
        if current_chunk:
            chunks.append(' '.join(current_chunk))
            
        return chunks
        
    def _size_based_chunking(self, sentences: List[str]) -> List[str]:
        """
        Chunk sentences based on size.
        
        Args:
            sentences: List of sentences
            
        Returns:
            List[str]: List of chunks
        """
        if not sentences:
            return []
            
        chunks = []
        current_chunk = []
        current_length = 0
        
        for sentence in sentences:
            sentence_length = len(sentence)
            
            # If adding this sentence would exceed the chunk size
            if current_length + sentence_length > self.chunk_size and current_chunk:
                chunks.append(' '.join(current_chunk))
                current_chunk = [sentence]
                current_length = sentence_length
            else:
                current_chunk.append(sentence)
                current_length += sentence_length
                
        # Add the last chunk if it's not empty
        if current_chunk:
            chunks.append(' '.join(current_chunk))
            
        return chunks