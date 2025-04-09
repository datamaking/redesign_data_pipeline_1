"""
Sentence chunking implementation.
"""
from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.functions import col, udf, monotonically_increasing_id, explode
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType
import logging
import nltk
from nltk.tokenize import sent_tokenize
from .base_chunker import BaseChunker

# Download required NLTK resources
try:
    nltk.download('punkt', quiet=True)
except Exception as e:
    logging.warning(f"Failed to download NLTK resources: {str(e)}")

class SentenceChunker(BaseChunker):
    """Chunker that splits text into sentences."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the sentence chunker.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for chunking
        """
        super().__init__(spark, config)
        self.min_sentence_length = self.config.get('min_sentence_length', 10)  # Minimum sentence length in characters
        self.max_sentences_per_chunk = self.config.get('max_sentences_per_chunk', 5)  # Maximum sentences per chunk
        self.chunk_overlap = self.config.get('chunk_overlap', 1)  # Number of sentences to overlap
        
    def chunk(self, df: DataFrame, text_column: str, id_column: Optional[str] = None) -> DataFrame:
        """
        Chunk text data in a DataFrame into sentences.
        
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
        def chunk_into_sentences(text):
            if text is None:
                return []
                
            # Split text into sentences
            sentences = sent_tokenize(text)
            
            # Filter out short sentences if configured
            if self.min_sentence_length > 0:
                sentences = [s for s in sentences if len(s) >= self.min_sentence_length]
                
            # Group sentences into chunks
            chunks = []
            if self.max_sentences_per_chunk > 0:
                for i in range(0, len(sentences), self.max_sentences_per_chunk - self.chunk_overlap):
                    chunk = ' '.join(sentences[i:i + self.max_sentences_per_chunk])
                    chunks.append(chunk)
            else:
                # If max_sentences_per_chunk is 0 or negative, treat each sentence as a chunk
                chunks = sentences
                
            # Apply smoothing if overlap is specified
            if self.chunk_overlap > 0 and len(chunks) > 1:
                chunks = self.smooth_chunks(chunks, self.chunk_overlap)
                
            return chunks
            
        # Apply the chunking
        chunked_df = df.withColumn("chunks", chunk_into_sentences(col(text_column)))
        
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
        
        self.logger.info(f"Chunked text in column '{text_column}' into {result_df.count()} sentence-based chunks")
        return result_df