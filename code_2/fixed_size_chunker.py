"""
Fixed-size chunking implementation.
"""
from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.functions import col, udf, monotonically_increasing_id
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType
import logging
import uuid
from .base_chunker import BaseChunker

class FixedSizeChunker(BaseChunker):
    """Chunker that splits text into fixed-size chunks."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the fixed-size chunker.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for chunking
        """
        super().__init__(spark, config)
        self.chunk_size = self.config.get('chunk_size', 500)  # Default chunk size (words)
        self.chunk_overlap = self.config.get('chunk_overlap', 50)  # Default overlap (words)
        
    def chunk(self, df: DataFrame, text_column: str, id_column: Optional[str] = None) -> DataFrame:
        """
        Chunk text data in a DataFrame into fixed-size chunks.
        
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
        def chunk_text(text):
            if text is None:
                return []
                
            words = text.split()
            chunks = []
            
            for i in range(0, len(words), self.chunk_size):
                chunk = ' '.join(words[i:i + self.chunk_size])
                chunks.append(chunk)
                
            # Apply smoothing if overlap is specified
            if self.chunk_overlap > 0:
                chunks = self.smooth_chunks(chunks, self.chunk_overlap)
                
            return chunks
            
        # Apply the chunking
        chunked_df = df.withColumn("chunks", chunk_text(col(text_column)))
        
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
        
        self.logger.info(f"Chunked text in column '{text_column}' into {result_df.count()} chunks")
        return result_df