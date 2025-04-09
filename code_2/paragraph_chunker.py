"""
Paragraph chunking implementation.
"""
from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.functions import col, udf, monotonically_increasing_id
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType
import logging
import re
from .base_chunker import BaseChunker

class ParagraphChunker(BaseChunker):
    """Chunker that splits text into paragraphs."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the paragraph chunker.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for chunking
        """
        super().__init__(spark, config)
        self.paragraph_separator = self.config.get('paragraph_separator', r'\n\s*\n')  # Default: blank line
        self.min_paragraph_length = self.config.get('min_paragraph_length', 50)  # Minimum paragraph length in characters
        self.max_paragraphs_per_chunk = self.config.get('max_paragraphs_per_chunk', 3)  # Maximum paragraphs per chunk
        self.chunk_overlap = self.config.get('chunk_overlap', 1)  # Number of paragraphs to overlap
        
    def chunk(self, df: DataFrame, text_column: str, id_column: Optional[str] = None) -> DataFrame:
        """
        Chunk text data in a DataFrame into paragraphs.
        
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
        def chunk_into_paragraphs(text):
            if text is None:
                return []
                
            # Split text into paragraphs
            paragraphs = re.split(self.paragraph_separator, text)
            
            # Remove empty paragraphs and strip whitespace
            paragraphs = [p.strip() for p in paragraphs if p.strip()]
            
            # Filter out short paragraphs if configured
            if self.min_paragraph_length > 0:
                paragraphs = [p for p in paragraphs if len(p) >= self.min_paragraph_length]
                
            # Group paragraphs into chunks
            chunks = []
            if self.max_paragraphs_per_chunk > 0:
                for i in range(0, len(paragraphs), self.max_paragraphs_per_chunk - self.chunk_overlap):
                    chunk = '\n\n'.join(paragraphs[i:i + self.max_paragraphs_per_chunk])
                    chunks.append(chunk)
            else:
                # If max_paragraphs_per_chunk is 0 or negative, treat each paragraph as a chunk
                chunks = paragraphs
                
            # Apply smoothing if overlap is specified
            if self.chunk_overlap > 0 and len(chunks) > 1:
                chunks = self.smooth_chunks(chunks, self.chunk_overlap)
                
            return chunks
            
        # Apply the chunking
        chunked_df = df.withColumn("chunks", chunk_into_paragraphs(col(text_column)))
        
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
        
        self.logger.info(f"Chunked text in column '{text_column}' into {result_df.count()} paragraph-based chunks")
        return result_df