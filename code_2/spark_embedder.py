"""
Spark embedding implementation.
"""
from typing import List, Dict, Any, Optional, Union
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, FloatType
from pyspark.ml.feature import Word2Vec, BertEmbeddings
import logging
import numpy as np
from .base_embedder import BaseEmbedder

class SparkEmbedder(BaseEmbedder):
    """Embedder that uses Spark ML models to generate embeddings."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the Spark embedder.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for embedding generation
        """
        super().__init__(spark, config)
        
        # Get embedding model type
        self.model_type = self.config.get('model_type', 'word2vec')  # Options: word2vec, bert
        self.model = None
        
        # Initialize the model
        if self.model_type == 'word2vec':
            self._init_word2vec()
        elif self.model_type == 'bert':
            self._init_bert()
        else:
            self.logger.error(f"Unsupported model type: {self.model_type}")
            raise ValueError(f"Unsupported model type: {self.model_type}")
            
    def _init_word2vec(self):
        """Initialize Word2Vec model."""
        try:
            # Configure Word2Vec model
            vector_size = self.config.get('vector_size', 100)
            window_size = self.config.get('window_size', 5)
            min_count = self.config.get('min_count', 0)
            max_iter = self.config.get('max_iter', 1)
            
            self.model = Word2Vec(
                vectorSize=vector_size,
                windowSize=window_size,
                minCount=min_count,
                maxIter=max_iter,
                inputCol="tokens",
                outputCol="embeddings"
            )
            
            self.logger.info(f"Initialized Word2Vec model with vector size {vector_size}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Word2Vec model: {str(e)}")
            raise
            
    def _init_bert(self):
        """Initialize BERT embeddings model."""
        try:
            # Configure BERT model
            bert_model_name = self.config.get('bert_model_name', 'bert_base_uncased')
            max_seq_length = self.config.get('max_seq_length', 128)
            case_sensitive = self.config.get('case_sensitive', False)
            
            self.model = BertEmbeddings.pretrained(bert_model_name) \
                .setInputCols(["text"]) \
                .setOutputCol("embeddings") \
                .setCaseSensitive(case_sensitive) \
                .setMaxSentenceLength(max_seq_length)
                
            self.logger.info(f"Initialized BERT embeddings model: {bert_model_name}")
        except Exception as e:
            self.logger.error(f"Failed to initialize BERT embeddings model: {str(e)}")
            raise
            
    def generate_embeddings(self, df: DataFrame, text_column: str, output_column: Optional[str] = None) -> DataFrame:
        """
        Generate embeddings for text data using Spark ML models.
        
        Args:
            df: Input DataFrame
            text_column: Name of the column containing text data
            output_column: Name of the output column (defaults to embeddings)
            
        Returns:
            DataFrame: DataFrame with text embeddings
        """
        if output_column is None:
            output_column = "embeddings"
            
        try:
            # Prepare the data based on model type
            if self.model_type == 'word2vec':
                # Word2Vec requires tokenized input
                from pyspark.ml.feature import Tokenizer
                
                # Check if the input is already tokenized
                if df.schema[text_column].dataType.typeName() == 'array':
                    prepared_df = df.withColumnRenamed(text_column, "tokens")
                else:
                    # Tokenize the text
                    tokenizer = Tokenizer(inputCol=text_column, outputCol="tokens")
                    prepared_df = tokenizer.transform(df)
                
                # Fit the model and transform the data
                word2vec_model = self.model.fit(prepared_df)
                result_df = word2vec_model.transform(prepared_df)
                
                # Rename the output column if needed
                if output_column != "embeddings":
                    result_df = result_df.withColumnRenamed("embeddings", output_column)
                    
            elif self.model_type == 'bert':
                # BERT requires text input
                prepared_df = df.withColumnRenamed(text_column, "text")
                
                # Transform the data
                result_df = self.model.transform(prepared_df)
                
                # Rename the output column if needed
                if output_column != "embeddings":
                    result_df = result_df.withColumnRenamed("embeddings", output_column)
            else:
                raise ValueError(f"Unsupported model type: {self.model_type}")
                
            self.logger.info(f"Generated {self.model_type} embeddings for text in column '{text_column}' to '{output_column}'")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error generating embeddings: {str(e)}")
            raise