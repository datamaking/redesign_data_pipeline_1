"""
Text tokenization module for the PySpark NLP pipeline.
"""
from typing import List, Dict, Any, Optional, Union, Callable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, StringType
import logging
import nltk
from nltk.tokenize import word_tokenize, sent_tokenize
import re

# Download required NLTK resources
try:
    nltk.download('punkt', quiet=True)
except Exception as e:
    logging.warning(f"Failed to download NLTK resources: {str(e)}")

class TextTokenizer:
    """Text tokenization class for NLP pipeline."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the text tokenizer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for tokenization
        """
        self.spark = spark
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def tokenize(self, df: DataFrame, text_column: str, output_column: Optional[str] = None,
                tokenize_type: str = 'word') -> DataFrame:
        """
        Tokenize text data in a DataFrame.
        
        Args:
            df: Input DataFrame
            text_column: Name of the column containing text data
            output_column: Name of the output column (defaults to text_column_tokens)
            tokenize_type: Type of tokenization ('word' or 'sentence')
            
        Returns:
            DataFrame: DataFrame with tokenized text
        """
        if output_column is None:
            output_column = f"{text_column}_tokens"
            
        # Select tokenization function based on type
        if tokenize_type.lower() == 'sentence':
            result_df = self._tokenize_sentences(df, text_column, output_column)
        else:  # default to word tokenization
            result_df = self._tokenize_words(df, text_column, output_column)
            
        self.logger.info(f"Tokenized text in column '{text_column}' to '{output_column}' using {tokenize_type} tokenization")
        return result_df
    
    def _tokenize_words(self, df: DataFrame, text_column: str, output_column: str) -> DataFrame:
        """Tokenize text into words."""
        # Define tokenization UDF
        @udf(ArrayType(StringType()))
        def tokenize_to_words(text):
            if text is None:
                return None
            return word_tokenize(text)
        
        # Apply tokenization
        return df.withColumn(output_column, tokenize_to_words(col(text_column)))
    
    def _tokenize_sentences(self, df: DataFrame, text_column: str, output_column: str) -> DataFrame:
        """Tokenize text into sentences."""
        # Define tokenization UDF
        @udf(ArrayType(StringType()))
        def tokenize_to_sentences(text):
            if text is None:
                return None
            return sent_tokenize(text)
        
        # Apply tokenization
        return df.withColumn(output_column, tokenize_to_sentences(col(text_column)))
    
    def tokenize_with_custom_regex(self, df: DataFrame, text_column: str, output_column: Optional[str] = None,
                                  pattern: str = r'\w+') -> DataFrame:
        """
        Tokenize text using a custom regex pattern.
        
        Args:
            df: Input DataFrame
            text_column: Name of the column containing text data
            output_column: Name of the output column (defaults to text_column_tokens)
            pattern: Regex pattern for tokenization
            
        Returns:
            DataFrame: DataFrame with tokenized text
        """
        if output_column is None:
            output_column = f"{text_column}_tokens"
            
        # Define custom tokenization UDF
        @udf(ArrayType(StringType()))
        def tokenize_with_regex(text):
            if text is None:
                return None
            return re.findall(pattern, text)
        
        # Apply tokenization
        result_df = df.withColumn(output_column, tokenize_with_regex(col(text_column)))
        
        self.logger.info(f"Tokenized text in column '{text_column}' to '{output_column}' using custom regex pattern")
        return result_df