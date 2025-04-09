"""
Text cleaning module for the PySpark NLP pipeline.
"""
from typing import List, Dict, Any, Optional, Union, Callable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import re
import logging
import html

class TextCleaner:
    """Text cleaning class for NLP pipeline."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the text cleaner.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for text cleaning
        """
        self.spark = spark
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def clean(self, df: DataFrame, text_column: str, output_column: Optional[str] = None) -> DataFrame:
        """
        Clean text data in a DataFrame.
        
        Args:
            df: Input DataFrame
            text_column: Name of the column containing text data
            output_column: Name of the output column (defaults to text_column_cleaned)
            
        Returns:
            DataFrame: DataFrame with cleaned text
        """
        if output_column is None:
            output_column = f"{text_column}_cleaned"
            
        # Create a copy of the input DataFrame
        result_df = df
        
        # Apply cleaning steps based on configuration
        if self.config.get('lowercase', True):
            result_df = self._apply_lowercase(result_df, text_column)
            
        if self.config.get('remove_html', True):
            result_df = self._remove_html_tags(result_df, text_column)
            
        if self.config.get('unescape_html', True):
            result_df = self._unescape_html(result_df, text_column)
            
        if self.config.get('remove_urls', True):
            result_df = self._remove_urls(result_df, text_column)
            
        if self.config.get('remove_email', True):
            result_df = self._remove_email(result_df, text_column)
            
        if self.config.get('remove_special_chars', True):
            result_df = self._remove_special_characters(result_df, text_column)
            
        if self.config.get('remove_numbers', False):
            result_df = self._remove_numbers(result_df, text_column)
            
        if self.config.get('remove_extra_whitespace', True):
            result_df = self._remove_extra_whitespace(result_df, text_column)
            
        # Rename the final column
        result_df = result_df.withColumnRenamed(text_column, output_column)
        
        self.logger.info(f"Cleaned text in column '{text_column}' to '{output_column}'")
        return result_df
    
    def _apply_lowercase(self, df: DataFrame, column: str) -> DataFrame:
        """Convert text to lowercase."""
        return df.withColumn(column, col(column).cast(StringType()).lower())
    
    def _remove_html_tags(self, df: DataFrame, column: str) -> DataFrame:
        """Remove HTML tags from text."""
        @udf(StringType())
        def remove_html(text):
            if text is None:
                return None
            clean = re.compile('<.*?>')
            return re.sub(clean, ' ', text)
        
        return df.withColumn(column, remove_html(col(column)))
    
    def _unescape_html(self, df: DataFrame, column: str) -> DataFrame:
        """Unescape HTML entities."""
        @udf(StringType())
        def unescape(text):
            if text is None:
                return None
            return html.unescape(text)
        
        return df.withColumn(column, unescape(col(column)))
    
    def _remove_urls(self, df: DataFrame, column: str) -> DataFrame:
        """Remove URLs from text."""
        @udf(StringType())
        def remove_url(text):
            if text is None:
                return None
            url_pattern = re.compile(r'https?://\S+|www\.\S+')
            return re.sub(url_pattern, ' ', text)
        
        return df.withColumn(column, remove_url(col(column)))
    
    def _remove_email(self, df: DataFrame, column: str) -> DataFrame:
        """Remove email addresses from text."""
        @udf(StringType())
        def remove_email(text):
            if text is None:
                return None
            email_pattern = re.compile(r'\S+@\S+')
            return re.sub(email_pattern, ' ', text)
        
        return df.withColumn(column, remove_email(col(column)))
    
    def _remove_special_characters(self, df: DataFrame, column: str) -> DataFrame:
        """Remove special characters from text."""
        @udf(StringType())
        def remove_special(text):
            if text is None:
                return None
            return re.sub(r'[^\w\s]', ' ', text)
        
        return df.withColumn(column, remove_special(col(column)))
    
    def _remove_numbers(self, df: DataFrame, column: str) -> DataFrame:
        """Remove numbers from text."""
        @udf(StringType())
        def remove_nums(text):
            if text is None:
                return None
            return re.sub(r'\d+', ' ', text)
        
        return df.withColumn(column, remove_nums(col(column)))
    
    def _remove_extra_whitespace(self, df: DataFrame, column: str) -> DataFrame:
        """Remove extra whitespace from text."""
        @udf(StringType())
        def remove_whitespace(text):
            if text is None:
                return None
            return re.sub(r'\s+', ' ', text).strip()
        
        return df.withColumn(column, remove_whitespace(col(column)))