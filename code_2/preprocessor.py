"""
Text preprocessing module for the PySpark NLP pipeline.
"""
from typing import List, Dict, Any, Optional, Union, Callable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, ArrayType
import re
import logging
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import spacy

# Download required NLTK resources
try:
    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)
except Exception as e:
    logging.warning(f"Failed to download NLTK resources: {str(e)}")

class TextPreprocessor:
    """Text preprocessing class for NLP pipeline."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the text preprocessor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for preprocessing
        """
        self.spark = spark
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize spaCy if needed
        self.nlp = None
        if self.config.get('use_spacy', False):
            try:
                spacy_model = self.config.get('spacy_model', 'en_core_web_sm')
                self.nlp = spacy.load(spacy_model)
                self.logger.info(f"Loaded spaCy model: {spacy_model}")
            except Exception as e:
                self.logger.error(f"Failed to load spaCy model: {str(e)}")
                
        # Get stopwords
        self.stop_words = set()
        if self.config.get('remove_stopwords', True):
            try:
                language = self.config.get('language', 'english')
                self.stop_words = set(stopwords.words(language))
                self.logger.info(f"Loaded stopwords for language: {language}")
            except Exception as e:
                self.logger.warning(f"Failed to load stopwords: {str(e)}")
    
    def preprocess(self, df: DataFrame, text_column: str, output_column: Optional[str] = None) -> DataFrame:
        """
        Preprocess text data in a DataFrame.
        
        Args:
            df: Input DataFrame
            text_column: Name of the column containing text data
            output_column: Name of the output column (defaults to text_column_processed)
            
        Returns:
            DataFrame: DataFrame with preprocessed text
        """
        if output_column is None:
            output_column = f"{text_column}_processed"
            
        # Create a copy of the input DataFrame
        result_df = df
        
        # Apply preprocessing steps based on configuration
        if self.config.get('lowercase', True):
            result_df = self._apply_lowercase(result_df, text_column)
            
        if self.config.get('remove_html', True):
            result_df = self._remove_html_tags(result_df, text_column)
            
        if self.config.get('remove_special_chars', True):
            result_df = self._remove_special_characters(result_df, text_column)
            
        if self.config.get('remove_numbers', False):
            result_df = self._remove_numbers(result_df, text_column)
            
        if self.config.get('remove_stopwords', True):
            result_df = self._remove_stopwords(result_df, text_column)
            
        if self.config.get('lemmatize', False) and self.nlp is not None:
            result_df = self._lemmatize(result_df, text_column)
            
        # Rename the final column
        result_df = result_df.withColumnRenamed(text_column, output_column)
        
        self.logger.info(f"Preprocessed text in column '{text_column}' to '{output_column}'")
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
            return re.sub(clean, '', text)
        
        return df.withColumn(column, remove_html(col(column)))
    
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
            return re.sub(r'\d+', '', text)
        
        return df.withColumn(column, remove_nums(col(column)))
    
    def _remove_stopwords(self, df: DataFrame, column: str) -> DataFrame:
        """Remove stopwords from text."""
        stop_words = self.stop_words
        
        @udf(StringType())
        def remove_stops(text):
            if text is None:
                return None
            word_tokens = word_tokenize(text)
            filtered_text = [word for word in word_tokens if word.lower() not in stop_words]
            return ' '.join(filtered_text)
        
        return df.withColumn(column, remove_stops(col(column)))
    
    def _lemmatize(self, df: DataFrame, column: str) -> DataFrame:
        """Lemmatize text using spaCy."""
        nlp = self.nlp
        
        @udf(StringType())
        def lemmatize_text(text):
            if text is None:
                return None
            doc = nlp(text)
            return ' '.join([token.lemma_ for token in doc])
        
        return df.withColumn(column, lemmatize_text(col(column)))