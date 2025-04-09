"""
Text normalization module for the PySpark NLP pipeline.
"""
from typing import List, Dict, Any, Optional, Union, Callable, Set
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, ArrayType
import logging
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer, WordNetLemmatizer
import spacy

# Download required NLTK resources
try:
    nltk.download('stopwords', quiet=True)
    nltk.download('wordnet', quiet=True)
except Exception as e:
    logging.warning(f"Failed to download NLTK resources: {str(e)}")

class TextNormalizer:
    """Text normalization class for NLP pipeline."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the text normalizer.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for normalization
        """
        self.spark = spark
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize stopwords
        self.stop_words = set()
        if self.config.get('remove_stopwords', True):
            try:
                language = self.config.get('language', 'english')
                self.stop_words = set(stopwords.words(language))
                self.logger.info(f"Loaded stopwords for language: {language}")
            except Exception as e:
                self.logger.warning(f"Failed to load stopwords: {str(e)}")
                
        # Initialize stemmer
        self.stemmer = None
        if self.config.get('use_stemming', False):
            self.stemmer = PorterStemmer()
            self.logger.info("Initialized Porter stemmer")
            
        # Initialize lemmatizer
        self.lemmatizer = None
        if self.config.get('use_lemmatization', False):
            self.lemmatizer = WordNetLemmatizer()
            self.logger.info("Initialized WordNet lemmatizer")
            
        # Initialize spaCy
        self.nlp = None
        if self.config.get('use_spacy', False):
            try:
                spacy_model = self.config.get('spacy_model', 'en_core_web_sm')
                self.nlp = spacy.load(spacy_model)
                self.logger.info(f"Loaded spaCy model: {spacy_model}")
            except Exception as e:
                self.logger.error(f"Failed to load spaCy model: {str(e)}")
    
    def normalize(self, df: DataFrame, text_column: str, output_column: Optional[str] = None,
                 is_tokenized: bool = False) -> DataFrame:
        """
        Normalize text data in a DataFrame.
        
        Args:
            df: Input DataFrame
            text_column: Name of the column containing text data
            output_column: Name of the output column (defaults to text_column_normalized)
            is_tokenized: Whether the input column contains tokenized text (array of strings)
            
        Returns:
            DataFrame: DataFrame with normalized text
        """
        if output_column is None:
            output_column = f"{text_column}_normalized"
            
        # Create a copy of the input DataFrame
        result_df = df
        
        # Apply normalization steps based on configuration
        if self.config.get('remove_stopwords', True):
            result_df = self._remove_stopwords(result_df, text_column, is_tokenized)
            
        if self.config.get('use_stemming', False) and self.stemmer is not None:
            result_df = self._apply_stemming(result_df, text_column, is_tokenized)
            
        if self.config.get('use_lemmatization', False) and self.lemmatizer is not None:
            result_df = self._apply_lemmatization(result_df, text_column, is_tokenized)
            
        if self.config.get('use_spacy', False) and self.nlp is not None:
            result_df = self._apply_spacy_normalization(result_df, text_column, is_tokenized)
            
        # Rename the final column
        result_df = result_df.withColumnRenamed(text_column, output_column)
        
        self.logger.info(f"Normalized text in column '{text_column}' to '{output_column}'")
        return result_df
    
    def _remove_stopwords(self, df: DataFrame, column: str, is_tokenized: bool) -> DataFrame:
        """Remove stopwords from text."""
        stop_words = self.stop_words
        
        if is_tokenized:
            # Input is already tokenized (array of strings)
            @udf(ArrayType(StringType()))
            def remove_stops_from_tokens(tokens):
                if tokens is None:
                    return None
                return [word for word in tokens if word.lower() not in stop_words]
            
            return df.withColumn(column, remove_stops_from_tokens(col(column)))
        else:
            # Input is plain text
            @udf(StringType())
            def remove_stops_from_text(text):
                if text is None:
                    return None
                words = text.split()
                filtered_words = [word for word in words if word.lower() not in stop_words]
                return ' '.join(filtered_words)
            
            return df.withColumn(column, remove_stops_from_text(col(column)))
    
    def _apply_stemming(self, df: DataFrame, column: str, is_tokenized: bool) -> DataFrame:
        """Apply stemming to text."""
        stemmer = self.stemmer
        
        if is_ 
        """Apply stemming to text."""
        stemmer = self.stemmer
        
        if is_tokenized:
            # Input is already tokenized (array of strings)
            @udf(ArrayType(StringType()))
            def stem_tokens(tokens):
                if tokens is None:
                    return None
                return [stemmer.stem(word) for word in tokens]
            
            return df.withColumn(column, stem_tokens(col(column)))
        else:
            # Input is plain text
            @udf(StringType())
            def stem_text(text):
                if text is None:
                    return None
                words = text.split()
                stemmed_words = [stemmer.stem(word) for word in words]
                return ' '.join(stemmed_words)
            
            return df.withColumn(column, stem_text(col(column)))
    
    def _apply_lemmatization(self, df: DataFrame, column: str, is_tokenized: bool) -> DataFrame:
        """Apply lemmatization to text."""
        lemmatizer = self.lemmatizer
        
        if is_tokenized:
            # Input is already tokenized (array of strings)
            @udf(ArrayType(StringType()))
            def lemmatize_tokens(tokens):
                if tokens is None:
                    return None
                return [lemmatizer.lemmatize(word) for word in tokens]
            
            return df.withColumn(column, lemmatize_tokens(col(column)))
        else:
            # Input is plain text
            @udf(StringType())
            def lemmatize_text(text):
                if text is None:
                    return None
                words = text.split()
                lemmatized_words = [lemmatizer.lemmatize(word) for word in words]
                return ' '.join(lemmatized_words)
            
            return df.withColumn(column, lemmatize_text(col(column)))
    
    def _apply_spacy_normalization(self, df: DataFrame, column: str, is_tokenized: bool) -> DataFrame:
        """Apply spaCy normalization to text."""
        nlp = self.nlp
        
        if is_tokenized:
            # Input is already tokenized (array of strings)
            @udf(ArrayType(StringType()))
            def normalize_tokens_with_spacy(tokens):
                if tokens is None:
                    return None
                # Join tokens to process with spaCy
                text = ' '.join(tokens)
                doc = nlp(text)
                return [token.lemma_ for token in doc]
            
            return df.withColumn(column, normalize_tokens_with_spacy(col(column)))
        else:
            # Input is plain text
            @udf(StringType())
            def normalize_text_with_spacy(text):
                if text is None:
                    return None
                doc = nlp(text)
                return ' '.join([token.lemma_ for token in doc])
            
            return df.withColumn(column, normalize_text_with_spacy(col(column)))
