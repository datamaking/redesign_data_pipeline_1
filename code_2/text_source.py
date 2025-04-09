"""
Text file data source implementation.
"""
from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame
import logging
import os
from .base_source import BaseDataSource
from config.source_configs.text_config import TextSourceConfig

class TextDataSource(BaseDataSource):
    """Data source for reading from text files."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the text file data source.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for the text file source (optional)
        """
        if config is None:
            config = TextSourceConfig().get('text', {})
        super().__init__(spark, config)
        
    def read(self, path: Optional[str] = None, recursive: Optional[bool] = None, 
             encoding: Optional[str] = None, **kwargs) -> DataFrame:
        """
        Read data from text files.
        
        Args:
            path: Path pattern for text files (overrides config)
            recursive: Whether to recursively search for files (overrides config)
            encoding: Character encoding (overrides config)
            **kwargs: Additional read parameters
            
        Returns:
            DataFrame: PySpark DataFrame containing the text data
        """
        try:
            # Get parameters from config if not provided
            file_path = path or self.config.get('path', 'data/input/*.txt')
            is_recursive = recursive if recursive is not None else self.config.get('recursive', True)
            file_encoding = encoding or self.config.get('encoding', 'utf-8')
            include_path = self.config.get('include_path', True)
            include_filename = self.config.get('include_filename', True)
            include_modification_time = self.config.get('include_modification_time', True)
            
            # Set up read options
            read_options = {
                'wholetext': 'true',
                'encoding': file_encoding,
                'recursiveFileLookup': str(is_recursive).lower()
            }
            
            # Add any additional options from kwargs
            for key, value in kwargs.items():
                if key not in ['path', 'recursive', 'encoding']:
                    read_options[key] = value
                    
            # Read the text files
            self.logger.info(f"Reading text files from {file_path}")
            df = self.spark.read.format('text').options(**read_options).load(file_path)
            
            # Add file metadata if requested
            if include_path or include_filename or include_modification_time:
                df = df.withColumn('input_file', self.spark.sql.functions.input_file_name())
                
                if include_filename:
                    df = df.withColumn('filename', self.spark.sql.functions.regexp_extract('input_file', r'[^/\\\\]+$', 0))
                    
                if include_modification_time:
                    # This requires a UDF to get file modification time
                    from pyspark.sql.functions import udf
                    from pyspark.sql.types import TimestampType
                    import time
                    
                    @udf(TimestampType())
                    def get_file_mtime(file_path):
                        try:
                            return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(file_path)))
                        except:
                            return None
                            
                    df = df.withColumn('modification_time', get_file_mtime(df.input_file))
                    
                if not include_path:
                    df = df.drop('input_file')
            
            # Log the operation
            row_count = df.count()
            self._log_read_operation(f"text files at {file_path}", row_count)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading text files from {path or self.config.get('path', 'data/input/*.txt')}: {str(e)}")
            raise
            
    def validate(self) -> bool:
        """
        Validate the text file source configuration.
        
        Returns:
            bool: True if validation is successful, False otherwise
        """
        try:
            # Check if the path exists
            file_path = self.config.get('path', 'data/input/*.txt')
            
            # For glob patterns, check if the directory exists
            import glob
            if '*' in file_path or '?' in file_path:
                base_dir = os.path.dirname(file_path)
                if not os.path.exists(base_dir):
                    self.logger.warning(f"Base directory {base_dir} does not exist")
                    return False
                    
                # Check if any files match the pattern
                matching_files = glob.glob(file_path, recursive=self.config.get('recursive', True))
                if not matching_files:
                    self.logger.warning(f"No files match the pattern {file_path}")
                    return False
            else:
                # For direct file paths, check if the file exists
                if not os.path.exists(file_path):
                    self.logger.warning(f"File {file_path} does not exist")
                    return False
                    
            self.logger.info("Text file source validated successfully")
            return True
        except Exception as e:
            self.logger.error(f"Text file source validation failed: {str(e)}")
            return False