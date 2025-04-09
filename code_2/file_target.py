"""
File data target implementation.
"""
from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame
import logging
import os
from .base_target import BaseDataTarget
from config.target_configs.file_config import FileTargetConfig

class FileDataTarget(BaseDataTarget):
    """Data target for writing to files."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the file data target.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for the file target (optional)
        """
        if config is None:
            config = FileTargetConfig().get('file', {})
        super().__init__(spark, config)
        
    def write(self, df: DataFrame, path: Optional[str] = None, format: Optional[str] = None,
              mode: Optional[str] = None, partition_by: Optional[List[str]] = None,
              compression: Optional[str] = None, **kwargs) -> None:
        """
        Write data to files.
        
        Args:
            df: DataFrame to write
            path: Output path (overrides config)
            format: File format (overrides config)
            mode: Write mode (overrides config)
            partition_by: Partition columns (overrides config)
            compression: Compression codec (overrides config)
            **kwargs: Additional write parameters
        """
        try:
            # Get parameters from config if not provided
            file_path = path or self.config.get('path', 'data/output/')
            file_format = format or self.config.get('format', 'parquet')
            write_mode = mode or self.config.get('mode', 'overwrite')
            partition_columns = partition_by or self.config.get('partition_by', [])
            compression_codec = compression or self.config.get('compression', 'snappy')
            
            # Create the directory if it doesn't exist
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Set up write options
            write_options = {}
            
            # Add format-specific options
            if file_format.lower() == 'csv':
                write_options.update({
                    'header': str(self.config.get('header', True)).lower(),
                    'delimiter': self.config.get('delimiter', ','),
                    'quote': self.config.get('quote', '"'),
                    'escape': self.config.get('escape', '\\'),
                    'encoding': self.config.get('encoding', 'UTF-8')
                })
            elif file_format.lower() in ['json', 'parquet', 'orc', 'avro']:
                write_options['compression'] = compression_codec
                
            # Add date/timestamp formats if provided
            date_format = self.config.get('date_format')
            if date_format:
                write_options['dateFormat'] = date_format
                
            timestamp_format = self.config.get('timestamp_format')
            if timestamp_format:
                write_options['timestampFormat'] = timestamp_format
                
            # Add any additional options from kwargs
            for key, value in kwargs.items():
                if key not in ['path', 'format', 'mode', 'partition_by', 'compression']:
                    write_options[key] = value
                    
            # Add any additional options from config
            for key, value in self.config.get('options', {}).items():
                if key not in write_options:
                    write_options[key] = value
                    
            # Create the writer
            writer = df.write.format(file_format).mode(write_mode)
            
            # Add options
            for key, value in write_options.items():
                writer = writer.option(key, value)
                
            # Add partitioning if specified
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
                
            # Write the data
            self.logger.info(f"Writing data to {file_format} files at {file_path}")
            writer.save(file_path)
            
            # Log the operation
            self._log_write_operation(f"{file_format} files at {file_path}", df.count())
            
        except Exception as e:
            self.logger.error(f"Error writing to files: {str(e)}")
            raise
            
    def validate(self) -> bool:
        """
        Validate the file target configuration.
        
        Returns:
            bool: True if validation is successful, False otherwise
        """
        try:
            # Check if the path is valid
            file_path = self.config.get('path', 'data/output/')
            
            # Create the directory if it doesn't exist
            try:
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
            except Exception as e:
                self.logger.error(f"Failed to create directory {os.path.dirname(file_path)}: {str(e)}")
                return False
                
            # Check if the format is supported
            file_format = self.config.get('format', 'parquet').lower()
            supported_formats = ['parquet', 'orc', 'avro', 'csv', 'json', 'text']
            if file_format not in supported_formats:
                self.logger.error(f"Unsupported file format: {file_format}. Supported formats: {', '.join(supported_formats)}")
                return False
                
            # Check if the mode is valid
            write_mode = self.config.get('mode', 'overwrite').lower()
            valid_modes = ['append', 'overwrite', 'ignore', 'error']
            if write_mode not in valid_modes:
                self.logger.error(f"Invalid write mode: {write_mode}. Valid modes: {', '.join(valid_modes)}")
                return False
                
            self.logger.info("File target configuration validated successfully")
            return True
        except Exception as e:
            self.logger.error(f"File target validation failed: {str(e)}")
            return False