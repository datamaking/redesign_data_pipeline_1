"""
Base data source module for the PySpark NLP pipeline.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
import logging

class BaseDataSource(ABC):
    """Abstract base class for all data sources."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the data source.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for the data source
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
    @abstractmethod
    def read(self, **kwargs) -> DataFrame:
        """
        Read data from the source.
        
        Returns:
            DataFrame: PySpark DataFrame containing the data
        """
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        """
        Validate the data source configuration and connectivity.
        
        Returns:
            bool: True if validation is successful, False otherwise
        """
        pass
    
    def _log_read_operation(self, source_details: str, rows_count: Optional[int] = None) -> None:
        """
        Log details about the read operation.
        
        Args:
            source_details: Details about the source
            rows_count: Number of rows read (if available)
        """
        if rows_count is not None:
            self.logger.info(f"Read {rows_count} rows from {source_details}")
        else:
            self.logger.info(f"Read data from {source_details}")