"""
Base data target module for the PySpark NLP pipeline.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
import logging

class BaseDataTarget(ABC):
    """Abstract base class for all data targets."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the data target.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for the data target
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
    @abstractmethod
    def write(self, df: DataFrame, **kwargs) -> None:
        """
        Write data to the target.
        
        Args:
            df: DataFrame to write
            **kwargs: Additional write parameters
        """
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        """
        Validate the data target configuration and connectivity.
        
        Returns:
            bool: True if validation is successful, False otherwise
        """
        pass
    
    def _log_write_operation(self, target_details: str, rows_count: Optional[int] = None) -> None:
        """
        Log details about the write operation.
        
        Args:
            target_details: Details about the target
            rows_count: Number of rows written (if available)
        """
        if rows_count is not None:
            self.logger.info(f"Wrote {rows_count} rows to {target_details}")
        else:
            self.logger.info(f"Wrote data to {target_details}")