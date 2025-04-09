"""
Logging utilities for the PySpark NLP pipeline.
"""
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from typing import Optional, Dict, Any

class PipelineLogger:
    """Logger configuration for the NLP pipeline."""
    
    _instance = None  # Singleton pattern
    
    def __new__(cls, config: Optional[Dict[str, Any]] = None):
        if cls._instance is None:
            cls._instance = super(PipelineLogger, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        if self._initialized:
            return
            
        self.config = config or {}
        self.log_level = self._get_log_level()
        self.log_format = self.config.get('log_format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.log_file = self.config.get('log_file', 'pipeline.log')
        self.max_log_size = self.config.get('max_log_size', 10 * 1024 * 1024)  # 10 MB
        self.backup_count = self.config.get('backup_count', 5)
        
        # Configure the root logger
        self._configure_root_logger()
        
        # Create a logger for this class
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Logging system initialized")
        
        self._initialized = True
        
    def _get_log_level(self) -> int:
        """Get the log level from the configuration."""
        level_str = self.config.get('log_level', 'INFO').upper()
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        return level_map.get(level_str, logging.INFO)
        
    def _configure_root_logger(self) -> None:
        """Configure the root logger."""
        # Create log directory if it doesn't exist
        log_dir = os.path.dirname(self.log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        # Configure the root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(self.log_level)
        
        # Remove existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            
        # Create a formatter
        formatter = logging.Formatter(self.log_format)
        
        # Create a console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
        
        # Create a file handler
        file_handler = RotatingFileHandler(
            self.log_file,
            maxBytes=self.max_log_size,
            backupCount=self.backup_count
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
        
    def get_logger(self, name: str) -> logging.Logger:
        """
        Get a logger with the specified name.
        
        Args:
            name: Name of the logger
            
        Returns:
            logging.Logger: Logger instance
        """
        return logging.getLogger(name)