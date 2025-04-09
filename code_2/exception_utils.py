"""
Exception handling utilities for the PySpark NLP pipeline.
"""
import logging
import traceback
import sys
from functools import wraps
from typing import Callable, Any, Type, Union, List, Optional

class PipelineException(Exception):
    """Base exception class for the NLP pipeline."""
    
    def __init__(self, message: str, original_exception: Optional[Exception] = None):
        """
        Initialize the pipeline exception.
        
        Args:
            message: Error message
            original_exception: Original exception that was caught (optional)
        """
        self.message = message
        self.original_exception = original_exception
        super().__init__(self.message)
        
    def __str__(self) -> str:
        """String representation of the exception."""
        if self.original_exception:
            return f"{self.message} (Original exception: {str(self.original_exception)})"
        return self.message

class DataSourceException(PipelineException):
    """Exception raised by data source components."""
    pass

class PreprocessingException(PipelineException):
    """Exception raised by preprocessing components."""
    pass

class ChunkingException(PipelineException):
    """Exception raised by chunking components."""
    pass

class EmbeddingException(PipelineException):
    """Exception raised by embedding components."""
    pass

class DataTargetException(PipelineException):
    """Exception raised by data target components."""
    pass

class VectorSearchException(PipelineException):
    """Exception raised by vector search components."""
    pass

def exception_handler(logger: Optional[logging.Logger] = None, 
                     raise_exception: bool = True,
                     exception_type: Type[Exception] = PipelineException,
                     exit_on_error: bool = False) -> Callable:
    """
    Decorator for handling exceptions in functions.
    
    Args:
        logger: Logger instance (optional)
        raise_exception: Whether to raise the exception after handling
        exception_type: Type of exception to raise
        exit_on_error: Whether to exit the program on error
        
    Returns:
        Callable: Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            nonlocal logger
            if logger is None:
                logger = logging.getLogger(func.__module__)
                
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Get the traceback
                tb = traceback.format_exc()
                
                # Log the error
                error_message = f"Error in {func.__name__}: {str(e)}"
                logger.error(error_message)
                logger.debug(tb)
                
                # Raise or return
                if raise_exception:
                    raise exception_type(error_message, e)
                elif exit_on_error:
                    logger.critical("Exiting due to error")
                    sys.exit(1)
                return None
                
        return wrapper
    return decorator

def retry(max_attempts: int = 3, 
         exceptions: Union[Type[Exception], List[Type[Exception]]] = Exception,
         logger: Optional[logging.Logger] = None,
         backoff_factor: float = 1.0) -> Callable:
    """
    Decorator for retrying a function on exception.
    
    Args:
        max_attempts: Maximum number of attempts
        exceptions: Exception type(s) to catch
        logger: Logger instance (optional)
        backoff_factor: Factor to multiply the delay between retries
        
    Returns:
        Callable: Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            nonlocal logger
            if logger is None:
                logger = logging.getLogger(func.__module__)
                
            import time
            
            attempt = 1
            while attempt <= max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        logger.error(f"Failed after {max_attempts} attempts: {str(e)}")
                        raise
                        
                    wait_time = backoff_factor * (2 ** (attempt - 1))
                    logger.warning(f"Attempt {attempt} failed: {str(e)}. Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                    attempt += 1
                    
        return wrapper
    return decorator