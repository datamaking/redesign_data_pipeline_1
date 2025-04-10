NLP ETL Data Pipeline with PySpark
I'll build a comprehensive NLP ETL data pipeline using PySpark with the modules you've specified, implementing the five design patterns you mentioned. Let's start by creating the project structure and core modules.
First, let me create an artifact with the project structure and implementation:
NLP ETL Pipeline with PySparkCode 
Now, let's implement each module one by one. I'll start with the core components: configuration, logging, and exception handling.
Configuration ModuleCode 
Now, let's implement the logging and exception handling modules:
Logging and Exception Handling ModuleCode 
Now, let's implement the source module with the Factory pattern:
Data Source ModuleCode 
Now, let's implement the preprocessing module with the Strategy pattern:



# Project Structure
"""
nlp_etl_pipeline/
│
├── config/
│   ├── __init__.py
│   ├── base_config.py
│   ├── config_manager.py
│   ├── source_configs/
│   │   ├── __init__.py
│   │   ├── file_config.py
│   │   ├── rdbms_config.py
│   │   ├── hive_config.py
│   │   ├── nosql_config.py
│   │   └── vector_db_config.py
│   ├── processing_configs/
│   │   ├── __init__.py
│   │   ├── preprocessing_config.py
│   │   ├── chunking_config.py
│   │   └── embedding_config.py
│   └── target_configs/
│       ├── __init__.py
│       ├── file_config.py
│       ├── rdbms_config.py
│       ├── hive_config.py
│       ├── nosql_config.py
│       └── vector_db_config.py
│
├── source/
│   ├── __init__.py
│   ├── source_factory.py
│   ├── base_source.py
│   ├── file_source.py
│   ├── rdbms_source.py
│   ├── hive_source.py
│   ├── nosql_source.py
│   └── vector_db_source.py
│
├── preprocessing/
│   ├── __init__.py
│   ├── preprocessing_factory.py
│   ├── base_preprocessor.py
│   ├── html_parser.py
│   └── data_cleaner.py
│
├── chunking/
│   ├── __init__.py
│   ├── chunking_factory.py
│   ├── base_chunker.py
│   ├── fixed_size_chunker.py
│   ├── sentence_chunker.py
│   ├── paragraph_chunker.py
│   └── chunk_smoother.py
│
├── embedding/
│   ├── __init__.py
│   ├── embedding_factory.py
│   ├── base_embedder.py
│   ├── tfidf_embedder.py
│   └── sentence_embedder.py
│
├── target/
│   ├── __init__.py
│   ├── target_factory.py
│   ├── base_target.py
│   ├── file_target.py
│   ├── rdbms_target.py
│   ├── hive_target.py
│   ├── nosql_target.py
│   └── vector_db_target.py
│
├── utils/
│   ├── __init__.py
│   ├── logging_util.py
│   └── exception_handler.py
│
├── pipeline/
│   ├── __init__.py
│   ├── etl_pipeline.py
│   └── pipeline_builder.py
│
├── requirements.txt
├── README.md
└── main.py
"""


# config/base_config.py
class BaseConfig:
    """Base configuration class for all configurations."""
    
    def __init__(self, config_dict=None):
        self.config_dict = config_dict or {}
    
    def get(self, key, default=None):
        """Get a configuration value by key."""
        return self.config_dict.get(key, default)
    
    def set(self, key, value):
        """Set a configuration value."""
        self.config_dict[key] = value
    
    def update(self, config_dict):
        """Update configuration with a dictionary."""
        self.config_dict.update(config_dict)
    
    def to_dict(self):
        """Convert configuration to dictionary."""
        return self.config_dict.copy()


# config/config_manager.py
class ConfigManager:
    """Singleton class to manage all configurations."""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._configs = {}
        return cls._instance
    
    def register_config(self, config_type, config):
        """Register a configuration by type."""
        self._configs[config_type] = config
    
    def get_config(self, config_type):
        """Get a configuration by type."""
        return self._configs.get(config_type)
    
    def combine_configs(self, config_types):
        """Combine multiple configurations."""
        combined_config = {}
        for config_type in config_types:
            config = self.get_config(config_type)
            if config:
                combined_config.update(config.to_dict())
        return combined_config


# config/source_configs/file_config.py
from ..base_config import BaseConfig

class FileSourceConfig(BaseConfig):
    """Configuration for file data sources."""
    
    def __init__(self, file_path, file_format="text", encoding="utf-8", **kwargs):
        config = {
            "file_path": file_path,
            "file_format": file_format,  # text, csv, json, html, xml, etc.
            "encoding": encoding,
            **kwargs
        }
        super().__init__(config)


# config/source_configs/rdbms_config.py
from ..base_config import BaseConfig

class RDBMSSourceConfig(BaseConfig):
    """Configuration for RDBMS data sources."""
    
    def __init__(self, jdbc_url, table_name, user, password, driver="org.postgresql.Driver", **kwargs):
        config = {
            "jdbc_url": jdbc_url,
            "table_name": table_name,
            "user": user,
            "password": password,
            "driver": driver,
            **kwargs
        }
        super().__init__(config)


# config/source_configs/hive_config.py
from ..base_config import BaseConfig

class HiveSourceConfig(BaseConfig):
    """Configuration for Hive data sources."""
    
    def __init__(self, database, table, partition_column=None, partition_values=None, **kwargs):
        config = {
            "database": database,
            "table": table,
            "partition_column": partition_column,
            "partition_values": partition_values,
            **kwargs
        }
        super().__init__(config)


# config/source_configs/nosql_config.py
from ..base_config import BaseConfig

class NoSQLSourceConfig(BaseConfig):
    """Configuration for NoSQL data sources."""
    
    def __init__(self, connection_string, database, collection, nosql_type="mongodb", **kwargs):
        config = {
            "connection_string": connection_string,
            "database": database,
            "collection": collection,
            "nosql_type": nosql_type,  # mongodb, cassandra, etc.
            **kwargs
        }
        super().__init__(config)


# config/source_configs/vector_db_config.py
from ..base_config import BaseConfig

class VectorDBSourceConfig(BaseConfig):
    """Configuration for vector database sources."""
    
    def __init__(self, connection_string, collection_name, db_type, dimension=None, **kwargs):
        config = {
            "connection_string": connection_string,
            "collection_name": collection_name,
            "db_type": db_type,  # chroma, postgres, neo4j
            "dimension": dimension,
            **kwargs
        }
        super().__init__(config)


# config/processing_configs/preprocessing_config.py
from ..base_config import BaseConfig

class PreprocessingConfig(BaseConfig):
    """Configuration for preprocessing operations."""
    
    def __init__(self, remove_html=True, remove_stopwords=True, lowercase=True, **kwargs):
        config = {
            "remove_html": remove_html,
            "remove_stopwords": remove_stopwords,
            "lowercase": lowercase,
            **kwargs
        }
        super().__init__(config)


# config/processing_configs/chunking_config.py
from ..base_config import BaseConfig

class ChunkingConfig(BaseConfig):
    """Configuration for text chunking operations."""
    
    def __init__(self, chunking_method="fixed_size", chunk_size=1000, chunk_overlap=200, **kwargs):
        config = {
            "chunking_method": chunking_method,  # fixed_size, sentence, paragraph
            "chunk_size": chunk_size,
            "chunk_overlap": chunk_overlap,
            **kwargs
        }
        super().__init__(config)


# config/processing_configs/embedding_config.py
from ..base_config import BaseConfig

class EmbeddingConfig(BaseConfig):
    """Configuration for text embedding operations."""
    
    def __init__(self, embedding_type="tfidf", model_name=None, dimension=100, **kwargs):
        config = {
            "embedding_type": embedding_type,  # tfidf, sentence
            "model_name": model_name,  # for pre-trained models
            "dimension": dimension,
            **kwargs
        }
        super().__init__(config)


# config/target_configs/file_config.py
from ..base_config import BaseConfig

class FileTargetConfig(BaseConfig):
    """Configuration for file data targets."""
    
    def __init__(self, file_path, file_format="text", encoding="utf-8", **kwargs):
        config = {
            "file_path": file_path,
            "file_format": file_format,  # text, csv, json, parquet, etc.
            "encoding": encoding,
            **kwargs
        }
        super().__init__(config)


# config/target_configs/rdbms_config.py
from ..base_config import BaseConfig

class RDBMSTargetConfig(BaseConfig):
    """Configuration for RDBMS data targets."""
    
    def __init__(self, jdbc_url, table_name, user, password, driver="org.postgresql.Driver", 
                 save_mode="append", **kwargs):
        config = {
            "jdbc_url": jdbc_url,
            "table_name": table_name,
            "user": user,
            "password": password,
            "driver": driver,
            "save_mode": save_mode,  # overwrite, append, ignore, error
            **kwargs
        }
        super().__init__(config)


# config/target_configs/hive_config.py
from ..base_config import BaseConfig

class HiveTargetConfig(BaseConfig):
    """Configuration for Hive data targets."""
    
    def __init__(self, database, table, partition_columns=None, save_mode="append", **kwargs):
        config = {
            "database": database,
            "table": table,
            "partition_columns": partition_columns,
            "save_mode": save_mode,  # overwrite, append, ignore, error
            **kwargs
        }
        super().__init__(config)


# config/target_configs/nosql_config.py
from ..base_config import BaseConfig

class NoSQLTargetConfig(BaseConfig):
    """Configuration for NoSQL data targets."""
    
    def __init__(self, connection_string, database, collection, nosql_type="mongodb", **kwargs):
        config = {
            "connection_string": connection_string,
            "database": database,
            "collection": collection,
            "nosql_type": nosql_type,  # mongodb, cassandra, etc.
            **kwargs
        }
        super().__init__(config)


# config/target_configs/vector_db_config.py
from ..base_config import BaseConfig

class VectorDBTargetConfig(BaseConfig):
    """Configuration for vector database targets."""
    
    def __init__(self, connection_string, collection_name, db_type, dimension=None, **kwargs):
        config = {
            "connection_string": connection_string,
            "collection_name": collection_name,
            "db_type": db_type,  # chroma, postgres, neo4j
            "dimension": dimension,
            **kwargs
        }
        super().__init__(config)


# utils/logging_util.py
import logging
import os
from datetime import datetime

class Logger:
    """Singleton logger class for the application."""
    _instance = None
    
    def __new__(cls, log_level=logging.INFO, log_file=None):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._init_logger(log_level, log_file)
        return cls._instance
    
    def _init_logger(self, log_level, log_file):
        """Initialize the logger."""
        self.logger = logging.getLogger('nlp_etl_pipeline')
        self.logger.setLevel(log_level)
        
        # Create log directory if it doesn't exist
        if log_file:
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)
        
        # Add console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)
        
        # Add file handler if log file is specified
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(log_level)
            file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_format)
            self.logger.addHandler(file_handler)
    
    def get_logger(self):
        """Get the logger instance."""
        return self.logger


# utils/exception_handler.py
import sys
import traceback
from datetime import datetime
from functools import wraps

class ETLException(Exception):
    """Base exception class for the ETL pipeline."""
    
    def __init__(self, message, error_code=None, source=None):
        self.message = message
        self.error_code = error_code
        self.source = source
        self.timestamp = datetime.now()
        super().__init__(self.message)
    
    def __str__(self):
        return f"{self.error_code}: {self.message} (Source: {self.source}, Time: {self.timestamp})"


class ExceptionHandler:
    """Class for handling exceptions in the ETL pipeline."""
    
    @staticmethod
    def handle_exception(logger, e, stage=None):
        """Handle exceptions and log them."""
        if isinstance(e, ETLException):
            logger.error(f"ETL Exception at stage {stage}: {str(e)}")
        else:
            logger.error(f"Exception at stage {stage}: {str(e)}")
            logger.error(traceback.format_exc())
    
    @staticmethod
    def exception_decorator(logger):
        """Decorator for exception handling."""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    ExceptionHandler.handle_exception(logger, e, func.__name__)
                    raise
            return wrapper
        return decorator



# source/base_source.py
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession

class BaseSource(ABC):
    """Abstract base class for all data sources."""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
    
    @abstractmethod
    def read(self):
        """Read data from the source and return a DataFrame."""
        pass
    
    def validate_config(self):
        """Validate the configuration."""
        return True


# source/source_factory.py
class SourceFactory:
    """Factory class for creating data sources."""
    
    @staticmethod
    def create_source(source_type, spark, config):
        """Create a source instance based on the source type."""
        if source_type == "file":
            from .file_source import FileSource
            return FileSource(spark, config)
        elif source_type == "rdbms":
            from .rdbms_source import RDBMSSource
            return RDBMSSource(spark, config)
        elif source_type == "hive":
            from .hive_source import HiveSource
            return HiveSource(spark, config)
        elif source_type == "nosql":
            from .nosql_source import NoSQLSource
            return NoSQLSource(spark, config)
        elif source_type == "vector_db":
            from .vector_db_source import VectorDBSource
            return VectorDBSource(spark, config)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")


# source/file_source.py
from .base_source import BaseSource
from utils.exception_handler import ETLException

class FileSource(BaseSource):
    """Class for reading data from file sources."""
    
    def validate_config(self):
        """Validate the file source configuration."""
        if not self.config.get("file_path"):
            raise ETLException("File path is required", error_code="INVALID_CONFIG", source="FileSource")
        return True
    
    def read(self):
        """Read data from a file source."""
        self.validate_config()
        
        file_path = self.config.get("file_path")
        file_format = self.config.get("file_format", "text")
        options = {k: v for k, v in self.config.to_dict().items() 
                   if k not in ["file_path", "file_format"]}
        
        reader = self.spark.read
        
        # Apply options
        for key, value in options.items():
            if value is not None:
                reader = reader.option(key, value)
        
        # Read based on format
        if file_format == "text":
            return reader.text(file_path)
        elif file_format == "csv":
            return reader.csv(file_path, header=self.config.get("header", True))
        elif file_format == "json":
            return reader.json(file_path)
        elif file_format == "parquet":
            return reader.parquet(file_path)
        elif file_format == "orc":
            return reader.orc(file_path)
        elif file_format == "html":
            # For HTML files, read as text and then process
            return reader.text(file_path)
        else:
            raise ETLException(f"Unsupported file format: {file_format}", 
                              error_code="UNSUPPORTED_FORMAT", source="FileSource")


# source/rdbms_source.py
from .base_source import BaseSource
from utils.exception_handler import ETLException

class RDBMSSource(BaseSource):
    """Class for reading data from RDBMS sources."""
    
    def validate_config(self):
        """Validate the RDBMS source configuration."""
        required_fields = ["jdbc_url", "table_name", "user", "password"]
        for field in required_fields:
            if not self.config.get(field):
                raise ETLException(f"{field} is required", error_code="INVALID_CONFIG", source="RDBMSSource")
        return True
    
    def read(self):
        """Read data from an RDBMS source."""
        self.validate_config()
        
        jdbc_url = self.config.get("jdbc_url")
        table_name = self.config.get("table_name")
        properties = {
            "user": self.config.get("user"),
            "password": self.config.get("password"),
            "driver": self.config.get("driver", "org.postgresql.Driver")
        }
        
        # Add any additional properties
        for key, value in self.config.to_dict().items():
            if key not in ["jdbc_url", "table_name", "user", "password", "driver"] and value is not None:
                properties[key] = value
        
        return self.spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)


# source/hive_source.py
from .base_source import BaseSource
from utils.exception_handler import ETLException

class HiveSource(BaseSource):
    """Class for reading data from Hive sources."""
    
    def validate_config(self):
        """Validate the Hive source configuration."""
        required_fields = ["database", "table"]
        for field in required_fields:
            if not self.config.get(field):
                raise ETLException(f"{field} is required", error_code="INVALID_CONFIG", source="HiveSource")
        return True
    
    def read(self):
        """Read data from a Hive source."""
        self.validate_config()
        
        database = self.config.get("database")
        table = self.config.get("table")
        partition_column = self.config.get("partition_column")
        partition_values = self.config.get("partition_values")
        
        # Create the full table name
        full_table_name = f"{database}.{table}"
        
        # Read from Hive table
        if partition_column and partition_values:
            # Build partition filter
            partition_filters = []
            if isinstance(partition_values, list):
                for value in partition_values:
                    partition_filters.append(f"{partition_column} = '{value}'")
                filter_clause = " OR ".join(partition_filters)
            else:
                filter_clause = f"{partition_column} = '{partition_values}'"
            
            return self.spark.sql(f"SELECT * FROM {full_table_name} WHERE {filter_clause}")
        else:
            return self.spark.table(full_table_name)


# source/nosql_source.py
from .base_source import BaseSource
from utils.exception_handler import ETLException

class NoSQLSource(BaseSource):
    """Class for reading data from NoSQL sources."""
    
    def validate_config(self):
        """Validate the NoSQL source configuration."""
        required_fields = ["connection_string", "database", "collection", "nosql_type"]
        for field in required_fields:
            if not self.config.get(field):
                raise ETLException(f"{field} is required", error_code="INVALID_CONFIG", source="NoSQLSource")
        return True
    
    def read(self):
        """Read data from a NoSQL source."""
        self.validate_config()
        
        nosql_type = self.config.get("nosql_type")
        connection_string = self.config.get("connection_string")
        database = self.config.get("database")
        collection = self.config.get("collection")
        
        if nosql_type == "mongodb":
            # MongoDB connection options
            read_options = {
                "uri": connection_string,
                "database": database,
                "collection": collection
            }
            
            # Add any additional options
            for key, value in self.config.to_dict().items():
                if key not in ["connection_string", "database", "collection", "nosql_type"] and value is not None:
                    read_options[key] = value
            
            return self.spark.read.format("mongo").options(**read_options).load()
        else:
            raise ETLException(f"Unsupported NoSQL type: {nosql_type}", 
                              error_code="UNSUPPORTED_NOSQL_TYPE", source="NoSQLSource")


# source/vector_db_source.py
from .base_source import BaseSource
from utils.exception_handler import ETLException

class VectorDBSource(BaseSource):
    """Class for reading data from vector database sources."""
    
    def validate_config(self):
        """Validate the vector database source configuration."""
        required_fields = ["connection_string", "collection_name", "db_type"]
        for field in required_fields:
            if not self.config.get(field):
                raise ETLException(f"{field} is required", error_code="INVALID_CONFIG", source="VectorDBSource")
        return True
    
    def read(self):
        """Read data from a vector database source."""
        self.validate_config()
        
        db_type = self.config.get("db_type")
        connection_string = self.config.get("connection_string")
        collection_name = self.config.get("collection_name")
        
        if db_type == "postgres":
            # PostgreSQL with vector extension
            jdbc_properties = {
                "url": connection_string,
                "dbtable": collection_name,
                "driver": "org.postgresql.Driver"
            }
            
            # Add any additional properties
            for key, value in self.config.to_dict().items():
                if key not in ["connection_string", "collection_name", "db_type", "dimension"] and value is not None:
                    jdbc_properties[key] = value
            
            return self.spark.read.jdbc(
                url=jdbc_properties["url"],
                table=jdbc_properties["dbtable"],
                properties=jdbc_properties
            )
        elif db_type == "neo4j":
            # Neo4j connection
            neo4j_options = {
                "url": connection_string,
                "labels": collection_name
            }
            
            # Add any additional options
            for key, value in self.config.to_dict().items():
                if key not in ["connection_string", "collection_name", "db_type", "dimension"] and value is not None:
                    neo4j_options[key] = value
            
            return self.spark.read.format("org.neo4j.spark.DataSource") \
                .options(**neo4j_options) \
                .load()
        elif db_type == "chroma":
            # For ChromaDB, which doesn't have direct Spark integration,
            # you might need to use a custom solution or API calls
            # This is a simplified approach using a dummy DataFrame
            raise ETLException("ChromaDB direct integration not supported. Use API and convert to DataFrame.", 
                              error_code="UNSUPPORTED_OPERATION", source="VectorDBSource")
        else:
            raise ETLException(f"Unsupported vector database type: {db_type}", 
                              error_code="UNSUPPORTED_VECTOR_DB", source="VectorDBSource")


# preprocessing/base_preprocessor.py
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class BasePreprocessor(ABC):
    """Abstract base class for all preprocessors."""
    
    def __init__(self, config=None):
        self.config = config or {}
    
    @abstractmethod
    def process(self, df: DataFrame) -> DataFrame:
        """Process the input DataFrame and return the processed DataFrame."""
        pass


# preprocessing/preprocessing_factory.py
class PreprocessingFactory:
    """Factory class for creating preprocessors."""
    
    @staticmethod
    def create_preprocessor(preprocessor_type, config=None):
        """Create a preprocessor instance based on the type."""
        if preprocessor_type == "html_parser":
            from .html_parser import HTMLParser
            return HTMLParser(config)
        elif preprocessor_type == "data_cleaner":
            from .data_cleaner import DataCleaner
            return DataCleaner(config)
        else:
            raise ValueError(f"Unsupported preprocessor type: {preprocessor_type}")


# preprocessing/html_parser.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re
from bs4 import BeautifulSoup

from .base_preprocessor import BasePreprocessor

class HTMLParser(BasePreprocessor):
    """Class for parsing HTML content in a DataFrame."""
    
    def process(self, df: DataFrame) -> DataFrame:
        """Parse HTML content in the specified column."""
        text_column = self.config.get("text_column", "text")
        output_column = self.config.get("output_column", "parsed_text")
        extract_tags = self.config.get("extract_tags", [])
        
        # Define HTML parsing function
        @udf(StringType())
        def parse_html(html_content):
            if not html_content:
                return None
            
            try:
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Remove script and style elements
                for script in soup(["script", "style"]):
                    script.extract()
                
                # Extract text from specific tags if specified
                if extract_tags:
                    extracted_text = []
                    for tag in extract_tags:
                        elements = soup.find_all(tag)
                        for element in elements:
                            extracted_text.append(element.get_text())
                    text = " ".join(extracted_text)
                else:
                    # Get all text
                    text = soup.get_text()
                
                # Handle whitespace
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                text = " ".join(chunk for chunk in chunks if chunk)
                
                return text
            except Exception as e:
                # Return original content in case of parsing errors
                return html_content
        
        # Apply the UDF to the DataFrame
        return df.withColumn(output_column, parse_html(df[text_column]))


# preprocessing/data_cleaner.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, lower, regexp_replace, trim
from pyspark.sql.types import StringType, ArrayType
from pyspark.ml.feature import StopWordsRemover
import re

from .base_preprocessor import BasePreprocessor

class DataCleaner(BasePreprocessor):
    """Class for cleaning text data in a DataFrame."""
    
    def process(self, df: DataFrame) -> DataFrame:
        """Clean text data in the specified column."""
        text_column = self.config.get("text_column", "text")
        output_column = self.config.get("output_column", "cleaned_text")
        lowercase = self.config.get("lowercase", True)
        remove_punctuation = self.config.get("remove_punctuation", True)
        remove_numbers = self.config.get("remove_numbers", False)
        remove_stopwords = self.config.get("remove_stopwords", True)
        remove_extra_whitespace = self.config.get("remove_extra_whitespace", True)
        
        result_df = df
        
        # Apply transformations in sequence
        if lowercase:
            result_df = result_df.withColumn(text_column, lower(result_df[text_column]))
        
        if remove_punctuation:
            result_df = result_df.withColumn(text_column, 
                                            regexp_replace(result_df[text_column], "[^\\
                                            
                                                                                          