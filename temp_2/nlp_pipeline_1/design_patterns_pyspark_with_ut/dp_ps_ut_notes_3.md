Below is a complete, end‐to‐end example of an NLP ETL pipeline implemented in PySpark. In this example, every module is built using only the following design patterns: **Singleton**, **Factory**, **Strategy**, **Template Method** and **Builder**. The code is organized into multiple Python packages and modules corresponding to configuration management, data sources, preprocessing, chunking, vector embedding creation, targets, logging and exception handling. In addition, there is a main driver and pytest‐based test cases for each module along with a README and requirements file.

> **Note:** In a real project you would add real connection parameters and logic (for example, connecting to Hive, MongoDB, etc.), but for clarity each module here includes sample methods and “dummy” operations (often printing a message or returning a static value). You can expand these implementations as needed.

---

## Project Structure

```
nlp_etl_pipeline/
├── config
│   ├── __init__.py
│   ├── common_config.py
│   ├── file_config.py
│   ├── hive_config.py
│   ├── nosql_config.py
│   ├── rdbms_config.py
│   ├── vectordb_config.py
│   └── config_manager.py
├── datasource
│   ├── __init__.py
│   ├── base_datasource.py
│   ├── datasource_factory.py
│   ├── file_datasource.py
│   ├── hive_datasource.py
│   ├── nosql_datasource.py
│   ├── rdbms_datasource.py
│   └── vectordb_datasource.py
├── exception
│   ├── __init__.py
│   └── etl_exception.py
├── logging_module
│   ├── __init__.py
│   └── logger.py
├── preprocessing
│   ├── __init__.py
│   ├── data_cleaner.py
│   └── html_parser.py
├── target
│   ├── __init__.py
│   ├── base_target.py
│   ├── file_target.py
│   ├── hive_target.py
│   ├── nosql_target.py
│   ├── rdbms_target.py
│   ├── target_factory.py
│   └── vectordb_target.py
├── chunking
│   ├── __init__.py
│   ├── base_chunker.py
│   ├── chunk_smoothing.py
│   ├── advanced_chunker.py
│   └── simple_chunker.py
├── embedding
│   ├── __init__.py
│   ├── base_embedding.py
│   ├── sentence_embedding.py
│   └── tfidf_embedding.py
├── tests
│   ├── __init__.py
│   ├── test_config.py
│   ├── test_datasource.py
│   ├── test_preprocessing.py
│   ├── test_chunking.py
│   ├── test_embedding.py
│   └── test_target.py
├── main.py
├── requirements.txt
└── README.md
```

Each package is responsible for part of the pipeline. The following sections provide the complete code for each module.

---

## 1. Configuration Module

### config/common_config.py
```python
# config/common_config.py
COMMON_CONFIG = {
    "spark_app_name": "NLP_ETL_Pipeline",
    "spark_master": "local[*]",
    "log_level": "INFO"
}
```

### config/file_config.py
```python
# config/file_config.py
FILE_CONFIG = {
    "input_path": "/path/to/input/files",
    "output_path": "/path/to/output/files",
    "file_format": "json"  # could be 'text', 'html', etc.
}
```

### config/hive_config.py
```python
# config/hive_config.py
HIVE_CONFIG = {
    "database": "default",
    "table": "my_hive_table",
    "hive_metastore": "thrift://localhost:9083"
}
```

### config/rdbms_config.py
```python
# config/rdbms_config.py
RDBMS_CONFIG = {
    "url": "jdbc:postgresql://localhost:5432/mydb",
    "table": "my_table",
    "user": "myuser",
    "password": "mypassword"
}
```

### config/nosql_config.py
```python
# config/nosql_config.py
NOSQL_CONFIG = {
    "uri": "mongodb://localhost:27017/",
    "database": "myMongoDB",
    "collection": "my_collection"
}
```

### config/vectordb_config.py
```python
# config/vectordb_config.py
VECTORDB_CONFIG = {
    "type": "chromadb",  # could be 'postgresql' or 'neo4j'
    "connection_params": {"host": "localhost", "port": 1234}
}
```

### config/config_manager.py  
This module uses the **Singleton** pattern to ensure one configuration manager instance. It also uses a simple **Builder** pattern for assembling the final configuration.
```python
# config/config_manager.py
from config.common_config import COMMON_CONFIG
from config.file_config import FILE_CONFIG
from config.hive_config import HIVE_CONFIG
from config.rdbms_config import RDBMS_CONFIG
from config.nosql_config import NOSQL_CONFIG
from config.vectordb_config import VECTORDB_CONFIG

class ConfigManager:
    __instance = None

    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super(ConfigManager, cls).__new__(cls)
        return cls.__instance

    def __init__(self):
        self.config = {}

    def build_config(self, config_types):
        # Example: config_types = ['common', 'file', 'hive']
        if "common" in config_types:
            self.config.update(COMMON_CONFIG)
        if "file" in config_types:
            self.config.update(FILE_CONFIG)
        if "hive" in config_types:
            self.config.update(HIVE_CONFIG)
        if "rdbms" in config_types:
            self.config.update(RDBMS_CONFIG)
        if "nosql" in config_types:
            self.config.update(NOSQL_CONFIG)
        if "vectordb" in config_types:
            self.config.update(VECTORDB_CONFIG)
        return self.config

# Usage:
# config_manager = ConfigManager()
# config = config_manager.build_config(['common','file','hive'])
```

---

## 2. Data Source Module

We create an abstract base class and concrete implementations for each source. Then we add a factory (using the **Factory** pattern) to create data source objects.

### datasource/base_datasource.py
```python
# datasource/base_datasource.py
from pyspark.sql import DataFrame

class BaseDataSource:
    def __init__(self, config: dict):
        self.config = config

    def read_data(self) -> DataFrame:
        """Template method – must be overridden by subclasses."""
        raise NotImplementedError("Subclasses must implement this method")
```

### datasource/hive_datasource.py
```python
# datasource/hive_datasource.py
from datasource.base_datasource import BaseDataSource

class HiveDataSource(BaseDataSource):
    def read_data(self):
        # Using the Template method pattern, implement reading from Hive.
        print("Reading data from Hive table:", self.config.get("table"))
        # In real code, you would use spark.read.table or spark.sql
        return None  # Replace with actual DataFrame
```

### datasource/file_datasource.py
```python
# datasource/file_datasource.py
from datasource.base_datasource import BaseDataSource

class FileDataSource(BaseDataSource):
    def read_data(self):
        file_format = self.config.get("file_format", "json")
        input_path = self.config.get("input_path")
        print(f"Reading {file_format} data from file: {input_path}")
        # In real code, you might call spark.read.format(file_format).load(input_path)
        return None
```

### datasource/rdbms_datasource.py
```python
# datasource/rdbms_datasource.py
from datasource.base_datasource import BaseDataSource

class RDBMSDataSource(BaseDataSource):
    def read_data(self):
        print("Reading data from RDBMS table:", self.config.get("table"))
        # Example spark.read.jdbc(...) implementation
        return None
```

### datasource/nosql_datasource.py
```python
# datasource/nosql_datasource.py
from datasource.base_datasource import BaseDataSource

class NoSQLDataSource(BaseDataSource):
    def read_data(self):
        print("Reading data from NoSQL system (MongoDB) using collection:", self.config.get("collection"))
        # Example: Use the MongoDB Spark connector
        return None
```

### datasource/vectordb_datasource.py
```python
# datasource/vectordb_datasource.py
from datasource.base_datasource import BaseDataSource

class VectorDBDataSource(BaseDataSource):
    def read_data(self):
        db_type = self.config.get("type")
        print(f"Reading data from {db_type} vector database using connection parameters: {self.config.get('connection_params')}")
        # Example: Use appropriate connector libraries
        return None
```

### datasource/datasource_factory.py  
This file implements the **Factory** pattern.
```python
# datasource/datasource_factory.py
from datasource.hive_datasource import HiveDataSource
from datasource.file_datasource import FileDataSource
from datasource.rdbms_datasource import RDBMSDataSource
from datasource.nosql_datasource import NoSQLDataSource
from datasource.vectordb_datasource import VectorDBDataSource

class DataSourceFactory:
    @staticmethod
    def get_datasource(source_type: str, config: dict):
        if source_type == "hive":
            return HiveDataSource(config)
        elif source_type == "file":
            return FileDataSource(config)
        elif source_type == "rdbms":
            return RDBMSDataSource(config)
        elif source_type == "nosql":
            return NoSQLDataSource(config)
        elif source_type == "vectordb":
            return VectorDBDataSource(config)
        else:
            raise ValueError(f"Unknown source type: {source_type}")
```

---

## 3. Preprocessing Module

Implement simple HTML parsing and data cleaning.

### preprocessing/html_parser.py
```python
# preprocessing/html_parser.py
from bs4 import BeautifulSoup

def parse_html(html_content: str) -> str:
    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    print("Parsed HTML content.")
    return text
```

### preprocessing/data_cleaner.py
```python
# preprocessing/data_cleaner.py
import re

def clean_text(text: str) -> str:
    cleaned = re.sub(r'\s+', ' ', text).strip()
    print("Cleaned text data.")
    return cleaned
```

---

## 4. Chunking Module

We use the **Strategy** pattern here so that different chunking strategies can be interchanged without modifying the core process. Additionally, a **Template Method** is used in the abstract base class.

### chunking/base_chunker.py
```python
# chunking/base_chunker.py
class BaseChunker:
    def __init__(self, text: str):
        self.text = text

    def chunk(self):
        """Template method defining the chunking algorithm."""
        chunks = self.do_chunk()
        chunks = self.smooth_chunks(chunks)
        return chunks

    def do_chunk(self):
        raise NotImplementedError("Subclasses must implement the do_chunk() method")

    def smooth_chunks(self, chunks):
        # A generic smoothing process; could be overridden if needed.
        print("Smoothing chunks...")
        # For example, merge small chunks or adjust boundaries.
        return chunks
```

### chunking/simple_chunker.py
```python
# chunking/simple_chunker.py
from chunking.base_chunker import BaseChunker

class SimpleChunker(BaseChunker):
    def do_chunk(self):
        # Simple chunking strategy: split by period.
        chunks = [chunk.strip() for chunk in self.text.split('.') if chunk]
        print("Performed simple chunking.")
        return chunks
```

### chunking/advanced_chunker.py
```python
# chunking/advanced_chunker.py
from chunking.base_chunker import BaseChunker

class AdvancedChunker(BaseChunker):
    def do_chunk(self):
        # A more advanced chunking strategy (dummy example, could be sentence tokenization etc.)
        words = self.text.split()
        chunk_size = 10  # for example, 10 words per chunk
        chunks = [' '.join(words[i:i+chunk_size]) for i in range(0, len(words), chunk_size)]
        print("Performed advanced chunking.")
        return chunks
```

### chunking/chunk_smoothing.py
```python
# chunking/chunk_smoothing.py
def smooth(chunks):
    # Apply additional smoothing logic – here we simply print a statement.
    print("Applying extra smoothing to chunks...")
    # For example: merge chunks that are too short.
    return chunks
```

*Note:* The smoothing step is also incorporated inside the base class template method.

---

## 5. Embedding Module

Again we use the **Strategy** pattern so that different embedding creation methods can be used interchangeably.

### embedding/base_embedding.py
```python
# embedding/base_embedding.py
class BaseEmbedding:
    def __init__(self, chunks):
        self.chunks = chunks

    def create_embedding(self):
        raise NotImplementedError("Subclasses must implement create_embedding()")
```

### embedding/tfidf_embedding.py
```python
# embedding/tfidf_embedding.py
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from embedding.base_embedding import BaseEmbedding

class TFIDFEmbedding(BaseEmbedding):
    def create_embedding(self):
        print("Creating TF-IDF embeddings...")
        # Dummy Spark ML pipeline code example:
        # Assume self.chunks is a list of strings.
        # In a real scenario, convert self.chunks to a Spark DataFrame.
        # Here we simply print the action.
        return {"embedding": "tfidf_vector"}  # Replace with actual vector
```

### embedding/sentence_embedding.py
```python
# embedding/sentence_embedding.py
from embedding.base_embedding import BaseEmbedding

class SentenceEmbedding(BaseEmbedding):
    def create_embedding(self):
        print("Creating Sentence embeddings...")
        # In a real implementation, you might call a sentence transformer.
        return {"embedding": "sentence_vector"}
```

---

## 6. Target Module

Similar to datasources, we define an abstract base class and several implementations for writing the results. A **Factory** is used to create the correct target writer. Full and incremental load modes are supported.

### target/base_target.py
```python
# target/base_target.py
class BaseTarget:
    def __init__(self, config: dict):
        self.config = config

    def write_data(self, data, mode="full"):
        """
        Template method to write data.
          mode: "full" for a complete reload, "incremental" for updates.
        """
        if mode not in ["full", "incremental"]:
            raise ValueError("mode must be 'full' or 'incremental'")
        self.do_write(data, mode)

    def do_write(self, data, mode):
        raise NotImplementedError("Subclasses must implement do_write()")
```

### target/hive_target.py
```python
# target/hive_target.py
from target.base_target import BaseTarget

class HiveTarget(BaseTarget):
    def do_write(self, data, mode):
        print(f"Writing data to Hive table {self.config.get('table')} using {mode} mode.")
        # Example: data.write.mode(...).saveAsTable(...)
```

### target/file_target.py
```python
# target/file_target.py
from target.base_target import BaseTarget

class FileTarget(BaseTarget):
    def do_write(self, data, mode):
        print(f"Writing data to file system at {self.config.get('output_path')} with format {self.config.get('file_format')} in {mode} mode.")
        # Example: data.write.format(...).save(...)
```

### target/rdbms_target.py
```python
# target/rdbms_target.py
from target.base_target import BaseTarget

class RDBMSTarget(BaseTarget):
    def do_write(self, data, mode):
        print(f"Writing data to RDBMS table {self.config.get('table')} in {mode} mode.")
        # Example: data.write.jdbc(url, table, mode=mode, properties=...)
```

### target/nosql_target.py
```python
# target/nosql_target.py
from target.base_target import BaseTarget

class NoSQLTarget(BaseTarget):
    def do_write(self, data, mode):
        print(f"Writing data to NoSQL system (MongoDB) collection {self.config.get('collection')} in {mode} mode.")
        # Example: Use appropriate MongoDB write methods
```

### target/vectordb_target.py
```python
# target/vectordb_target.py
from target.base_target import BaseTarget

class VectorDBTarget(BaseTarget):
    def do_write(self, data, mode):
        print(f"Writing data to vector database ({self.config.get('type')}) using connection params {self.config.get('connection_params')} in {mode} mode.")
        # Example: Use the appropriate connector to write embedding vectors
```

### target/target_factory.py  
Implements a **Factory** for target systems.
```python
# target/target_factory.py
from target.hive_target import HiveTarget
from target.file_target import FileTarget
from target.rdbms_target import RDBMSTarget
from target.nosql_target import NoSQLTarget
from target.vectordb_target import VectorDBTarget

class TargetFactory:
    @staticmethod
    def get_target(target_type: str, config: dict):
        if target_type == "hive":
            return HiveTarget(config)
        elif target_type == "file":
            return FileTarget(config)
        elif target_type == "rdbms":
            return RDBMSTarget(config)
        elif target_type == "nosql":
            return NoSQLTarget(config)
        elif target_type == "vectordb":
            return VectorDBTarget(config)
        else:
            raise ValueError(f"Unknown target type: {target_type}")
```

---

## 7. Logging Module

We implement a logging module using the **Singleton** pattern.
  
### logging_module/logger.py
```python
# logging_module/logger.py
import logging

class ETLLogger:
    __instance = None

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super(ETLLogger, cls).__new__(cls)
            cls.__instance._initialize_logger()
        return cls.__instance

    def _initialize_logger(self):
        self.logger = logging.getLogger("NLP_ETL_Pipeline")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def get_logger(self):
        return self.logger
```

---

## 8. Exception Handling Module

A simple custom exception class for ETL errors.

### exception/etl_exception.py
```python
# exception/etl_exception.py
class ETLException(Exception):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors
```

---

## 9. Main Pipeline Module

The main driver demonstrates how you can call one or multiple modules. It uses the **Factory** pattern (for sources and targets) and **Strategy** pattern (for choosing the chunker and embedding method) to let you plug and play various steps without modifying existing code.

### main.py
```python
# main.py
import sys
from pyspark.sql import SparkSession

# Configuration
from config.config_manager import ConfigManager

# Data source factory
from datasource.datasource_factory import DataSourceFactory

# Preprocessing functions
from preprocessing.html_parser import parse_html
from preprocessing.data_cleaner import clean_text

# Chunking strategies
from chunking.simple_chunker import SimpleChunker
from chunking.advanced_chunker import AdvancedChunker

# Embedding strategies
from embedding.tfidf_embedding import TFIDFEmbedding
from embedding.sentence_embedding import SentenceEmbedding

# Target factory
from target.target_factory import TargetFactory

# Logging
from logging_module.logger import ETLLogger
# Exception
from exception.etl_exception import ETLException

def main():
    logger = ETLLogger().get_logger()
    try:
        # Initialize Spark session (for a real pipeline)
        spark = SparkSession.builder \
            .appName("NLP_ETL_Pipeline") \
            .master("local[*]") \
            .getOrCreate()
        logger.info("Spark session created.")

        # Build configuration using the builder pattern in ConfigManager
        config_manager = ConfigManager()
        config = config_manager.build_config(['common', 'file', 'hive', 'rdbms', 'nosql', 'vectordb'])

        # Data Source: Example reading from a file source
        datasource = DataSourceFactory.get_datasource("file", config)
        raw_data = datasource.read_data()

        # For demonstration, assume we obtain HTML content as a string
        html_content = "<html><body><p>This is a sample document. It is meant to test the pipeline.</p></body></html>"

        # Preprocessing
        text_data = parse_html(html_content)
        text_data = clean_text(text_data)

        # Chunking: use a strategy pattern to choose the chunking method
        # You can select between SimpleChunker or AdvancedChunker without modifying this code.
        chunker_strategy = SimpleChunker(text_data)
        chunks = chunker_strategy.chunk()
        logger.info(f"Chunks created: {chunks}")

        # Embedding: choose an embedding strategy
        # Using TF-IDF embedding as an example.
        embedding_strategy = TFIDFEmbedding(chunks)
        embeddings = embedding_strategy.create_embedding()
        logger.info(f"Embeddings created: {embeddings}")

        # Target: write data (both structured data and embeddings)
        # Select target type dynamically.
        target = TargetFactory.get_target("hive", config)
        target.write_data(embeddings, mode="full")

        logger.info("ETL Pipeline execution completed successfully.")
        spark.stop()
    except Exception as e:
        logger.error("ETL Pipeline failed.", exc_info=e)
        raise ETLException("An error occurred in the ETL pipeline.", errors=e) from e

if __name__ == "__main__":
    main()
```

---

## 10. Test Cases Using pytest

Below are sample tests for each module. (In a real project you would add assertions comparing actual output – here we use simple prints.)

### tests/test_config.py
```python
# tests/test_config.py
import pytest
from config.config_manager import ConfigManager

def test_config_manager_singleton():
    cm1 = ConfigManager()
    cm2 = ConfigManager()
    assert cm1 is cm2

def test_build_config():
    cm = ConfigManager()
    config = cm.build_config(['common', 'file'])
    assert 'spark_app_name' in config
    assert 'input_path' in config
```

### tests/test_datasource.py
```python
# tests/test_datasource.py
import pytest
from datasource.datasource_factory import DataSourceFactory

def test_file_datasource():
    config = {"input_path": "/dummy/path", "file_format": "json"}
    ds = DataSourceFactory.get_datasource("file", config)
    data = ds.read_data()
    # Since read_data() is a dummy, test for None is acceptable
    assert data is None
```

### tests/test_preprocessing.py
```python
# tests/test_preprocessing.py
from preprocessing.html_parser import parse_html
from preprocessing.data_cleaner import clean_text

def test_parse_html():
    html = "<html><body><p>Hello</p></body></html>"
    text = parse_html(html)
    assert "Hello" in text

def test_clean_text():
    dirty = "This   is    a test\n"
    cleaned = clean_text(dirty)
    assert cleaned == "This is a test"
```

### tests/test_chunking.py
```python
# tests/test_chunking.py
from chunking.simple_chunker import SimpleChunker
from chunking.advanced_chunker import AdvancedChunker

def test_simple_chunker():
    text = "Sentence one. Sentence two. Sentence three."
    sc = SimpleChunker(text)
    chunks = sc.chunk()
    assert len(chunks) == 3

def test_advanced_chunker():
    text = "Word " * 50  # 50 words
    ac = AdvancedChunker(text)
    chunks = ac.chunk()
    # With a chunk size of 10, expect 5 chunks.
    assert len(chunks) == 5
```

### tests/test_embedding.py
```python
# tests/test_embedding.py
from embedding.tfidf_embedding import TFIDFEmbedding
from embedding.sentence_embedding import SentenceEmbedding

def test_tfidf_embedding():
    chunks = ["sample sentence one", "sample sentence two"]
    emb = TFIDFEmbedding(chunks)
    embedding = emb.create_embedding()
    assert "embedding" in embedding

def test_sentence_embedding():
    chunks = ["sample sentence one", "sample sentence two"]
    emb = SentenceEmbedding(chunks)
    embedding = emb.create_embedding()
    assert "embedding" in embedding
```

### tests/test_target.py
```python
# tests/test_target.py
from target.target_factory import TargetFactory

def test_file_target():
    config = {"output_path": "/dummy/path", "file_format": "json"}
    target = TargetFactory.get_target("file", config)
    # For a dummy data structure, pass a dict
    target.write_data({"data": "dummy"}, mode="full")
    # If no exception is raised, the test passes.
```

---

## 11. Requirements File

### requirements.txt
```
pyspark
beautifulsoup4
pytest
```

Add any additional packages you might require (e.g., connectors for MongoDB, JDBC drivers, etc.) as needed.

---

## 12. README File

### README.md
```markdown
# NLP ETL Pipeline using PySpark

This project is an end-to-end NLP ETL pipeline implemented in PySpark. It shows how to build a modular pipeline using design patterns such as **Singleton**, **Factory**, **Strategy**, **Template Method**, and **Builder**.

## Project Structure

```
nlp_etl_pipeline/
├── config
│   ├── __init__.py
│   ├── common_config.py         # Common configurations.
│   ├── file_config.py           # Configurations for file data sources/targets.
│   ├── hive_config.py           # Configurations for Hive.
│   ├── nosql_config.py          # Configurations for NoSQL systems like MongoDB.
│   ├── rdbms_config.py          # Configurations for RDBMS sources/targets.
│   ├── vectordb_config.py       # Configurations for vector database systems.
│   └── config_manager.py        # Singleton and Builder pattern for configuration management.
├── datasource
│   ├── __init__.py
│   ├── base_datasource.py       # Abstract class for all data sources.
│   ├── datasource_factory.py    # Factory for datasource objects.
│   ├── file_datasource.py       # File source implementation.
│   ├── hive_datasource.py       # Hive data source implementation.
│   ├── nosql_datasource.py      # NoSQL source implementation.
│   ├── rdbms_datasource.py      # RDBMS source implementation.
│   └── vectordb_datasource.py   # Vector database source implementation.
├── preprocessing
│   ├── __init__.py
│   ├── html_parser.py           # HTML parsing using BeautifulSoup.
│   └── data_cleaner.py          # Text cleaning routines.
├── chunking
│   ├── __init__.py
│   ├── base_chunker.py          # Template method for chunking.
│   ├── simple_chunker.py        # Simple chunking strategy.
│   ├── advanced_chunker.py      # Advanced chunking strategy.
│   └── chunk_smoothing.py       # Additional smoothing routines.
├── embedding
│   ├── __init__.py
│   ├── base_embedding.py        # Abstract class for embedding strategies.
│   ├── tfidf_embedding.py       # TF-IDF based embedding creation.
│   └── sentence_embedding.py    # Sentence embedding creation.
├── target
│   ├── __init__.py
│   ├── base_target.py           # Abstract target writer.
│   ├── file_target.py           # File target implementation.
│   ├── hive_target.py           # Hive target implementation.
│   ├── nosql_target.py          # NoSQL target implementation.
│   ├── rdbms_target.py          # RDBMS target implementation.
│   ├── vectordb_target.py       # Vector database target implementation.
│   └── target_factory.py        # Factory for target objects.
├── logging_module
│   ├── __init__.py
│   └── logger.py                # Singleton logging implementation.
├── exception
│   ├── __init__.py
│   └── etl_exception.py         # Custom exception for the ETL pipeline.
├── tests
│   ├── __init__.py
│   ├── test_config.py           # Pytest for config management.
│   ├── test_datasource.py       # Pytest for data source modules.
│   ├── test_preprocessing.py    # Pytest for preprocessing functions.
│   ├── test_chunking.py         # Pytest for chunking strategies.
│   ├── test_embedding.py        # Pytest for embedding strategies.
│   └── test_target.py           # Pytest for target modules.
├── main.py                      # Main driver for the pipeline.
├── requirements.txt             # Python package dependencies.
└── README.md                    # Project documentation.
```

## How to Run

1. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the Pipeline:**
   ```bash
   python main.py
   ```

3. **Run Tests:**
   ```bash
   pytest
   ```

## Integration Overview

- **Configuration:** Uses a singleton `ConfigManager` combined with a builder pattern to merge different source/target configurations.
- **Data Sources/Targets:** Use factories (`DataSourceFactory` and `TargetFactory`) to instantiate the appropriate classes based on configuration.
- **Preprocessing, Chunking, and Embedding:** All modules use the strategy or template method patterns to allow easy interchangeability of logic.
- **Logging and Exception Handling:** Logging is provided by a singleton logger. Exceptions use a custom exception class.
- **Modularity:** The entire pipeline is modular so that you can run specific modules (e.g., only data extraction or only embedding) without modifying existing code.

Feel free to extend this pipeline by implementing real data reads/writes and more sophisticated processing logic as needed.
```

---

## Summary

This complete code example demonstrates how to implement a modular NLP ETL pipeline using PySpark with the use of **Singleton**, **Factory**, **Strategy**, **Template Method**, and **Builder** design patterns. Each module (configuration, data source, preprocessing, chunking, embedding, target writing, logging, and exception handling) is organized in its own package with a clearly defined API. The accompanying test cases using pytest help ensure the functionality of each module. You can run the pipeline via the main driver and execute tests to validate the implementation.