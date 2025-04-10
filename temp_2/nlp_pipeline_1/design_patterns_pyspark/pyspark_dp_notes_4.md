Below is a complete example of an NLP ETL data pipeline built in PySpark that incorporates the requested modules and uses only the following design patterns:

- **Singleton Pattern** – for the configuration manager and logger.
- **Factory Pattern** – for creating data source and target objects.
- **Strategy Pattern** – for plugging in different chunking and embedding strategies.
- **Template Method Pattern** – for defining common processing steps (in preprocessing and chunking).
- **Builder Pattern** – for assembling the configuration manager from multiple configuration files.

You can structure your project directory as follows:

```
nlp_etl_pipeline/
├── config/
│   ├── __init__.py
│   ├── common_config.py
│   ├── file_config.py
│   ├── hive_config.py
│   ├── nosql_config.py
│   ├── rdbms_config.py
│   ├── vectordb_config.py
│   ├── preprocessing_config.py
│   ├── chunking_config.py
│   ├── embedding_config.py
│   ├── target_config.py
│   └── config_manager.py
├── datasource/
│   ├── __init__.py
│   ├── datasource_factory.py
│   ├── hive_datasource.py
│   ├── file_datasource.py
│   ├── rdbms_datasource.py
│   ├── nosql_datasource.py
│   └── vectordb_datasource.py
├── preprocessing/
│   ├── __init__.py
│   ├── base_preprocessor.py
│   ├── html_parser.py
│   └── data_cleaner.py
├── chunking/
│   ├── __init__.py
│   ├── base_chunker.py
│   ├── simple_chunker.py
│   └── smoothing_chunker.py
├── embedding/
│   ├── __init__.py
│   ├── base_embedding.py
│   ├── tfidf_embedding.py
│   └── sentence_embedding.py
├── target/
│   ├── __init__.py
│   ├── target_factory.py
│   ├── hive_target.py
│   ├── file_target.py
│   ├── rdbms_target.py
│   ├── nosql_target.py
│   └── vectordb_target.py
├── utils/
│   ├── __init__.py
│   ├── logger.py
│   └── exceptions.py
├── main.py
├── requirements.txt
└── README.md
```

Below are the complete code listings for every file:

---

### **config/common_config.py**
```python
# config/common_config.py
common_config = {
    "app_name": "NLP_ETL_Pipeline",
    "spark_master": "local[*]"
}
```

---

### **config/file_config.py**
```python
# config/file_config.py
file_config = {
    "input_path": "data/input.txt",
    "output_path": "data/output.txt"
}
```

---

### **config/rdbms_config.py**
```python
# config/rdbms_config.py
rdbms_config = {
    "url": "jdbc:postgresql://localhost:5432/etl_db",
    "table": "source_table",
    "properties": {
        "user": "etl_user",
        "password": "password123"
    }
}
```

---

### **config/hive_config.py**
```python
# config/hive_config.py
hive_config = {
    "database": "default",
    "table": "source_hive_table"
}
```

---

### **config/nosql_config.py**
```python
# config/nosql_config.py
nosql_config = {
    "uri": "mongodb://localhost:27017",
    "database": "etl_db",
    "collection": "source_collection"
}
```

---

### **config/vectordb_config.py**
```python
# config/vectordb_config.py
vectordb_config = {
    "chromadb": {
        "host": "localhost",
        "port": 8000
    },
    "postgres_vector": {
        "url": "jdbc:postgresql://localhost:5432/vector_db",
        "table": "vector_table",
        "properties": {
            "user": "vector_user",
            "password": "vector_pass"
        }
    },
    "neo4j": {
        "uri": "bolt://localhost:7687",
        "user": "neo4j",
        "password": "neo_pass"
    }
}
```

---

### **config/preprocessing_config.py**
```python
# config/preprocessing_config.py
preprocessing_config = {
    "html_parser": {
        "parser": "html.parser"
    },
    "data_cleaner": {
        "remove_special_chars": True,
        "lower_case": True
    }
}
```

---

### **config/chunking_config.py**
```python
# config/chunking_config.py
chunking_config = {
    "chunk_size": 1000,
    "overlap": 100,
    "smoothing": True
}
```

---

### **config/embedding_config.py**
```python
# config/embedding_config.py
embedding_config = {
    "method": "tfidf",  # or "sentence"
    "tfidf": {
        "max_features": 5000
    },
    "sentence": {
        "model": "all-MiniLM-L6-v2"
    }
}
```

---

### **config/target_config.py**
```python
# config/target_config.py
target_config = {
    "file": {
        "output_path": "data/target_output.txt"
    },
    "hive": {
        "database": "default",
        "table": "target_hive_table"
    },
    "rdbms": {
        "url": "jdbc:postgresql://localhost:5432/etl_db",
        "table": "target_table",
        "properties": {
            "user": "etl_user",
            "password": "password123"
        }
    },
    "nosql": {
        "uri": "mongodb://localhost:27017",
        "database": "etl_db",
        "collection": "target_collection"
    },
    "vectordb": {
        "chromadb": {
            "host": "localhost",
            "port": 8000
        },
        "postgres_vector": {
            "url": "jdbc:postgresql://localhost:5432/vector_db",
            "table": "vector_table",
            "properties": {
                "user": "vector_user",
                "password": "vector_pass"
            }
        },
        "neo4j": {
            "uri": "bolt://localhost:7687",
            "user": "neo4j",
            "password": "neo_pass"
        }
    }
}
```

---

### **config/config_manager.py**  
*This module uses the Singleton and Builder patterns to combine all configurations.*
```python
# config/config_manager.py
from config.common_config import common_config
from config.file_config import file_config
from config.rdbms_config import rdbms_config
from config.hive_config import hive_config
from config.nosql_config import nosql_config
from config.vectordb_config import vectordb_config
from config.preprocessing_config import preprocessing_config
from config.chunking_config import chunking_config
from config.embedding_config import embedding_config
from config.target_config import target_config

class ConfigManager:
    __instance = None

    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super(ConfigManager, cls).__new__(cls)
        return cls.__instance

    def __init__(self, config):
        self.config = config

    def get(self, key, default=None):
        return self.config.get(key, default)

class ConfigManagerBuilder:
    def __init__(self):
        self._config = {}

    def add_common_config(self):
        self._config.update(common_config)
        return self

    def add_file_config(self):
        self._config["file"] = file_config
        return self

    def add_rdbms_config(self):
        self._config["rdbms"] = rdbms_config
        return self

    def add_hive_config(self):
        self._config["hive"] = hive_config
        return self

    def add_nosql_config(self):
        self._config["nosql"] = nosql_config
        return self

    def add_vectordb_config(self):
        self._config["vectordb"] = vectordb_config
        return self

    def add_preprocessing_config(self):
        self._config["preprocessing"] = preprocessing_config
        return self

    def add_chunking_config(self):
        self._config["chunking"] = chunking_config
        return self

    def add_embedding_config(self):
        self._config["embedding"] = embedding_config
        return self

    def add_target_config(self):
        self._config["target"] = target_config
        return self

    def build(self):
        return ConfigManager(self._config)

# Usage Example (can be done in main.py):
# builder = ConfigManagerBuilder()
# config_manager = builder.add_common_config().add_file_config().add_rdbms_config()...build()
```

---

### **datasource/datasource_factory.py**  
*The Data Source Factory uses the Factory pattern to instantiate different data source classes.*
```python
# datasource/datasource_factory.py
from datasource.hive_datasource import HiveDataSource
from datasource.file_datasource import FileDataSource
from datasource.rdbms_datasource import RDBMSDataSource
from datasource.nosql_datasource import NoSQLDataSource
from datasource.vectordb_datasource import VectorDBDataSource

class DataSourceFactory:
    @staticmethod
    def create_datasource(source_type, spark, config):
        if source_type == "hive":
            return HiveDataSource(spark, config.get("hive"))
        elif source_type == "file":
            return FileDataSource(spark, config.get("file"))
        elif source_type == "rdbms":
            return RDBMSDataSource(spark, config.get("rdbms"))
        elif source_type == "nosql":
            return NoSQLDataSource(spark, config.get("nosql"))
        elif source_type == "vectordb":
            return VectorDBDataSource(spark, config.get("vectordb"))
        else:
            raise ValueError(f"Unsupported data source type: {source_type}")
```

---

### **datasource/hive_datasource.py**
```python
# datasource/hive_datasource.py
class HiveDataSource:
    def __init__(self, spark, config):
        self.spark = spark
        self.database = config.get("database")
        self.table = config.get("table")

    def read_data(self):
        # Reading data from a Hive table using Spark SQL
        self.spark.sql(f"USE {self.database}")
        return self.spark.table(self.table)
```

---

### **datasource/file_datasource.py**
```python
# datasource/file_datasource.py
class FileDataSource:
    def __init__(self, spark, config):
        self.spark = spark
        self.input_path = config.get("input_path")

    def read_data(self):
        # Assumes a text file input, can be modified to read JSON/HTML etc.
        return self.spark.read.text(self.input_path)
```

---

### **datasource/rdbms_datasource.py**
```python
# datasource/rdbms_datasource.py
class RDBMSDataSource:
    def __init__(self, spark, config):
        self.spark = spark
        self.url = config.get("url")
        self.table = config.get("table")
        self.properties = config.get("properties")

    def read_data(self):
        return self.spark.read.jdbc(url=self.url, table=self.table, properties=self.properties)
```

---

### **datasource/nosql_datasource.py**
```python
# datasource/nosql_datasource.py
class NoSQLDataSource:
    def __init__(self, spark, config):
        self.spark = spark
        self.uri = config.get("uri")
        self.database = config.get("database")
        self.collection = config.get("collection")

    def read_data(self):
        # For simplicity, we assume the use of MongoDB Connector for Spark.
        read_config = {
            "uri": self.uri,
            "database": self.database,
            "collection": self.collection
        }
        return self.spark.read.format("mongo").options(**read_config).load()
```

---

### **datasource/vectordb_datasource.py**
```python
# datasource/vectordb_datasource.py
class VectorDBDataSource:
    def __init__(self, spark, config):
        self.spark = spark
        # For demonstration, we simply store the vectordb config.
        self.config = config

    def read_data(self):
        # Pseudocode: Depending on vector db type, use appropriate connector.
        # Here we simulate by returning an empty DataFrame.
        return self.spark.createDataFrame([], schema="id INT, vector ARRAY<DOUBLE>")
```

---

### **preprocessing/base_preprocessor.py**  
*The Template Method pattern is used here: define the skeleton and let subclasses override specific steps.*
```python
# preprocessing/base_preprocessor.py
from abc import ABC, abstractmethod

class BasePreprocessor(ABC):
    def process(self, data):
        data = self.extract(data)
        data = self.transform(data)
        return data

    @abstractmethod
    def extract(self, data):
        pass

    @abstractmethod
    def transform(self, data):
        pass
```

---

### **preprocessing/html_parser.py**
```python
# preprocessing/html_parser.py
from bs4 import BeautifulSoup
from preprocessing.base_preprocessor import BasePreprocessor

class HTMLParserPreprocessor(BasePreprocessor):
    def __init__(self, parser="html.parser"):
        self.parser = parser

    def extract(self, data):
        # In a real scenario, data would include HTML content.
        # Here we simply return the text content.
        return data

    def transform(self, data):
        # Using BeautifulSoup to parse HTML content.
        def parse_html(html):
            soup = BeautifulSoup(html, self.parser)
            return soup.get_text(separator=" ", strip=True)
        return data.rdd.map(lambda row: parse_html(row.value)).toDF(["text"])
```

---

### **preprocessing/data_cleaner.py**
```python
# preprocessing/data_cleaner.py
from pyspark.sql.functions import lower, regexp_replace
from preprocessing.base_preprocessor import BasePreprocessor

class DataCleanerPreprocessor(BasePreprocessor):
    def __init__(self, remove_special_chars=True, lower_case=True):
        self.remove_special_chars = remove_special_chars
        self.lower_case = lower_case

    def extract(self, data):
        return data

    def transform(self, data):
        df = data
        if self.lower_case:
            df = df.withColumn("text", lower(df["text"]))
        if self.remove_special_chars:
            # Remove any non-alphanumeric characters except spaces.
            df = df.withColumn("text", regexp_replace(df["text"], "[^a-zA-Z0-9 ]", ""))
        return df
```

---

### **chunking/base_chunker.py**  
*This is the abstract class for chunking, defining the template method.*
```python
# chunking/base_chunker.py
from abc import ABC, abstractmethod

class BaseChunker(ABC):
    def __init__(self, chunk_size, overlap):
        self.chunk_size = chunk_size
        self.overlap = overlap

    def chunk(self, text):
        # Template method defining the chunking process.
        chunks = self._do_chunk(text)
        return self._smooth_chunks(chunks)

    @abstractmethod
    def _do_chunk(self, text):
        # Subclasses will implement different chunking strategies.
        pass

    def _smooth_chunks(self, chunks):
        # Default implementation for chunk smoothing.
        return chunks
```

---

### **chunking/simple_chunker.py**  
*Implements a basic chunking strategy using the Strategy pattern.*
```python
# chunking/simple_chunker.py
from chunking.base_chunker import BaseChunker

class SimpleChunker(BaseChunker):
    def _do_chunk(self, text):
        # Simple strategy: split the text into chunks of fixed size.
        chunks = []
        start = 0
        text_length = len(text)
        while start < text_length:
            end = min(start + self.chunk_size, text_length)
            chunks.append(text[start:end])
            start += self.chunk_size
        return chunks
```

---

### **chunking/smoothing_chunker.py**  
*An alternative strategy that adds overlapping and smoothing.*
```python
# chunking/smoothing_chunker.py
from chunking.base_chunker import BaseChunker

class SmoothingChunker(BaseChunker):
    def _do_chunk(self, text):
        chunks = []
        start = 0
        text_length = len(text)
        while start < text_length:
            end = min(start + self.chunk_size, text_length)
            chunks.append(text[start:end])
            start += self.chunk_size - self.overlap  # move with overlap
        return chunks

    def _smooth_chunks(self, chunks):
        # Implement a simple smoothing technique.
        smoothed = []
        for i, chunk in enumerate(chunks):
            # For demonstration, just trim white spaces.
            smoothed.append(chunk.strip())
        return smoothed
```

---

### **embedding/base_embedding.py**  
*Abstract class to define the common interface for creating embeddings.*
```python
# embedding/base_embedding.py
from abc import ABC, abstractmethod

class BaseEmbedding(ABC):
    @abstractmethod
    def generate(self, text):
        pass
```

---

### **embedding/tfidf_embedding.py**
```python
# embedding/tfidf_embedding.py
from embedding.base_embedding import BaseEmbedding
from pyspark.ml.feature import HashingTF, IDF, Tokenizer

class TfIdfEmbedding(BaseEmbedding):
    def __init__(self, max_features=5000):
        self.max_features = max_features

    def generate(self, data):
        # 'data' is expected to be a Spark DataFrame with a column "text"
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        wordsData = tokenizer.transform(data)
        hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=self.max_features)
        featurizedData = hashingTF.transform(wordsData)
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(featurizedData)
        rescaledData = idfModel.transform(featurizedData)
        return rescaledData.select("features")
```

---

### **embedding/sentence_embedding.py**
```python
# embedding/sentence_embedding.py
from embedding.base_embedding import BaseEmbedding

class SentenceEmbedding(BaseEmbedding):
    def __init__(self, model="all-MiniLM-L6-v2"):
        # In a real implementation, load the sentence transformer model.
        self.model = model

    def generate(self, data):
        # For demonstration, we simulate sentence embedding creation.
        # In production, you might use a UDF that calls a sentence transformer.
        from pyspark.sql.functions import udf
        from pyspark.sql.types import ArrayType, FloatType

        def embed(text):
            # Simulate embedding as a list of floats
            return [0.1] * 768

        embed_udf = udf(embed, ArrayType(FloatType()))
        data = data.withColumn("features", embed_udf(data["text"]))
        return data.select("features")
```

---

### **target/target_factory.py**  
*The Target Factory uses the Factory pattern to instantiate target writers.*
```python
# target/target_factory.py
from target.hive_target import HiveTarget
from target.file_target import FileTarget
from target.rdbms_target import RDBMSTarget
from target.nosql_target import NoSQLTarget
from target.vectordb_target import VectorDBTarget

class TargetFactory:
    @staticmethod
    def create_target(target_type, spark, config):
        if target_type == "hive":
            return HiveTarget(spark, config.get("hive"))
        elif target_type == "file":
            return FileTarget(spark, config.get("file"))
        elif target_type == "rdbms":
            return RDBMSTarget(spark, config.get("rdbms"))
        elif target_type == "nosql":
            return NoSQLTarget(spark, config.get("nosql"))
        elif target_type == "vectordb":
            return VectorDBTarget(spark, config.get("vectordb"))
        else:
            raise ValueError(f"Unsupported target type: {target_type}")
```

---

### **target/hive_target.py**
```python
# target/hive_target.py
class HiveTarget:
    def __init__(self, spark, config):
        self.spark = spark
        self.database = config.get("database")
        self.table = config.get("table")

    def write_data(self, df):
        self.spark.sql(f"USE {self.database}")
        df.write.mode("overwrite").saveAsTable(self.table)
```

---

### **target/file_target.py**
```python
# target/file_target.py
class FileTarget:
    def __init__(self, spark, config):
        self.spark = spark
        self.output_path = config.get("output_path")

    def write_data(self, df):
        # Writing as text for demonstration; adjust format as needed.
        df.write.mode("overwrite").text(self.output_path)
```

---

### **target/rdbms_target.py**
```python
# target/rdbms_target.py
class RDBMSTarget:
    def __init__(self, spark, config):
        self.spark = spark
        self.url = config.get("url")
        self.table = config.get("table")
        self.properties = config.get("properties")

    def write_data(self, df):
        df.write.jdbc(url=self.url, table=self.table, mode="overwrite", properties=self.properties)
```

---

### **target/nosql_target.py**
```python
# target/nosql_target.py
class NoSQLTarget:
    def __init__(self, spark, config):
        self.spark = spark
        self.uri = config.get("uri")
        self.database = config.get("database")
        self.collection = config.get("collection")

    def write_data(self, df):
        write_config = {
            "uri": self.uri,
            "database": self.database,
            "collection": self.collection
        }
        df.write.format("mongo").mode("overwrite").options(**write_config).save()
```

---

### **target/vectordb_target.py**
```python
# target/vectordb_target.py
class VectorDBTarget:
    def __init__(self, spark, config):
        self.spark = spark
        # Store vectordb config; actual implementation would use specific connectors.
        self.config = config

    def write_data(self, df):
        # Pseudocode: Save vector embeddings to the vector database.
        # Here we simulate by simply showing the schema.
        df.printSchema()
```

---

### **utils/logger.py**  
*Singleton Logger implementation.*
```python
# utils/logger.py
import logging

class Logger:
    __instance = None

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super(Logger, cls).__new__(cls)
            cls.__instance._configure()
        return cls.__instance

    def _configure(self):
        self.logger = logging.getLogger("NLP_ETL_Pipeline")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)
```

---

### **utils/exceptions.py**  
*Custom exceptions for the pipeline.*
```python
# utils/exceptions.py

class PipelineException(Exception):
    pass

class ConfigurationException(PipelineException):
    pass

class DataSourceException(PipelineException):
    pass

class TargetException(PipelineException):
    pass
```

---

### **main.py**  
*This file ties together configuration building, data source reading, preprocessing, chunking, embedding, and writing to target. It makes use of all the design patterns specified.*
```python
# main.py
from pyspark.sql import SparkSession
from config.config_manager import ConfigManagerBuilder
from datasource.datasource_factory import DataSourceFactory
from preprocessing.html_parser import HTMLParserPreprocessor
from preprocessing.data_cleaner import DataCleanerPreprocessor
from chunking.simple_chunker import SimpleChunker
from chunking.smoothing_chunker import SmoothingChunker
from embedding.tfidf_embedding import TfIdfEmbedding
from embedding.sentence_embedding import SentenceEmbedding
from target.target_factory import TargetFactory
from utils.logger import Logger

def main():
    logger = Logger()
    logger.info("Starting NLP ETL Pipeline")

    # Build configuration using Builder Pattern & Singleton for config manager.
    config_manager = (ConfigManagerBuilder()
                      .add_common_config()
                      .add_file_config()
                      .add_rdbms_config()
                      .add_hive_config()
                      .add_nosql_config()
                      .add_vectordb_config()
                      .add_preprocessing_config()
                      .add_chunking_config()
                      .add_embedding_config()
                      .add_target_config()
                      .build())
    config = config_manager.config

    # Initialize Spark Session
    spark = SparkSession.builder.appName(config.get("app_name")) \
        .master(config.get("spark_master")).getOrCreate()
    logger.info("Spark session started.")

    try:
        # Create data source using Factory Pattern.
        source_type = "file"  # change as needed ("hive", "rdbms", "nosql", "vectordb")
        datasource = DataSourceFactory.create_datasource(source_type, spark, config)
        df = datasource.read_data()
        logger.info(f"Data read successfully from {source_type}")

        # Preprocessing: using Template Method Pattern.
        # First, HTML parsing (if needed).
        html_parser_config = config.get("preprocessing", {}).get("html_parser", {})
        html_processor = HTMLParserPreprocessor(parser=html_parser_config.get("parser", "html.parser"))
        df = html_processor.process(df)
        logger.info("HTML parsing completed.")

        # Data Cleaning
        cleaner_config = config.get("preprocessing", {}).get("data_cleaner", {})
        data_cleaner = DataCleanerPreprocessor(
            remove_special_chars=cleaner_config.get("remove_special_chars", True),
            lower_case=cleaner_config.get("lower_case", True)
        )
        df = data_cleaner.process(df)
        logger.info("Data cleaning completed.")

        # Chunking: Using Strategy Pattern.
        chunk_config = config.get("chunking", {})
        # Choose chunker based on config (for example, if smoothing is enabled)
        if chunk_config.get("smoothing", False):
            chunker = SmoothingChunker(chunk_size=chunk_config.get("chunk_size", 1000),
                                       overlap=chunk_config.get("overlap", 100))
        else:
            chunker = SimpleChunker(chunk_size=chunk_config.get("chunk_size", 1000),
                                    overlap=chunk_config.get("overlap", 0))
        # For demonstration, collect a text column and process chunking.
        text_data = df.collect()
        if text_data:
            first_row = text_data[0]
            text = first_row["text"]
            chunks = chunker.chunk(text)
            logger.info(f"Chunking produced {len(chunks)} chunks.")
        else:
            chunks = []
            logger.info("No data available for chunking.")

        # Embedding: using Strategy Pattern.
        embed_config = config.get("embedding", {})
        method = embed_config.get("method", "tfidf")
        if method == "tfidf":
            tfidf_config = embed_config.get("tfidf", {})
            embedder = TfIdfEmbedding(max_features=tfidf_config.get("max_features", 5000))
        elif method == "sentence":
            sentence_config = embed_config.get("sentence", {})
            embedder = SentenceEmbedding(model=sentence_config.get("model", "all-MiniLM-L6-v2"))
        else:
            raise ValueError("Unsupported embedding method")
        df_embeddings = embedder.generate(df)
        logger.info("Embedding generation completed.")

        # Write to target: Using Factory Pattern.
        target_type = "file"  # change to "hive", "rdbms", "nosql", "vectordb" as needed
        target = TargetFactory.create_target(target_type, spark, config)
        target.write_data(df_embeddings)
        logger.info(f"Data written successfully to target: {target_type}")

    except Exception as e:
        logger.error(f"ETL Pipeline failed: {str(e)}")
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()
```

---

### **requirements.txt**  
*List all required Python packages.*
```txt
pyspark
beautifulsoup4
```

*Note:* Depending on your target data sources, you might need additional connectors (e.g., MongoDB Spark Connector, JDBC drivers).

---

### **README.md**  
*Step-by-step instructions and project structure overview.*
```markdown
# NLP ETL Pipeline with PySpark

## Project Overview
This project implements an NLP ETL data pipeline using PySpark. The pipeline includes modules for configuration, data source ingestion, preprocessing (HTML parsing and data cleaning), chunking, embedding generation (TF-IDF and sentence embeddings), and writing results to various target systems.

The code uses the following design patterns:
- **Singleton Pattern**: For configuration manager and logger.
- **Factory Pattern**: To instantiate data source and target modules.
- **Strategy Pattern**: To select different chunking and embedding strategies.
- **Template Method Pattern**: In the preprocessing module (defining a processing skeleton).
- **Builder Pattern**: To construct the configuration manager by aggregating various configurations.

## Project Structure
```
nlp_etl_pipeline/
├── config/
│   ├── common_config.py
│   ├── file_config.py
│   ├── hive_config.py
│   ├── nosql_config.py
│   ├── rdbms_config.py
│   ├── vectordb_config.py
│   ├── preprocessing_config.py
│   ├── chunking_config.py
│   ├── embedding_config.py
│   ├── target_config.py
│   └── config_manager.py
├── datasource/
│   ├── datasource_factory.py
│   ├── hive_datasource.py
│   ├── file_datasource.py
│   ├── rdbms_datasource.py
│   ├── nosql_datasource.py
│   └── vectordb_datasource.py
├── preprocessing/
│   ├── base_preprocessor.py
│   ├── html_parser.py
│   └── data_cleaner.py
├── chunking/
│   ├── base_chunker.py
│   ├── simple_chunker.py
│   └── smoothing_chunker.py
├── embedding/
│   ├── base_embedding.py
│   ├── tfidf_embedding.py
│   └── sentence_embedding.py
├── target/
│   ├── target_factory.py
│   ├── hive_target.py
│   ├── file_target.py
│   ├── rdbms_target.py
│   ├── nosql_target.py
│   └── vectordb_target.py
├── utils/
│   ├── logger.py
│   └── exceptions.py
├── main.py
├── requirements.txt
└── README.md
```

## Setup & Installation

1. **Clone the repository:**
   ```
   git clone https://github.com/your_username/nlp_etl_pipeline.git
   cd nlp_etl_pipeline
   ```

2. **Install required packages:**
   ```
   pip install -r requirements.txt
   ```

3. **Configure Data Sources and Targets:**

   Edit the files inside the `config/` folder to set your data source parameters (e.g., file paths, Hive table names, RDBMS connection details, etc.).

4. **Run the Pipeline:**
   ```
   python main.py
   ```

## Module Descriptions

- **Configuration Module:**  
  Combines common and system-specific configurations using a Builder (in `config_manager.py`) and ensures a single configuration instance with the Singleton Pattern.

- **Data Source Module:**  
  Reads data from various data stores (Hive, file, RDBMS, NoSQL, vector databases) using a Factory pattern (`datasource_factory.py`).

- **Preprocessing Module:**  
  Uses the Template Method pattern to define common preprocessing steps. Modules include HTML parsing (`html_parser.py`) and data cleaning (`data_cleaner.py`).

- **Chunking Module:**  
  Provides multiple chunking strategies (simple and smoothing) that employ the Strategy pattern (`simple_chunker.py` and `smoothing_chunker.py`).

- **Vector Embedding Module:**  
  Generates embeddings using either TF-IDF (`tfidf_embedding.py`) or sentence embeddings (`sentence_embedding.py`), selectable by configuration.

- **Target Module:**  
  Writes the processed data to various systems via a Factory pattern (`target_factory.py`).

- **Logging & Exception Handling Modules:**  
  Logging is handled via a Singleton Logger (`logger.py`), and custom exceptions are defined in (`exceptions.py`).

## Final Notes
This project uses only the following design patterns: Singleton, Factory, Strategy, Template Method, and Builder.

Feel free to extend the modules as needed for your specific NLP and data processing requirements.
```

---

This complete code provides an end-to-end PySpark NLP ETL pipeline following the design constraints you specified. Customize configurations and connector implementations as required by your actual data sources and targets.

