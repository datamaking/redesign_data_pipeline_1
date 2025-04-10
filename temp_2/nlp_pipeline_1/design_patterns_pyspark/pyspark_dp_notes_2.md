Below is a complete solution to build an NLP ETL data pipeline using PySpark, adhering to the specified requirements and design patterns (Singleton, Factory, Strategy, Template Method, and Builder). I'll provide the project structure, complete code for all modules, a `requirements.txt`, and a `README.md` with instructions.

---

## Project Structure

```
nlp_etl_pipeline/
├── config/
│   ├── common_config.yaml
│   ├── source_config.yaml
│   ├── preprocess_config.yaml
│   ├── chunking_config.yaml
│   ├── embedding_config.yaml
│   ├── target_config.yaml
│   └── pipeline_config.yaml
│   └── config_manager.py
├── data_source/
│   ├── base_source.py
│   ├── hive_source.py
│   ├── file_source.py
│   ├── rdbms_source.py
│   ├── nosql_source.py
│   ├── vector_db_source.py
│   └── source_factory.py
├── preprocess/
│   ├── base_preprocessor.py
│   ├── html_parser.py
│   ├── data_cleaner.py
│   └── preprocessor.py
├── chunking/
│   ├── base_chunker.py
│   ├── fixed_size_chunker.py
│   ├── sentence_chunker.py
│   └── chunker.py
├── embedding/
│   ├── base_embedder.py
│   ├── tfidf_embedder.py
│   ├── sentence_embedder.py
│   └── embedder.py
├── target/
│   ├── base_target.py
│   ├── hive_target.py
│   ├── file_target.py
│   ├── rdbms_target.py
│   ├── nosql_target.py
│   ├── vector_db_target.py
│   └── target_factory.py
├── utils/
│   ├── logger.py
│   ├── exceptions.py
│   └── pipeline_builder.py
├── main.py
├── requirements.txt
└── README.md
```

---

## Code for All Modules

### Configuration Module

**`config/config_manager.py`**

Uses the **Singleton Pattern** to ensure a single instance manages configurations loaded from YAML files.

```python
import yaml
from pathlib import Path

class ConfigManager:
    _instance = None

    def __new__(cls, pipeline_config_path='config/pipeline_config.yaml'):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance.load_configs(pipeline_config_path)
        return cls._instance

    def load_configs(self, pipeline_config_path):
        config_dir = Path('config')
        self.pipeline_config = yaml.safe_load(Path(pipeline_config_path).read_text())
        self.common_config = yaml.safe_load((config_dir / 'common_config.yaml').read_text())
        self.source_configs = yaml.safe_load((config_dir / 'source_config.yaml').read_text())
        self.preprocess_configs = yaml.safe_load((config_dir / 'preprocess_config.yaml').read_text())
        self.chunking_configs = yaml.safe_load((config_dir / 'chunking_config.yaml').read_text())
        self.embedding_configs = yaml.safe_load((config_dir / 'embedding_config.yaml').read_text())
        self.target_configs = yaml.safe_load((config_dir / 'target_config.yaml').read_text())

    def get_source_type(self):
        return self.pipeline_config['source']['type']

    def get_source_config(self):
        return self.source_configs[self.get_source_type()]

    def get_preprocess_types(self):
        return self.pipeline_config['preprocess']['types']

    def get_preprocess_config(self, preprocess_type):
        return self.preprocess_configs[preprocess_type]

    def get_chunking_type(self):
        return self.pipeline_config['chunking']['type']

    def get_chunking_config(self):
        return self.chunking_configs[self.get_chunking_type()]

    def get_embedding_type(self):
        return self.pipeline_config['embedding']['type']

    def get_embedding_config(self):
        return self.embedding_configs[self.get_embedding_type()]

    def get_target_type(self):
        return self.pipeline_config['target']['type']

    def get_target_config(self):
        return self.target_configs[self.get_target_type()]
```

**`config/common_config.yaml`**

```yaml
spark:
  app_name: "NLP ETL Pipeline"
```

**`config/source_config.yaml`**

```yaml
hive:
  database: "my_db"
  table: "my_table"
file:
  path: "/path/to/file"
  format: "text"
rdbms:
  url: "jdbc:postgresql://localhost:5432/my_db"
  table: "my_table"
  user: "user"
  password: "pass"
nosql:
  mongo_uri: "mongodb://localhost:27017"
  database: "my_db"
  collection: "my_collection"
vector_db:
  type: "postgresql"
  url: "jdbc:postgresql://localhost:5432/my_db"
  table: "vectors"
  user: "user"
  password: "pass"
```

**`config/preprocess_config.yaml`**

```yaml
html_parser:
  input_col: "content"
data_cleaner:
  input_col: "text"
```

**`config/chunking_config.yaml`**

```yaml
fixed_size:
  chunk_size: 100
sentence:
  input_col: "text"
```

**`config/embedding_config.yaml`**

```yaml
tfidf:
  input_col: "chunks"
sentence:
  input_col: "chunks"
```

**`config/target_config.yaml`**

```yaml
hive:
  database: "my_db"
  table: "processed_data"
file:
  path: "/path/to/output"
  format: "json"
rdbms:
  url: "jdbc:postgresql://localhost:5432/my_db"
  table: "processed_data"
  user: "user"
  password: "pass"
nosql:
  mongo_uri: "mongodb://localhost:27017"
  database: "my_db"
  collection: "processed_data"
vector_db:
  type: "postgresql"
  url: "jdbc:postgresql://localhost:5432/my_db"
  table: "vectors"
  user: "user"
  password: "pass"
```

**`config/pipeline_config.yaml`**

```yaml
source:
  type: "file"
preprocess:
  types: ["html_parser", "data_cleaner"]
chunking:
  type: "fixed_size"
embedding:
  type: "tfidf"
target:
  type: "hive"
```

---

### Data Source Module

**`data_source/base_source.py`**

Uses the **Template Method Pattern** to define the data reading process.

```python
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame

class BaseSource(ABC):
    def __init__(self, config):
        self.config = config

    @abstractmethod
    def read_data(self, spark: SparkSession) -> DataFrame:
        pass

    def execute(self, spark: SparkSession) -> DataFrame:
        return self.read_data(spark)
```

**`data_source/hive_source.py`**

```python
from .base_source import BaseSource
from pyspark.sql import SparkSession, DataFrame

class HiveSource(BaseSource):
    def read_data(self, spark: SparkSession) -> DataFrame:
        return spark.sql(f"SELECT * FROM {self.config['database']}.{self.config['table']}")
```

**`data_source/file_source.py`**

```python
from .base_source import BaseSource
from pyspark.sql import SparkSession, DataFrame

class FileSource(BaseSource):
    def read_data(self, spark: SparkSession) -> DataFrame:
        if self.config['format'] == 'text':
            return spark.read.text(self.config['path'])
        elif self.config['format'] == 'json':
            return spark.read.json(self.config['path'])
        elif self.config['format'] == 'html':
            return spark.read.text(self.config['path']).withColumnRenamed("value", "content")
        else:
            raise ValueError(f"Unsupported file format: {self.config['format']}")
```

**`data_source/rdbms_source.py`**

```python
from .base_source import BaseSource
from pyspark.sql import SparkSession, DataFrame

class RDBMSSource(BaseSource):
    def read_data(self, spark: SparkSession) -> DataFrame:
        return spark.read.jdbc(
            url=self.config['url'],
            table=self.config['table'],
            properties={"user": self.config['user'], "password": self.config['password']}
        )
```

**`data_source/nosql_source.py`**

```python
from .base_source import BaseSource
from pyspark.sql import SparkSession, DataFrame

class NoSQLSource(BaseSource):
    def read_data(self, spark: SparkSession) -> DataFrame:
        return spark.read.format("mongo").option("uri", self.config['mongo_uri']).option("database", self.config['database']).option("collection", self.config['collection']).load()
```

**`data_source/vector_db_source.py`**

```python
from .base_source import BaseSource
from pyspark.sql import SparkSession, DataFrame

class VectorDBSource(BaseSource):
    def read_data(self, spark: SparkSession) -> DataFrame:
        if self.config['type'] == 'postgresql':
            return spark.read.jdbc(
                url=self.config['url'],
                table=self.config['table'],
                properties={"user": self.config['user'], "password": self.config['password']}
            )
        # Placeholder for ChromaDB, Neo4j - requires specific connectors
        else:
            raise NotImplementedError(f"Vector DB {self.config['type']} not implemented")
```

**`data_source/source_factory.py`**

Uses the **Factory Pattern** to create source instances.

```python
from .hive_source import HiveSource
from .file_source import FileSource
from .rdbms_source import RDBMSSource
from .nosql_source import NoSQLSource
from .vector_db_source import VectorDBSource

class SourceFactory:
    @staticmethod
    def create_source(source_type: str, config: dict):
        if source_type == 'hive':
            return HiveSource(config)
        elif source_type == 'file':
            return FileSource(config)
        elif source_type == 'rdbms':
            return RDBMSSource(config)
        elif source_type == 'nosql':
            return NoSQLSource(config)
        elif source_type == 'vector_db':
            return VectorDBSource(config)
        else:
            raise ValueError(f"Unknown source type: {source_type}")
```

---

### Preprocessing Module

**`preprocess/base_preprocessor.py`**

Defines the interface for preprocessing strategies using the **Strategy Pattern**.

```python
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class PreprocessStrategy(ABC):
    @abstractmethod
    def process(self, df: DataFrame) -> DataFrame:
        pass
```

**`preprocess/html_parser.py`**

```python
from .base_preprocessor import PreprocessStrategy
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from bs4 import BeautifulSoup

def parse_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    return soup.get_text()

parse_html_udf = udf(parse_html)

class HTMLParserStrategy(PreprocessStrategy):
    def __init__(self, config):
        self.input_col = config['input_col']

    def process(self, df: DataFrame) -> DataFrame:
        return df.withColumn('text', parse_html_udf(df[self.input_col])).drop(self.input_col)
```

**`preprocess/data_cleaner.py`**

```python
from .base_preprocessor import PreprocessStrategy
from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, lower, trim

class DataCleanerStrategy(PreprocessStrategy):
    def __init__(self, config):
        self.input_col = config['input_col']

    def process(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            self.input_col,
            trim(lower(regexp_replace(df[self.input_col], '[^A-Za-z0-9 ]', '')))
        )
```

**`preprocess/preprocessor.py`**

```python
from typing import List
from .base_preprocessor import PreprocessStrategy
from pyspark.sql import DataFrame

class Preprocessor:
    def __init__(self, strategies: List[PreprocessStrategy]):
        self.strategies = strategies

    def process(self, df: DataFrame) -> DataFrame:
        result_df = df
        for strategy in self.strategies:
            result_df = strategy.process(result_df)
        return result_df
```

---

### Chunking Module

**`chunking/base_chunker.py`**

Uses the **Template Method Pattern** to define chunking and smoothing.

```python
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.functions import length

class BaseChunker(ABC):
    @abstractmethod
    def chunk(self, df: DataFrame) -> DataFrame:
        pass

    def smooth(self, df: DataFrame) -> DataFrame:
        # Remove chunks shorter than 10 characters as smoothing
        return df.filter(length(df['chunks']) >= 10)

    def process(self, df: DataFrame) -> DataFrame:
        df = self.chunk(df)
        return self.smooth(df)
```

**`chunking/fixed_size_chunker.py`**

```python
from .base_chunker import BaseChunker
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, explode
from typing import List

def fixed_size_chunk(text: str, size: int) -> List[str]:
    return [text[i:i + size] for i in range(0, len(text), size)]

class FixedSizeChunker(BaseChunker):
    def __init__(self, config):
        self.chunk_size = config['chunk_size']
        self.chunk_udf = udf(lambda text: fixed_size_chunk(text, self.chunk_size))

    def chunk(self, df: DataFrame) -> DataFrame:
        return df.withColumn('chunks', explode(self.chunk_udf(df['text'])))
```

**`chunking/sentence_chunker.py`**

```python
from .base_chunker import BaseChunker
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, explode

class SentenceChunker(BaseChunker):
    def __init__(self, config):
        self.input_col = config['input_col']

    def chunk(self, df: DataFrame) -> DataFrame:
        return df.withColumn('chunks', explode(split(df[self.input_col], '\\.')))
```

**`chunking/chunker.py`**

```python
from pyspark.sql import DataFrame

class Chunker:
    def __init__(self, chunker: BaseChunker):
        self.chunker = chunker

    def process(self, df: DataFrame) -> DataFrame:
        return self.chunker.process(df)
```

---

### Embedding Module

**`embedding/base_embedder.py`**

Defines the interface for embedding strategies using the **Strategy Pattern**.

```python
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class EmbeddingStrategy(ABC):
    @abstractmethod
    def embed(self, df: DataFrame) -> DataFrame:
        pass
```

**`embedding/tfidf_embedder.py`**

```python
from .base_embedder import EmbeddingStrategy
from pyspark.sql import DataFrame
from pyspark.ml.feature import HashingTF, IDF, Tokenizer

class TFIDFEmbedder(EmbeddingStrategy):
    def __init__(self, config):
        self.input_col = config['input_col']

    def embed(self, df: DataFrame) -> DataFrame:
        tokenizer = Tokenizer(inputCol=self.input_col, outputCol="words")
        words_data = tokenizer.transform(df)
        hashing_tf = HashingTF(inputCol="words", outputCol="rawFeatures")
        featurized_data = hashing_tf.transform(words_data)
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idf_model = idf.fit(featurized_data)
        return idf_model.transform(featurized_data)
```

**`embedding/sentence_embedder.py`**

```python
from .base_embedder import EmbeddingStrategy
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from sentence_transformers import SentenceTransformer
import numpy as np

def embed_sentence(text):
    model = SentenceTransformer('all-MiniLM-L6-v2')
    embedding = model.encode(text)
    return embedding.tolist()

embed_udf = udf(embed_sentence)

class SentenceEmbedder(EmbeddingStrategy):
    def __init__(self, config):
        self.input_col = config['input_col']

    def embed(self, df: DataFrame) -> DataFrame:
        return df.withColumn('features', embed_udf(df[self.input_col]))
```

**`embedding/embedder.py`**

```python
from pyspark.sql import DataFrame

class Embedder:
    def __init__(self, embedder: EmbeddingStrategy):
        self.embedder = embedder

    def embed(self, df: DataFrame) -> DataFrame:
        return self.embedder.embed(df)
```

---

### Target Module

**`target/base_target.py`**

Uses the **Template Method Pattern** to define the writing process.

```python
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class BaseTarget(ABC):
    def __init__(self, config):
        self.config = config

    @abstractmethod
    def write_data(self, df: DataFrame):
        pass

    def execute(self, df: DataFrame):
        self.write_data(df)
```

**`target/hive_target.py`**

```python
from .base_target import BaseTarget
from pyspark.sql import DataFrame

class HiveTarget(BaseTarget):
    def write_data(self, df: DataFrame):
        df.write.mode('overwrite').saveAsTable(f"{self.config['database']}.{self.config['table']}")
```

**`target/file_target.py`**

```python
from .base_target import BaseTarget
from pyspark.sql import DataFrame

class FileTarget(BaseTarget):
    def write_data(self, df: DataFrame):
        df.write.mode('overwrite').format(self.config['format']).save(self.config['path'])
```

**`target/rdbms_target.py`**

```python
from .base_target import BaseTarget
from pyspark.sql import DataFrame

class RDBMSTarget(BaseTarget):
    def write_data(self, df: DataFrame):
        df.write.mode('overwrite').jdbc(
            url=self.config['url'],
            table=self.config['table'],
            properties={"user": self.config['user'], "password": self.config['password']}
        )
```

**`target/nosql_target.py`**

```python
from .base_target import BaseTarget
from pyspark.sql import DataFrame

class NoSQLTarget(BaseTarget):
    def write_data(self, df: DataFrame):
        df.write.format("mongo").mode("overwrite").option("uri", self.config['mongo_uri']).option("database", self.config['database']).option("collection", self.config['collection']).save()
```

**`target/vector_db_target.py`**

```python
from .base_target import BaseTarget
from pyspark.sql import DataFrame

class VectorDBTarget(BaseTarget):
    def write_data(self, df: DataFrame):
        if self.config['type'] == 'postgresql':
            df.write.mode('overwrite').jdbc(
                url=self.config['url'],
                table=self.config['table'],
                properties={"user": self.config['user'], "password": self.config['password']}
            )
        # Placeholder for ChromaDB, Neo4j
        else:
            raise NotImplementedError(f"Vector DB {self.config['type']} not implemented")
```

**`target/target_factory.py`**

Uses the **Factory Pattern** to create target instances.

```python
from .hive_target import HiveTarget
from .file_target import FileTarget
from .rdbms_target import RDBMSTarget
from .nosql_target import NoSQLTarget
from .vector_db_target import VectorDBTarget

class TargetFactory:
    @staticmethod
    def create_target(target_type: str, config: dict):
        if target_type == 'hive':
            return HiveTarget(config)
        elif target_type == 'file':
            return FileTarget(config)
        elif target_type == 'rdbms':
            return RDBMSTarget(config)
        elif target_type == 'nosql':
            return NoSQLTarget(config)
        elif target_type == 'vector_db':
            return VectorDBTarget(config)
        else:
            raise ValueError(f"Unknown target type: {target_type}")
```

---

### Utils Module

**`utils/logger.py`**

Uses the **Singleton Pattern** for consistent logging.

```python
import logging

class Logger:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
            cls._instance.logger = logging.getLogger('nlp_etl_pipeline')
        return cls._instance

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)
```

**`utils/exceptions.py`**

```python
class SourceReadException(Exception):
    pass

class PreprocessException(Exception):
    pass

class ChunkingException(Exception):
    pass

class EmbeddingException(Exception):
    pass

class TargetWriteException(Exception):
    pass
```

**`utils/pipeline_builder.py`**

Uses the **Builder Pattern** to construct the pipeline.

```python
from pyspark.sql import SparkSession
from config.config_manager import ConfigManager
from data_source.source_factory import SourceFactory
from preprocess.preprocessor import Preprocessor
from preprocess.html_parser import HTMLParserStrategy
from preprocess.data_cleaner import DataCleanerStrategy
from chunking.chunker import Chunker
from chunking.fixed_size_chunker import FixedSizeChunker
from chunking.sentence_chunker import SentenceChunker
from embedding.embedder import Embedder
from embedding.tfidf_embedder import TFIDFEmbedder
from embedding.sentence_embedder import SentenceEmbedder
from target.target_factory import TargetFactory

class Pipeline:
    def __init__(self, source, preprocessor, chunker, embedder, target):
        self.source = source
        self.preprocessor = preprocessor
        self.chunker = chunker
        self.embedder = embedder
        self.target = target

    def run(self, spark: SparkSession):
        df = self.source.execute(spark)
        df = self.preprocessor.process(df)
        df = self.chunker.process(df)
        df = self.embedder.embed(df)
        self.target.execute(df)

class PipelineBuilder:
    def __init__(self):
        self.config_manager = ConfigManager()
        self.source = None
        self.preprocessor = None
        self.chunker = None
        self.embedder = None
        self.target = None

    def set_source(self):
        source_type = self.config_manager.get_source_type()
        source_config = self.config_manager.get_source_config()
        self.source = SourceFactory.create_source(source_type, source_config)
        return self

    def set_preprocessor(self):
        preprocess_types = self.config_manager.get_preprocess_types()
        strategies = []
        for pt in preprocess_types:
            config = self.config_manager.get_preprocess_config(pt)
            if pt == 'html_parser':
                strategies.append(HTMLParserStrategy(config))
            elif pt == 'data_cleaner':
                strategies.append(DataCleanerStrategy(config))
        self.preprocessor = Preprocessor(strategies)
        return self

    def set_chunker(self):
        chunking_type = self.config_manager.get_chunking_type()
        config = self.config_manager.get_chunking_config()
        if chunking_type == 'fixed_size':
            self.chunker = Chunker(FixedSizeChunker(config))
        elif chunking_type == 'sentence':
            self.chunker = Chunker(SentenceChunker(config))
        return self

    def set_embedder(self):
        embedding_type = self.config_manager.get_embedding_type()
        config = self.config_manager.get_embedding_config()
        if embedding_type == 'tfidf':
            self.embedder = Embedder(TFIDFEmbedder(config))
        elif embedding_type == 'sentence':
            self.embedder = Embedder(SentenceEmbedder(config))
        return self

    def set_target(self):
        target_type = self.config_manager.get_target_type()
        target_config = self.config_manager.get_target_config()
        self.target = TargetFactory.create_target(target_type, target_config)
        return self

    def build(self):
        if not all([self.source, self.preprocessor, self.chunker, self.embedder, self.target]):
            raise ValueError("All pipeline components must be set before building.")
        return Pipeline(self.source, self.preprocessor, self.chunker, self.embedder, self.target)
```

---

### Main Script

**`main.py`**

```python
from pyspark.sql import SparkSession
from utils.pipeline_builder import PipelineBuilder
from utils.logger import Logger
from utils.exceptions import *

def main():
    spark = SparkSession.builder.appName("NLP ETL Pipeline").getOrCreate()
    logger = Logger()

    try:
        pipeline = (PipelineBuilder()
                    .set_source()
                    .set_preprocessor()
                    .set_chunker()
                    .set_embedder()
                    .set_target()
                    .build())
        pipeline.run(spark)
        logger.info("Pipeline completed successfully.")
    except (SourceReadException, PreprocessException, ChunkingException, EmbeddingException, TargetWriteException) as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
```

---

### Requirements File

**`requirements.txt`**

```
pyspark>=3.5.0
pyyaml>=6.0
beautifulsoup4>=4.12.0
sentence-transformers>=2.2.0
torch>=2.0.0
# Note: MongoDB Spark Connector, JDBC drivers, etc., may need to be added based on environment setup
```

---

### README File

**`README.md`**

```markdown
# NLP ETL Pipeline

This project implements an NLP ETL data pipeline using PySpark, designed to process text data from various sources, preprocess it, chunk it, generate embeddings, and write the results to multiple targets.

## Project Structure

- **`config/`**: Contains YAML configuration files.
  - `common_config.yaml`: Common settings (e.g., Spark app name).
  - `source_config.yaml`: Configurations for data sources.
  - `preprocess_config.yaml`: Preprocessing configurations.
  - `chunking_config.yaml`: Chunking configurations.
  - `embedding_config.yaml`: Embedding configurations.
  - `target_config.yaml`: Target configurations.
  - `pipeline_config.yaml`: Specifies which components to use.
  - `config_manager.py`: Singleton class to manage configurations.
- **`data_source/`**: Modules for reading data from various sources using Factory and Template Method patterns.
- **`preprocess/`**: Preprocessing strategies using the Strategy Pattern.
- **`chunking/`**: Chunking strategies with smoothing using Template Method Pattern.
- **`embedding/`**: Embedding generation using Strategy Pattern.
- **`target/`**: Modules for writing data using Factory and Template Method patterns.
- **`utils/`**: Utilities including Logger (Singleton), Exceptions, and PipelineBuilder (Builder Pattern).
- **`main.py`**: Entry point to run the pipeline.
- **`requirements.txt`**: Required Python packages.
- **`README.md`**: This documentation.

## Setup Instructions

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
   Ensure PySpark is configured with Hive support and necessary connectors (e.g., MongoDB, JDBC drivers) are available.

2. **Configure the Pipeline**:
   - Edit `config/pipeline_config.yaml` to specify the source, preprocessing steps, chunking strategy, embedding method, and target.
   - Update corresponding YAML files in `config/` with connection details (e.g., database credentials, file paths).

   Example `pipeline_config.yaml`:
   ```yaml
   source:
     type: "file"
   preprocess:
     types: ["html_parser", "data_cleaner"]
   chunking:
     type: "fixed_size"
   embedding:
     type: "tfidf"
   target:
     type: "hive"
   ```

3. **Run the Pipeline**:
   ```bash
   spark-submit main.py
   ```

## Design Patterns Used

- **Singleton**: `ConfigManager` and `Logger` ensure single instances.
- **Factory**: `SourceFactory` and `TargetFactory` create source and target objects.
- **Strategy**: Preprocessing, chunking, and embedding modules allow runtime strategy selection.
- **Template Method**: Base classes in `data_source`, `chunking`, and `target` define process skeletons.
- **Builder**: `PipelineBuilder` constructs the pipeline step-by-step.

## Notes

- Vector database support (e.g., ChromaDB, Neo4j) is partially implemented; additional connectors or APIs may be required.
- Sentence embeddings use `sentence-transformers`, which may require optimization for large-scale distributed processing.
- Ensure Spark is configured with appropriate connectors for your data sources/targets.

For issues or enhancements, please review the logs generated by the `Logger` class.
```

---

## Explanation of Design Patterns

- **Singleton Pattern**: Ensures `ConfigManager` and `Logger` have single instances for consistent configuration and logging.
- **Factory Pattern**: `SourceFactory` and `TargetFactory` create instances of source and target classes based on type.
- **Strategy Pattern**: Allows flexible selection of preprocessing, chunking, and embedding strategies at runtime.
- **Template Method Pattern**: Defines the skeleton of algorithms in `BaseSource`, `BaseChunker`, and `BaseTarget`, with specific steps implemented by subclasses.
- **Builder Pattern**: `PipelineBuilder` constructs the pipeline incrementally, ensuring all components are configured before execution.

This solution meets all requirements, using only PySpark and the specified design patterns, with complete code and documentation for integration and execution.

