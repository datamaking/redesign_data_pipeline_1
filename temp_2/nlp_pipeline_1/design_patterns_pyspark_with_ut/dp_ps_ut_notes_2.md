Given the complexity and space constraints, I'll provide a concise implementation with key code structures. Let's create the project step-by-step:

1. Project Structure:
```
nlp_etl/
├── config/
│   ├── common.yaml
│   ├── sources/
│   │   ├── hive.yaml
│   │   ├── file.yaml
│   │   └── rdbms.yaml
│   └── targets/
│       ├── vector_db.yaml
│       └── hive.yaml
├── src/
│   ├── config_manager.py
│   ├── data_source/
│   │   ├── __init__.py
│   │   └── source_factory.py
│   ├── preprocessing/
│   │   ├── strategies.py
│   │   └── preprocessor.py
│   ├── chunking/
│   │   ├── strategies.py
│   │   └── chunk_manager.py
│   ├── embedding/
│   │   ├── strategies.py
│   │   └── embedder.py
│   ├── target/
│   │   ├── target_factory.py
│   │   └── writers.py
│   ├── pipeline/
│   │   └── pipeline_builder.py
│   ├── utils/
│   │   ├── logger.py
│   │   └── exceptions.py
├── tests/
│   ├── test_data_source.py
│   └── test_preprocessing.py
├── requirements.txt
└── README.md
```

2. Configuration Manager (Singleton):
```python
# src/config_manager.py
import yaml
from pathlib import Path

class ConfigurationManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.load_configs()
        return cls._instance

    def load_configs(self):
        self.config = {}
        # Load common config
        common_path = Path("config/common.yaml")
        self.config.update(yaml.safe_load(common_path.read_text()))
        
        # Load source configs
        for src in Path("config/sources").glob("*.yaml"):
            self.config[f"source_{src.stem}"] = yaml.safe_load(src.read_text())
        
        # Load target configs
        for tgt in Path("config/targets").glob("*.yaml"):
            self.config[f"target_{tgt.stem}"] = yaml.safe_load(tgt.read_text())

    def get_config(self, key):
        return self.config.get(key)
```

3. Data Source Factory (Factory Pattern):
```python
# src/data_source/source_factory.py
from pyspark.sql import SparkSession

class DataSourceFactory:
    @staticmethod
    def create_source(source_type):
        if source_type == "hive":
            return HiveSource()
        elif source_type == "file":
            return FileSource()
        # Add other sources
        raise ValueError(f"Unknown source type: {source_type}")

class HiveSource:
    def read(self, config):
        return SparkSession.getActiveSession().sql(config["query"])

class FileSource:
    def read(self, config):
        return SparkSession.getActiveSession().read.format(config["format"]).load(config["path"])
```

4. Preprocessing Module (Strategy Pattern):
```python
# src/preprocessing/strategies.py
from bs4 import BeautifulSoup

class PreprocessingStrategy:
    def process(self, data):
        raise NotImplementedError

class HTMLParsingStrategy(PreprocessingStrategy):
    def process(self, data):
        return data.rdd.map(lambda x: BeautifulSoup(x[0], "html.parser").get_text())

class DataCleaningStrategy(PreprocessingStrategy):
    def process(self, data):
        # Implement cleaning logic
        return data
```

5. Pipeline Builder (Builder Pattern):
```python
# src/pipeline/pipeline_builder.py
class PipelineBuilder:
    def __init__(self):
        self.pipeline = Pipeline()

    def add_source(self, source):
        self.pipeline.sources.append(source)
        return self

    def add_preprocessor(self, preprocessor):
        self.pipeline.preprocessors.append(preprocessor)
        return self

    def build(self):
        return self.pipeline

class Pipeline:
    def __init__(self):
        self.sources = []
        self.preprocessors = []
        
    def execute(self):
        data = None
        for source in self.sources:
            data = source.read()
            for processor in self.preprocessors:
                data = processor.process(data)
        return data
```

6. Logger (Singleton):
```python
# src/utils/logger.py
import logging

class Logger:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            logging.basicConfig(level=logging.INFO)
            cls._instance.logger = logging.getLogger("ETL")
        return cls._instance
```

7. Test Case Example:
```python
# tests/test_data_source.py
import pytest
from src.data_source.source_factory import DataSourceFactory

def test_hive_source():
    source = DataSourceFactory.create_source("hive")
    assert isinstance(source, HiveSource)
```

8. requirements.txt:
```
pyspark>=3.3.0
pyyaml>=6.0
beautifulsoup4>=4.11.1
pytest>=7.1.2
sentence-transformers>=2.2.2
```

9. README.md (excerpt):
```
## Project Structure
- config/: Contains YAML configuration files
- src/: Main source code
- tests/: Unit tests

## Execution Steps
1. Configure your data sources/targets in YAML files
2. Build pipeline:
```python
pipeline = (PipelineBuilder()
            .add_source(hive_source)
            .add_preprocessor(html_parser)
            .build())
pipeline.execute()
```

This implementation uses:
- Singleton: ConfigurationManager, Logger
- Factory: DataSourceFactory, TargetFactory
- Strategy: Preprocessing, Chunking, Embedding
- Template Method: Pipeline execution
- Builder: Pipeline construction

Would you like me to elaborate on any specific module or provide additional implementation details?