# PySpark NLP Data Pipeline

A comprehensive NLP data pipeline built with PySpark that supports multiple data sources, preprocessing strategies, chunking methods, vector embeddings, and data targets.

## Project Structure

```
pyspark-nlp-pipeline/
├── config/
│   ├── __init__.py
│   ├── base_config.py
│   ├── source_configs/
│   │   ├── __init__.py
│   │   ├── hive_config.py
│   │   ├── text_config.py
│   │   ├── html_config.py
│   │   ├── rdbms_config.py
│   │   ├── mongodb_config.py
│   │   ├── chromadb_config.py
│   │   ├── postgres_config.py
│   │   └── neo4j_config.py
│   └── target_configs/
│       ├── __init__.py
│       ├── hive_config.py
│       ├── file_config.py
│       ├── rdbms_config.py
│       ├── mongodb_config.py
│       ├── chromadb_config.py
│       ├── postgres_config.py
│       └── neo4j_config.py
├── src/
│   ├── __init__.py
│   ├── data_source/
│   │   ├── __init__.py
│   │   ├── base_source.py
│   │   ├── hive_source.py
│   │   ├── text_source.py
│   │   ├── html_source.py
│   │   ├── rdbms_source.py
│   │   ├── mongodb_source.py
│   │   ├── chromadb_source.py
│   │   ├── postgres_source.py
│   │   └── neo4j_source.py
│   ├── preprocessing/
│   │   ├── __init__.py
│   │   ├── preprocessor.py
│   │   ├── text_cleaner.py
│   │   ├── tokenizer.py
│   │   └── normalizer.py
│   ├── chunking/
│   │   ├── __init__.py
│   │   ├── base_chunker.py
│   │   ├── fixed_size_chunker.py
│   │   ├── sentence_chunker.py
│   │   ├── paragraph_chunker.py
│   │   └── semantic_chunker.py
│   ├── embedding/
│   │   ├── __init__.py
│   │   ├── base_embedder.py
│   │   ├── spark_embedder.py
│   │   └── huggingface_embedder.py
│   ├── data_target/
│   │   ├── __init__.py
│   │   ├── base_target.py
│   │   ├── hive_target.py
│   │   ├── file_target.py
│   │   ├── rdbms_target.py
│   │   ├── mongodb_target.py
│   │   ├── chromadb_target.py
│   │   ├── postgres_target.py
│   │   └── neo4j_target.py
│   ├── vector_search/
│   │   ├── __init__.py
│   │   ├── base_search.py
│   │   ├── chromadb_search.py
│   │   ├── postgres_search.py
│   │   └── neo4j_search.py
│   └── utils/
│       ├── __init__.py
│       ├── logging_utils.py
│       └── exception_utils.py
├── main.py
├── requirements.txt
└── README.md
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/pyspark-nlp-pipeline.git
cd pyspark-nlp-pipeline