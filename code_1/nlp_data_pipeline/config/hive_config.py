"""
Hive-specific configuration settings for the NLP data pipeline.
"""
from typing import Dict, List, Optional

# Hive connection configuration
HIVE_HOST = "localhost"
HIVE_PORT = 10000
HIVE_USER = "hive"
HIVE_PASSWORD = ""  # Should be loaded from environment variables in production
HIVE_DATABASE = "default"
HIVE_AUTH_MECHANISM = "PLAIN"  # Options: PLAIN, KERBEROS, LDAP, CUSTOM

# Hive warehouse directory
HIVE_WAREHOUSE_DIR = "/user/hive/warehouse"

# Default Hive table format
DEFAULT_HIVE_FORMAT = "parquet"  # Options: parquet, orc, avro, text

# Hive query settings
HIVE_QUERY_TIMEOUT = 3600  # seconds
HIVE_FETCH_SIZE = 10000  # Number of rows to fetch at once

# Hive table partitioning strategy
DEFAULT_PARTITION_COLUMNS = ["year", "month", "day"]

# Hive compression settings
HIVE_COMPRESSION = "snappy"  # Options: snappy, gzip, none

# Hive table properties
DEFAULT_HIVE_TABLE_PROPERTIES: Dict[str, str] = {
    "parquet.compression": HIVE_COMPRESSION,
    "orc.compress": HIVE_COMPRESSION,
    "avro.output.codec": HIVE_COMPRESSION,
}

# Default Hive tables for NLP pipeline
NLP_SOURCE_TABLES: Dict[str, Dict[str, str]] = {
    "documents": {
        "database": HIVE_DATABASE,
        "table": "raw_documents",
        "text_column": "document_text",
        "id_column": "document_id",
        "metadata_columns": ["title", "author", "date", "source"]
    },
    "articles": {
        "database": HIVE_DATABASE,
        "table": "raw_articles",
        "text_column": "article_text",
        "id_column": "article_id",
        "metadata_columns": ["title", "author", "publication_date", "source"]
    }
}

# Default Hive tables for storing processed NLP data
NLP_TARGET_TABLES: Dict[str, Dict[str, str]] = {
    "document_chunks": {
        "database": HIVE_DATABASE,
        "table": "document_chunks",
        "text_column": "chunk_text",
        "id_column": "chunk_id",
        "document_id_column": "document_id",
        "metadata_columns": ["chunk_index", "chunk_size"]
    },
    "document_embeddings": {
        "database": HIVE_DATABASE,
        "table": "document_embeddings",
        "embedding_column": "embedding_vector",
        "id_column": "embedding_id",
        "document_id_column": "document_id",
        "chunk_id_column": "chunk_id",
        "metadata_columns": ["model_name", "embedding_dimension"]
    }
}

# Hive JDBC connection string template
def get_hive_jdbc_url(
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    auth_mechanism: Optional[str] = None
) -> str:
    """
    Generate a JDBC connection URL for Hive.
    
    Args:
        host: Hive server hostname
        port: Hive server port
        database: Hive database name
        auth_mechanism: Authentication mechanism
        
    Returns:
        JDBC connection URL string
    """
    host = host or HIVE_HOST
    port = port or HIVE_PORT
    database = database or HIVE_DATABASE
    auth_mechanism = auth_mechanism or HIVE_AUTH_MECHANISM
    
    return f"jdbc:hive2://{host}:{port}/{database};auth={auth_mechanism}"

# Hive SparkSession configuration
HIVE_SPARK_CONFIG: Dict[str, str] = {
    "spark.sql.warehouse.dir": HIVE_WAREHOUSE_DIR,
    "spark.sql.hive.metastore.version": "3.1.2",
    "spark.sql.hive.metastore.jars": "builtin",
    "spark.sql.catalogImplementation": "hive",
    "spark.hadoop.hive.metastore.uris": f"thrift://{HIVE_HOST}:{HIVE_PORT}",
}
