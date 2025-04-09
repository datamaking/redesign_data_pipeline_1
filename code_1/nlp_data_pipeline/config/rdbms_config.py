"""
RDBMS-specific configuration settings for the NLP data pipeline.
This includes settings for various relational databases like MySQL, PostgreSQL, Oracle, etc.
"""
from typing import Dict, List, Optional, Union
import os

# Default connection timeouts (seconds)
CONNECTION_TIMEOUT = 30
QUERY_TIMEOUT = 300

# Connection pool settings
MIN_POOL_SIZE = 1
MAX_POOL_SIZE = 10
POOL_RECYCLE = 3600  # Recycle connections after 1 hour

# Batch processing settings
DEFAULT_BATCH_SIZE = 1000
MAX_BATCH_SIZE = 10000

# MySQL configuration
MYSQL_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "localhost"),
    "port": int(os.environ.get("MYSQL_PORT", "3306")),
    "user": os.environ.get("MYSQL_USER", "root"),
    "password": os.environ.get("MYSQL_PASSWORD", ""),
    "database": os.environ.get("MYSQL_DATABASE", "nlp_data"),
    "charset": "utf8mb4",
    "use_unicode": True,
    "connect_timeout": CONNECTION_TIMEOUT,
    "read_timeout": QUERY_TIMEOUT,
    "write_timeout": QUERY_TIMEOUT,
    "pool_size": MAX_POOL_SIZE,
    "pool_recycle": POOL_RECYCLE,
}

# PostgreSQL configuration
POSTGRESQL_CONFIG = {
    "host": os.environ.get("POSTGRESQL_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRESQL_PORT", "5432")),
    "user": os.environ.get("POSTGRESQL_USER", "postgres"),
    "password": os.environ.get("POSTGRESQL_PASSWORD", ""),
    "database": os.environ.get("POSTGRESQL_DATABASE", "nlp_data"),
    "connect_timeout": CONNECTION_TIMEOUT,
    "application_name": "nlp_data_pipeline",
    "client_encoding": "utf8",
    "pool_size": MAX_POOL_SIZE,
    "pool_recycle": POOL_RECYCLE,
}

# Oracle configuration
ORACLE_CONFIG = {
    "host": os.environ.get("ORACLE_HOST", "localhost"),
    "port": int(os.environ.get("ORACLE_PORT", "1521")),
    "user": os.environ.get("ORACLE_USER", "system"),
    "password": os.environ.get("ORACLE_PASSWORD", ""),
    "service_name": os.environ.get("ORACLE_SERVICE", "XEPDB1"),
    "encoding": "UTF-8",
    "nencoding": "UTF-8",
    "pool_size": MAX_POOL_SIZE,
    "pool_recycle": POOL_RECYCLE,
}

# SQL Server configuration
SQLSERVER_CONFIG = {
    "server": os.environ.get("SQLSERVER_HOST", "localhost"),
    "port": int(os.environ.get("SQLSERVER_PORT", "1433")),
    "user": os.environ.get("SQLSERVER_USER", "sa"),
    "password": os.environ.get("SQLSERVER_PASSWORD", ""),
    "database": os.environ.get("SQLSERVER_DATABASE", "nlp_data"),
    "driver": "{ODBC Driver 17 for SQL Server}",
    "timeout": QUERY_TIMEOUT,
    "pool_size": MAX_POOL_SIZE,
    "pool_recycle": POOL_RECYCLE,
}

# SQLite configuration
SQLITE_CONFIG = {
    "database": os.environ.get("SQLITE_DATABASE", "nlp_data.db"),
    "timeout": QUERY_TIMEOUT,
    "isolation_level": None,  # Autocommit mode
    "detect_types": 1,  # Parse declared types
    "check_same_thread": False,  # Allow multiple threads
}

# Default tables for NLP data
DEFAULT_DOCUMENT_TABLE = "documents"
DEFAULT_CHUNK_TABLE = "document_chunks"
DEFAULT_EMBEDDING_TABLE = "document_embeddings"
DEFAULT_METADATA_TABLE = "document_metadata"

# Table schemas
DOCUMENT_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {table_name} (
    document_id VARCHAR(255) PRIMARY KEY,
    document_text TEXT NOT NULL,
    title VARCHAR(255),
    author VARCHAR(255),
    source VARCHAR(255),
    date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSON
)
"""

CHUNK_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {table_name} (
    chunk_id VARCHAR(255) PRIMARY KEY,
    document_id VARCHAR(255) NOT NULL,
    chunk_text TEXT NOT NULL,
    chunk_index INT NOT NULL,
    chunk_size INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSON,
    FOREIGN KEY (document_id) REFERENCES {document_table}(document_id)
)
"""

EMBEDDING_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS {table_name} (
    embedding_id VARCHAR(255) PRIMARY KEY,
    document_id VARCHAR(255) NOT NULL,
    chunk_id VARCHAR(255) NOT NULL,
    embedding_vector BLOB NOT NULL,
    model_name VARCHAR(255) NOT NULL,
    embedding_dimension INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSON,
    FOREIGN KEY (document_id) REFERENCES {document_table}(document_id),
    FOREIGN KEY (chunk_id) REFERENCES {chunk_table}(chunk_id)
)
"""

# Function to get JDBC URL for different database types
def get_jdbc_url(db_type: str, config: Dict[str, Union[str, int]]) -> str:
    """
    Generate a JDBC URL for the specified database type.
    
    Args:
        db_type: Database type (mysql, postgresql, oracle, sqlserver)
        config: Database configuration dictionary
        
    Returns:
        JDBC URL string
    """
    if db_type.lower() == "mysql":
        return f"jdbc:mysql://{config['host']}:{config['port']}/{config['database']}"
    elif db_type.lower() == "postgresql":
        return f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database']}"
    elif db_type.lower() == "oracle":
        return f"jdbc:oracle:thin:@{config['host']}:{config['port']}/{config['service_name']}"
    elif db_type.lower() == "sqlserver":
        return f"jdbc:sqlserver://{config['server']}:{config['port']};databaseName={config['database']}"
    elif db_type.lower() == "sqlite":
        return f"jdbc:sqlite:{config['database']}"
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

# Function to get SQLAlchemy URL for different database types
def get_sqlalchemy_url(db_type: str, config: Dict[str, Union[str, int]]) -> str:
    """
    Generate a SQLAlchemy URL for the specified database type.
    
    Args:
        db_type: Database type (mysql, postgresql, oracle, sqlserver, sqlite)
        config: Database configuration dictionary
        
    Returns:
        SQLAlchemy URL string
    """
    if db_type.lower() == "mysql":
        return f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    elif db_type.lower() == "postgresql":
        return f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    elif db_type.lower() == "oracle":
        return f"oracle+cx_oracle://{config['user']}:{config['password']}@{config['host']}:{config['port']}/?service_name={config['service_name']}"
    elif db_type.lower() == "sqlserver":
        return f"mssql+pyodbc://{config['user']}:{config['password']}@{config['server']}:{config['port']}/{config['database']}?driver={config['driver']}"
    elif db_type.lower() == "sqlite":
        return f"sqlite:///{config['database']}"
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

# Database-specific Spark configurations
MYSQL_SPARK_CONFIG = {
    "spark.jars": "mysql-connector-java-8.0.28.jar",
    "spark.driver.extraClassPath": "mysql-connector-java-8.0.28.jar",
}

POSTGRESQL_SPARK_CONFIG = {
    "spark.jars": "postgresql-42.3.1.jar",
    "spark.driver.extraClassPath": "postgresql-42.3.1.jar",
}

ORACLE_SPARK_CONFIG = {
    "spark.jars": "ojdbc8-21.5.0.0.jar",
    "spark.driver.extraClassPath": "ojdbc8-21.5.0.0.jar",
}

SQLSERVER_SPARK_CONFIG = {
    "spark.jars": "mssql-jdbc-9.4.1.jre8.jar",
    "spark.driver.extraClassPath": "mssql-jdbc-9.4.1.jre8.jar",
}

# Get Spark configuration for a specific database type
def get_db_spark_config(db_type: str) -> Dict[str, str]:
    """
    Get Spark configuration for a specific database type.
    
    Args:
        db_type: Database type (mysql, postgresql, oracle, sqlserver)
        
    Returns:
        Dictionary of Spark configuration properties
    """
    if db_type.lower() == "mysql":
        return MYSQL_SPARK_CONFIG
    elif db_type.lower() == "postgresql":
        return POSTGRESQL_SPARK_CONFIG
    elif db_type.lower() == "oracle":
        return ORACLE_SPARK_CONFIG
    elif db_type.lower() == "sqlserver":
        return SQLSERVER_SPARK_CONFIG
    else:
        return {}
