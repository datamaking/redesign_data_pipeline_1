"""
Vector database configuration settings for the NLP data pipeline.
This includes settings for ChromaDB, PostgreSQL vector, and Neo4j vector databases.
"""
from typing import Dict, List, Optional, Union
import os
from pathlib import Path

from nlp_data_pipeline.config.common_config import DATA_DIR, EMBEDDING_DIMENSION

# ChromaDB configuration
CHROMADB_PERSIST_DIRECTORY = str(DATA_DIR / "chromadb")
CHROMADB_HOST = os.environ.get("CHROMADB_HOST", "localhost")
CHROMADB_PORT = int(os.environ.get("CHROMADB_PORT", "8000"))
CHROMADB_HTTP_HOST = os.environ.get("CHROMADB_HTTP_HOST", "localhost")
CHROMADB_HTTP_PORT = int(os.environ.get("CHROMADB_HTTP_PORT", "8000"))

# ChromaDB client settings
CHROMADB_CLIENT_SETTINGS = {
    "chroma_db_impl": "duckdb+parquet",  # Options: duckdb+parquet, clickhouse
    "persist_directory": CHROMADB_PERSIST_DIRECTORY,
    "anonymized_telemetry": False,
}

# ChromaDB collection settings
CHROMADB_COLLECTION_SETTINGS = {
    "documents": {
        "name": "documents",
        "metadata": {"description": "Document collection"},
        "embedding_function": "default",  # Will be replaced with actual embedding function
        "distance_function": "cosine",  # Options: cosine, l2, ip (inner product)
    },
    "chunks": {
        "name": "document_chunks",
        "metadata": {"description": "Document chunks collection"},
        "embedding_function": "default",  # Will be replaced with actual embedding function
        "distance_function": "cosine",  # Options: cosine, l2, ip (inner product)
    }
}

# PostgreSQL vector database configuration
PG_VECTOR_HOST = os.environ.get("PG_VECTOR_HOST", "localhost")
PG_VECTOR_PORT = int(os.environ.get("PG_VECTOR_PORT", "5432"))
PG_VECTOR_USER = os.environ.get("PG_VECTOR_USER", "postgres")
PG_VECTOR_PASSWORD = os.environ.get("PG_VECTOR_PASSWORD", "")
PG_VECTOR_DATABASE = os.environ.get("PG_VECTOR_DATABASE", "vector_db")

# PostgreSQL vector connection string
def get_pg_vector_uri() -> str:
    """
    Generate a PostgreSQL connection URI for vector database.
    
    Returns:
        PostgreSQL connection URI string
    """
    return f"postgresql://{PG_VECTOR_USER}:{PG_VECTOR_PASSWORD}@{PG_VECTOR_HOST}:{PG_VECTOR_PORT}/{PG_VECTOR_DATABASE}"

# PostgreSQL vector extension settings
PG_VECTOR_EXTENSION = "pgvector"
PG_VECTOR_EXTENSION_SCHEMA = "public"

# PostgreSQL vector table schemas
PG_VECTOR_DOCUMENT_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    document_id VARCHAR(255) PRIMARY KEY,
    document_text TEXT NOT NULL,
    title VARCHAR(255),
    author VARCHAR(255),
    source VARCHAR(255),
    date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);
"""

PG_VECTOR_CHUNK_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS document_chunks (
    chunk_id VARCHAR(255) PRIMARY KEY,
    document_id VARCHAR(255) NOT NULL,
    chunk_text TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    chunk_size INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB,
    FOREIGN KEY (document_id) REFERENCES documents(document_id)
);
"""

PG_VECTOR_EMBEDDING_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS document_embeddings (
    embedding_id VARCHAR(255) PRIMARY KEY,
    document_id VARCHAR(255) NOT NULL,
    chunk_id VARCHAR(255) NOT NULL,
    embedding_vector VECTOR({dimension}),
    model_name VARCHAR(255) NOT NULL,
    embedding_dimension INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB,
    FOREIGN KEY (document_id) REFERENCES documents(document_id),
    FOREIGN KEY (chunk_id) REFERENCES document_chunks(chunk_id)
);
"""

# PostgreSQL vector index creation
PG_VECTOR_INDEX_CREATION = """
CREATE INDEX IF NOT EXISTS document_embeddings_vector_idx 
ON document_embeddings 
USING ivfflat (embedding_vector vector_cosine_ops)
WITH (lists = 100);
"""

# PostgreSQL vector search query template
PG_VECTOR_SEARCH_QUERY = """
SELECT 
    e.chunk_id,
    c.chunk_text,
    c.document_id,
    d.title,
    1 - (e.embedding_vector <=> %s) as similarity
FROM 
    document_embeddings e
JOIN 
    document_chunks c ON e.chunk_id = c.chunk_id
JOIN 
    documents d ON c.document_id = d.document_id
WHERE 
    e.model_name = %s
ORDER BY 
    e.embedding_vector <=> %s
LIMIT %s;
"""

# Neo4j vector database configuration
NEO4J_HOST = os.environ.get("NEO4J_HOST", "localhost")
NEO4J_PORT = int(os.environ.get("NEO4J_PORT", "7687"))
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "")
NEO4J_DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")

# Neo4j connection URI
def get_neo4j_uri() -> str:
    """
    Generate a Neo4j connection URI.
    
    Returns:
        Neo4j connection URI string
    """
    return f"bolt://{NEO4J_HOST}:{NEO4J_PORT}"

# Neo4j vector index settings
NEO4J_VECTOR_INDEX_SETTINGS = {
    "name": "document_embedding_vector_index",
    "node_label": "DocumentEmbedding",
    "property_key": "embedding_vector",
    "dimension": EMBEDDING_DIMENSION,
    "similarity_function": "cosine",  # Options: cosine, euclidean
    "index_config": {
        "type": "vector",
        "dimensions": EMBEDDING_DIMENSION,
        "similarity": "cosine"
    }
}

# Neo4j Cypher queries for schema creation
NEO4J_CREATE_CONSTRAINTS = [
    "CREATE CONSTRAINT document_id IF NOT EXISTS FOR (d:Document) REQUIRE d.document_id IS UNIQUE",
    "CREATE CONSTRAINT chunk_id IF NOT EXISTS FOR (c:DocumentChunk) REQUIRE c.chunk_id IS UNIQUE",
    "CREATE CONSTRAINT embedding_id IF NOT EXISTS FOR (e:DocumentEmbedding) REQUIRE e.embedding_id IS UNIQUE"
]

NEO4J_CREATE_VECTOR_INDEX = """
CALL db.index.vector.createNodeIndex(
    $index_name,
    $node_label,
    $property_key,
    $dimension,
    $similarity_function,
    $index_config
)
"""

# Neo4j Cypher queries for data operations
NEO4J_CREATE_DOCUMENT_NODE = """
CREATE (d:Document {
    document_id: $document_id,
    document_text: $document_text,
    title: $title,
    author: $author,
    source: $source,
    date: $date,
    created_at: datetime(),
    updated_at: datetime(),
    metadata: $metadata
})
RETURN d
"""

NEO4J_CREATE_CHUNK_NODE = """
MATCH (d:Document {document_id: $document_id})
CREATE (c:DocumentChunk {
    chunk_id: $chunk_id,
    chunk_text: $chunk_text,
    chunk_index: $chunk_index,
    chunk_size: $chunk_size,
    created_at: datetime(),
    metadata: $metadata
})-[:PART_OF]->(d)
RETURN c
"""

NEO4J_CREATE_EMBEDDING_NODE = """
MATCH (d:Document {document_id: $document_id})
MATCH (c:DocumentChunk {chunk_id: $chunk_id})
CREATE (e:DocumentEmbedding {
    embedding_id: $embedding_id,
    embedding_vector: $embedding_vector,
    model_name: $model_name,
    embedding_dimension: $embedding_dimension,
    created_at: datetime(),
    metadata: $metadata
})-[:EMBEDDING_OF]->(c)
RETURN e
"""

NEO4J_VECTOR_SEARCH_QUERY = """
CALL db.index.vector.queryNodes($index_name, $k, $embedding_vector)
YIELD node, score
MATCH (node)-[:EMBEDDING_OF]->(c:DocumentChunk)-[:PART_OF]->(d:Document)
RETURN 
    node.embedding_id AS embedding_id,
    c.chunk_id AS chunk_id,
    c.chunk_text AS chunk_text,
    d.document_id AS document_id,
    d.title AS title,
    score AS similarity
ORDER BY score DESC
LIMIT $limit
"""

# Vector database batch processing settings
VECTOR_DB_BATCH_SIZE = 100
MAX_VECTOR_DB_BATCH_SIZE = 1000

# Vector search settings
DEFAULT_TOP_K = 5
DEFAULT_SIMILARITY_THRESHOLD = 0.7

# Vector database connection pool settings
CONNECTION_POOL_MIN_SIZE = 1
CONNECTION_POOL_MAX_SIZE = 10
CONNECTION_POOL_MAX_IDLE_TIME = 300  # seconds

# Vector database query timeout settings
QUERY_TIMEOUT = 60  # seconds

# Vector database retry settings
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds
RETRY_BACKOFF = 2  # exponential backoff multiplier

# Vector database Spark configurations
PG_VECTOR_SPARK_CONFIG = {
    "spark.jars": "postgresql-42.3.1.jar",
    "spark.driver.extraClassPath": "postgresql-42.3.1.jar",
}

NEO4J_SPARK_CONFIG = {
    "spark.jars": "neo4j-connector-apache-spark_2.12-4.1.2_for_spark_3.jar",
    "spark.driver.extraClassPath": "neo4j-connector-apache-spark_2.12-4.1.2_for_spark_3.jar",
    "spark.neo4j.bolt.url": get_neo4j_uri(),
    "spark.neo4j.bolt.user": NEO4J_USER,
    "spark.neo4j.bolt.password": NEO4J_PASSWORD,
}
