"""
MongoDB-specific configuration settings for the NLP data pipeline.
"""
from typing import Dict, List, Optional, Union
import os

# MongoDB connection configuration
MONGODB_HOST = os.environ.get("MONGODB_HOST", "localhost")
MONGODB_PORT = int(os.environ.get("MONGODB_PORT", "27017"))
MONGODB_USERNAME = os.environ.get("MONGODB_USERNAME", "")
MONGODB_PASSWORD = os.environ.get("MONGODB_PASSWORD", "")
MONGODB_DATABASE = os.environ.get("MONGODB_DATABASE", "nlp_data")
MONGODB_AUTH_SOURCE = os.environ.get("MONGODB_AUTH_SOURCE", "admin")
MONGODB_AUTH_MECHANISM = os.environ.get("MONGODB_AUTH_MECHANISM", "SCRAM-SHA-256")

# MongoDB connection string
def get_mongodb_uri() -> str:
    """
    Generate a MongoDB connection URI.
    
    Returns:
        MongoDB connection URI string
    """
    if MONGODB_USERNAME and MONGODB_PASSWORD:
        return f"mongodb://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@{MONGODB_HOST}:{MONGODB_PORT}/{MONGODB_DATABASE}?authSource={MONGODB_AUTH_SOURCE}&authMechanism={MONGODB_AUTH_MECHANISM}"
    else:
        return f"mongodb://{MONGODB_HOST}:{MONGODB_PORT}/{MONGODB_DATABASE}"

# MongoDB connection options
MONGODB_CONNECTION_OPTIONS = {
    "connectTimeoutMS": 30000,
    "socketTimeoutMS": 300000,
    "serverSelectionTimeoutMS": 30000,
    "maxPoolSize": 100,
    "minPoolSize": 10,
    "maxIdleTimeMS": 600000,
    "retryWrites": True,
    "retryReads": True,
    "w": "majority",
    "readPreference": "primaryPreferred",
}

# Default MongoDB collections for NLP pipeline
DEFAULT_COLLECTIONS = {
    "documents": "documents",
    "chunks": "document_chunks",
    "embeddings": "document_embeddings",
    "metadata": "document_metadata",
    "vector_search": "vector_search_index",
}

# MongoDB document schemas (for validation)
DOCUMENT_SCHEMA = {
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["document_id", "document_text"],
            "properties": {
                "document_id": {
                    "bsonType": "string",
                    "description": "Unique identifier for the document"
                },
                "document_text": {
                    "bsonType": "string",
                    "description": "Text content of the document"
                },
                "title": {
                    "bsonType": ["string", "null"],
                    "description": "Document title"
                },
                "author": {
                    "bsonType": ["string", "null"],
                    "description": "Document author"
                },
                "source": {
                    "bsonType": ["string", "null"],
                    "description": "Document source"
                },
                "date": {
                    "bsonType": ["date", "null"],
                    "description": "Document date"
                },
                "created_at": {
                    "bsonType": "date",
                    "description": "Creation timestamp"
                },
                "updated_at": {
                    "bsonType": "date",
                    "description": "Last update timestamp"
                },
                "metadata": {
                    "bsonType": ["object", "null"],
                    "description": "Additional document metadata"
                }
            }
        }
    },
    "validationLevel": "moderate",
    "validationAction": "warn"
}

CHUNK_SCHEMA = {
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["chunk_id", "document_id", "chunk_text", "chunk_index"],
            "properties": {
                "chunk_id": {
                    "bsonType": "string",
                    "description": "Unique identifier for the chunk"
                },
                "document_id": {
                    "bsonType": "string",
                    "description": "Reference to the parent document"
                },
                "chunk_text": {
                    "bsonType": "string",
                    "description": "Text content of the chunk"
                },
                "chunk_index": {
                    "bsonType": "int",
                    "description": "Index of the chunk within the document"
                },
                "chunk_size": {
                    "bsonType": "int",
                    "description": "Size of the chunk in characters"
                },
                "created_at": {
                    "bsonType": "date",
                    "description": "Creation timestamp"
                },
                "metadata": {
                    "bsonType": ["object", "null"],
                    "description": "Additional chunk metadata"
                }
            }
        }
    },
    "validationLevel": "moderate",
    "validationAction": "warn"
}

EMBEDDING_SCHEMA = {
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["embedding_id", "document_id", "chunk_id", "embedding_vector", "model_name"],
            "properties": {
                "embedding_id": {
                    "bsonType": "string",
                    "description": "Unique identifier for the embedding"
                },
                "document_id": {
                    "bsonType": "string",
                    "description": "Reference to the parent document"
                },
                "chunk_id": {
                    "bsonType": "string",
                    "description": "Reference to the chunk"
                },
                "embedding_vector": {
                    "bsonType": "array",
                    "description": "Vector embedding of the chunk text"
                },
                "model_name": {
                    "bsonType": "string",
                    "description": "Name of the embedding model used"
                },
                "embedding_dimension": {
                    "bsonType": "int",
                    "description": "Dimension of the embedding vector"
                },
                "created_at": {
                    "bsonType": "date",
                    "description": "Creation timestamp"
                },
                "metadata": {
                    "bsonType": ["object", "null"],
                    "description": "Additional embedding metadata"
                }
            }
        }
    },
    "validationLevel": "moderate",
    "validationAction": "warn"
}

# MongoDB indexes
DOCUMENT_INDEXES = [
    {"key": {"document_id": 1}, "unique": True},
    {"key": {"title": 1}},
    {"key": {"author": 1}},
    {"key": {"source": 1}},
    {"key": {"date": 1}},
    {"key": {"created_at": 1}}
]

CHUNK_INDEXES = [
    {"key": {"chunk_id": 1}, "unique": True},
    {"key": {"document_id": 1}},
    {"key": {"document_id": 1, "chunk_index": 1}, "unique": True},
    {"key": {"created_at": 1}}
]

EMBEDDING_INDEXES = [
    {"key": {"embedding_id": 1}, "unique": True},
    {"key": {"document_id": 1}},
    {"key": {"chunk_id": 1}, "unique": True},
    {"key": {"model_name": 1}},
    {"key": {"created_at": 1}}
]

# Vector search configuration
VECTOR_SEARCH_CONFIG = {
    "index_type": "hnsw",  # Options: hnsw, flat, ivf
    "dimensions": 384,  # Should match the embedding dimension
    "metric": "cosine",  # Options: cosine, euclidean, dot
    "hnsw_config": {
        "m": 16,  # Number of connections per layer
        "ef_construction": 100,  # Size of the dynamic candidate list for constructing the graph
        "ef_search": 50  # Size of the dynamic candidate list for searching the graph
    },
    "ivf_config": {
        "nlist": 100,  # Number of clusters
        "nprobe": 10  # Number of clusters to search
    }
}

# MongoDB batch processing settings
BATCH_SIZE = 1000
MAX_BATCH_SIZE = 10000

# MongoDB aggregation pipeline settings
MAX_MEMORY_USAGE = 100 * 1024 * 1024  # 100MB
ALLOW_DISK_USE = True
MAX_TIME_MS = 300000  # 5 minutes

# MongoDB change stream settings
CHANGE_STREAM_RESUME_AFTER_SECONDS = 3600  # 1 hour
CHANGE_STREAM_FULL_DOCUMENT = "updateLookup"  # Options: default, updateLookup

# Function to create MongoDB indexes
def get_collection_indexes(collection_name: str) -> List[Dict]:
    """
    Get the indexes for a specific collection.
    
    Args:
        collection_name: Name of the collection
        
    Returns:
        List of index specifications
    """
    if collection_name == DEFAULT_COLLECTIONS["documents"]:
        return DOCUMENT_INDEXES
    elif collection_name == DEFAULT_COLLECTIONS["chunks"]:
        return CHUNK_INDEXES
    elif collection_name == DEFAULT_COLLECTIONS["embeddings"]:
        return EMBEDDING_INDEXES
    else:
        return []

# Function to create MongoDB collection schema
def get_collection_schema(collection_name: str) -> Dict:
    """
    Get the schema for a specific collection.
    
    Args:
        collection_name: Name of the collection
        
    Returns:
        Collection schema specification
    """
    if collection_name == DEFAULT_COLLECTIONS["documents"]:
        return DOCUMENT_SCHEMA
    elif collection_name == DEFAULT_COLLECTIONS["chunks"]:
        return CHUNK_SCHEMA
    elif collection_name == DEFAULT_COLLECTIONS["embeddings"]:
        return EMBEDDING_SCHEMA
    else:
        return {}

# MongoDB Spark configuration
MONGODB_SPARK_CONFIG = {
    "spark.mongodb.input.uri": get_mongodb_uri(),
    "spark.mongodb.output.uri": get_mongodb_uri(),
    "spark.mongodb.input.partitioner": "MongoSamplePartitioner",
    "spark.mongodb.input.partitionerOptions.partitionKey": "_id",
    "spark.mongodb.input.partitionerOptions.partitionSizeMB": "32",
    "spark.jars.packages": "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
}
