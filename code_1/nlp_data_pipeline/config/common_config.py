"""
Common configuration settings for the NLP data pipeline.
This module contains configuration parameters that are shared across all pipeline components.
"""
import os
from pathlib import Path
from typing import Dict, List, Optional, Union
import logging

# Project base paths
PROJECT_ROOT = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"
MODELS_DIR = PROJECT_ROOT / "models"

# Create directories if they don't exist
for directory in [DATA_DIR, LOGS_DIR, MODELS_DIR]:
    directory.mkdir(exist_ok=True, parents=True)

# Logging configuration
LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Default embedding model configuration
DEFAULT_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIMENSION = 384  # Dimension for the default model

# Chunking configuration
DEFAULT_CHUNK_SIZE = 1000
DEFAULT_CHUNK_OVERLAP = 200
MAX_CHUNK_SIZE = 8192

# Processing configuration
DEFAULT_BATCH_SIZE = 64
NUM_WORKERS = os.cpu_count() or 4
DEFAULT_SPARK_CONFIG = {
    "spark.driver.memory": "4g",
    "spark.executor.memory": "4g",
    "spark.executor.cores": "2",
    "spark.driver.maxResultSize": "2g",
    "spark.sql.shuffle.partitions": "20",
    "spark.default.parallelism": "20",
}

# Database connection timeouts (seconds)
DB_CONNECTION_TIMEOUT = 30
DB_QUERY_TIMEOUT = 300

# Vector search configuration
DEFAULT_TOP_K = 5
DEFAULT_SIMILARITY_THRESHOLD = 0.7

# Cache configuration
ENABLE_CACHE = True
CACHE_TTL = 3600  # Time to live in seconds

# API keys and credentials should be loaded from environment variables
# or a secure vault in production environments
def get_api_key(key_name: str, default: Optional[str] = None) -> Optional[str]:
    """Safely retrieve API keys from environment variables."""
    return os.environ.get(key_name, default)

# Environment-specific configurations
ENV = os.environ.get("PIPELINE_ENV", "development")

# Configuration dictionary that can be overridden by environment-specific settings
CONFIG: Dict[str, Union[str, int, float, bool, Dict, List]] = {
    "environment": ENV,
    "log_level": LOG_LEVEL,
    "batch_size": DEFAULT_BATCH_SIZE,
    "num_workers": NUM_WORKERS,
    "spark_config": DEFAULT_SPARK_CONFIG,
    "embedding_model": DEFAULT_EMBEDDING_MODEL,
    "embedding_dimension": EMBEDDING_DIMENSION,
    "chunk_size": DEFAULT_CHUNK_SIZE,
    "chunk_overlap": DEFAULT_CHUNK_OVERLAP,
    "enable_cache": ENABLE_CACHE,
    "cache_ttl": CACHE_TTL,
    "top_k": DEFAULT_TOP_K,
    "similarity_threshold": DEFAULT_SIMILARITY_THRESHOLD,
}

# Load environment-specific configurations
if ENV == "production":
    CONFIG.update({
        "log_level": logging.WARNING,
        "batch_size": 128,
        "enable_cache": True,
    })
elif ENV == "testing":
    CONFIG.update({
        "log_level": logging.DEBUG,
        "batch_size": 16,
        "enable_cache": False,
    })
