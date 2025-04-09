"""
Embedding model configuration settings for the NLP data pipeline.
"""
from typing import Dict, List, Optional, Union
import os
from pathlib import Path

from nlp_data_pipeline.config.common_config import MODELS_DIR

# Create models directory if it doesn't exist
MODELS_DIR.mkdir(exist_ok=True, parents=True)

# Default embedding model
DEFAULT_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

# Available embedding models
EMBEDDING_MODELS = {
    "all-MiniLM-L6-v2": {
        "name": "sentence-transformers/all-MiniLM-L6-v2",
        "dimension": 384,
        "description": "A small and fast sentence transformer model",
        "type": "sentence-transformers",
        "max_sequence_length": 256,
        "normalize_embeddings": True,
    },
    "all-mpnet-base-v2": {
        "name": "sentence-transformers/all-mpnet-base-v2",
        "dimension": 768,
        "description": "A more powerful sentence transformer model",
        "type": "sentence-transformers",
        "max_sequence_length": 384,
        "normalize_embeddings": True,
    },
    "e5-large-v2": {
        "name": "intfloat/e5-large-v2",
        "dimension": 1024,
        "description": "E5 large model for text embeddings",
        "type": "transformers",
        "max_sequence_length": 512,
        "normalize_embeddings": True,
        "prefix": "query: ",
    },
    "bge-large-en-v1.5": {
        "name": "BAAI/bge-large-en-v1.5",
        "dimension": 1024,
        "description": "BGE large English model",
        "type": "sentence-transformers",
        "max_sequence_length": 512,
        "normalize_embeddings": True,
    },
    "openai-ada-002": {
        "name": "text-embedding-ada-002",
        "dimension": 1536,
        "description": "OpenAI's Ada embedding model",
        "type": "openai",
        "max_sequence_length": 8191,
        "normalize_embeddings": False,
        "api_key_env": "OPENAI_API_KEY",
    },
    "cohere-embed-english-v3.0": {
        "name": "embed-english-v3.0",
        "dimension": 1024,
        "description": "Cohere's English embedding model",
        "type": "cohere",
        "max_sequence_length": 512,
        "normalize_embeddings": False,
        "api_key_env": "COHERE_API_KEY",
    },
}

# Model cache settings
MODEL_CACHE_DIR = MODELS_DIR / "cache"
MODEL_CACHE_DIR.mkdir(exist_ok=True, parents=True)
MODEL_CACHE_SIZE = 2  # Number of models to keep in memory

# Model download settings
DOWNLOAD_MODELS = True
FORCE_DOWNLOAD = False
RESUME_DOWNLOAD = True
LOCAL_FILES_ONLY = False

# Batch processing settings
DEFAULT_BATCH_SIZE = 32
MAX_SEQUENCE_LENGTH = 512

# Device settings
DEVICE = "cuda" if os.environ.get("USE_CUDA", "0") == "1" else "cpu"
DEVICE_MAP = "auto" if DEVICE == "cuda" else None
TORCH_DTYPE = "float16" if DEVICE == "cuda" else "float32"

# Quantization settings
QUANTIZE_MODELS = os.environ.get("QUANTIZE_MODELS", "0") == "1"
QUANTIZATION_TYPE = "int8"  # Options: int8, int4, fp16

# API settings for hosted models
OPENAI_API_BASE = os.environ.get("OPENAI_API_BASE", "https://api.openai.com/v1")
OPENAI_API_VERSION = os.environ.get("OPENAI_API_VERSION", "2023-05-15")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

COHERE_API_BASE = os.environ.get("COHERE_API_BASE", "https://api.cohere.ai/v1")
COHERE_API_KEY = os.environ.get("COHERE_API_KEY", "")

# Embedding pooling strategies
POOLING_STRATEGIES = {
    "mean": "Mean pooling of token embeddings",
    "max": "Max pooling of token embeddings",
    "cls": "Use [CLS] token embedding",
    "last_hidden_state": "Use last hidden state",
}
DEFAULT_POOLING_STRATEGY = "mean"

# Embedding normalization settings
NORMALIZE_EMBEDDINGS = True
NORMALIZATION_TYPE = "l2"  # Options: l2, l1, max

# Embedding caching settings
CACHE_EMBEDDINGS = True
EMBEDDING_CACHE_DIR = MODELS_DIR / "embedding_cache"
EMBEDDING_CACHE_DIR.mkdir(exist_ok=True, parents=True)

# Function to get embedding model config
def get_embedding_model_config(model_name: str) -> Dict[str, Union[str, int, bool]]:
    """
    Get configuration for a specific embedding model.
    
    Args:
        model_name: Name of the embedding model
        
    Returns:
        Dictionary of model configuration
    """
    # Check if model name is a key in EMBEDDING_MODELS
    if model_name in EMBEDDING_MODELS:
        return EMBEDDING_MODELS[model_name]
    
    # Check if model name is a full name in EMBEDDING_MODELS values
    for model_config in EMBEDDING_MODELS.values():
        if model_config["name"] == model_name:
            return model_config
    
    # Return default model config if model name is not found
    return EMBEDDING_MODELS[DEFAULT_EMBEDDING_MODEL.split("/")[-1]]

# Function to get API key for a model
def get_model_api_key(model_config: Dict[str, Union[str, int, bool]]) -> Optional[str]:
    """
    Get API key for a specific model.
    
    Args:
        model_config: Model configuration dictionary
        
    Returns:
        API key string or None if not applicable
    """
    if "api_key_env" in model_config:
        return os.environ.get(model_config["api_key_env"], "")
    return None

# Spark configuration for embedding models
EMBEDDING_SPARK_CONFIG = {
    "spark.jars.packages": "com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.0",
    "spark.driver.memory": "4g",
    "spark.executor.memory": "4g",
}
