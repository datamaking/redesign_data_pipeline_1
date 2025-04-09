"""
Chunking configuration settings for the NLP data pipeline.
"""
from typing import Dict, List, Optional, Union, Callable
import re

# Default chunking settings
DEFAULT_CHUNK_SIZE = 1000
DEFAULT_CHUNK_OVERLAP = 200
MAX_CHUNK_SIZE = 8192
MIN_CHUNK_SIZE = 100

# Chunking strategies
CHUNKING_STRATEGIES = {
    "fixed_size": "Split text into chunks of fixed size",
    "sentence": "Split text at sentence boundaries",
    "paragraph": "Split text at paragraph boundaries",
    "semantic": "Split text at semantic boundaries",
    "heading": "Split text at heading boundaries",
    "recursive": "Recursively split text based on multiple delimiters",
    "token": "Split text based on token count",
    "sliding_window": "Create overlapping chunks using a sliding window",
}

DEFAULT_CHUNKING_STRATEGY = "fixed_size"

# Delimiter patterns for different chunking strategies
SENTENCE_DELIMITER = r'(?<=[.!?])\s+'
PARAGRAPH_DELIMITER = r'\n\s*\n'
HEADING_DELIMITER = r'(?<=\n)#{1,6}\s+[^\n]+\n'
SEMANTIC_DELIMITERS = [
    r'\n\s*\n',  # Paragraphs
    r'(?<=[.!?])\s+',  # Sentences
    r'(?<=[:;])\s+',  # Clauses
]

# Regular expressions for different chunking strategies
SENTENCE_PATTERN = re.compile(SENTENCE_DELIMITER)
PARAGRAPH_PATTERN = re.compile(PARAGRAPH_DELIMITER)
HEADING_PATTERN = re.compile(HEADING_DELIMITER)

# Recursive chunking settings
RECURSIVE_CHUNKING_DELIMITERS = [
    {"delimiter": "\n\n", "min_chunk_size": 500},
    {"delimiter": "\n", "min_chunk_size": 300},
    {"delimiter": ". ", "min_chunk_size": 100},
]

# Token-based chunking settings
DEFAULT_TOKEN_LIMIT = 256
TOKEN_OVERLAP = 20
TOKENIZER_NAME = "gpt2"  # Options: gpt2, bert-base-uncased, t5-base

# Sliding window chunking settings
SLIDING_WINDOW_SIZE = 1000
SLIDING_WINDOW_STEP = 800

# Chunk smoothing settings
APPLY_CHUNK_SMOOTHING = True
SMOOTHING_STRATEGIES = {
    "boundary_adjustment": "Adjust chunk boundaries to avoid cutting sentences",
    "context_retention": "Retain context from previous chunks",
    "semantic_coherence": "Ensure semantic coherence within chunks",
    "length_normalization": "Normalize chunk lengths",
}
DEFAULT_SMOOTHING_STRATEGY = "boundary_adjustment"

# Boundary adjustment settings
MAX_BOUNDARY_ADJUSTMENT = 100  # Maximum number of characters to adjust
BOUNDARY_ADJUSTMENT_DELIMITERS = [". ", "! ", "? ", "\n", ";"]

# Context retention settings
CONTEXT_RETENTION_SIZE = 100  # Number of characters to retain from previous chunk
CONTEXT_SEPARATOR = "\n...\n"  # Separator between context and new content

# Semantic coherence settings
SEMANTIC_COHERENCE_THRESHOLD = 0.7  # Minimum cosine similarity for coherence
SEMANTIC_COHERENCE_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

# Length normalization settings
TARGET_LENGTH_VARIATION = 0.2  # Maximum allowed variation from target length
MIN_NORMALIZED_CHUNK_SIZE = 200

# Chunk metadata settings
INCLUDE_CHUNK_METADATA = True
CHUNK_METADATA_FIELDS = [
    "chunk_id",
    "document_id",
    "chunk_index",
    "chunk_size",
    "chunk_strategy",
    "start_char_index",
    "end_char_index",
    "overlap_with_previous",
    "overlap_with_next",
]

# Chunk processing settings
PARALLEL_CHUNKING = True
MAX_WORKERS = 4

# Function to get chunking strategy configuration
def get_chunking_strategy_config(strategy_name: str) -> Dict[str, Union[str, int, bool, List, Dict]]:
    """
    Get configuration for a specific chunking strategy.
    
    Args:
        strategy_name: Name of the chunking strategy
        
    Returns:
        Dictionary of chunking strategy configuration
    """
    if strategy_name == "fixed_size":
        return {
            "name": "fixed_size",
            "description": CHUNKING_STRATEGIES["fixed_size"],
            "chunk_size": DEFAULT_CHUNK_SIZE,
            "chunk_overlap": DEFAULT_CHUNK_OVERLAP,
        }
    elif strategy_name == "sentence":
        return {
            "name": "sentence",
            "description": CHUNKING_STRATEGIES["sentence"],
            "delimiter_pattern": SENTENCE_DELIMITER,
            "max_chunk_size": MAX_CHUNK_SIZE,
            "min_chunk_size": MIN_CHUNK_SIZE,
        }
    elif strategy_name == "paragraph":
        return {
            "name": "paragraph",
            "description": CHUNKING_STRATEGIES["paragraph"],
            "delimiter_pattern": PARAGRAPH_DELIMITER,
            "max_chunk_size": MAX_CHUNK_SIZE,
            "min_chunk_size": MIN_CHUNK_SIZE,
        }
    elif strategy_name == "semantic":
        return {
            "name": "semantic",
            "description": CHUNKING_STRATEGIES["semantic"],
            "delimiters": SEMANTIC_DELIMITERS,
            "max_chunk_size": MAX_CHUNK_SIZE,
            "min_chunk_size": MIN_CHUNK_SIZE,
        }
    elif strategy_name == "heading":
        return {
            "name": "heading",
            "description": CHUNKING_STRATEGIES["heading"],
            "delimiter_pattern": HEADING_DELIMITER,
            "max_chunk_size": MAX_CHUNK_SIZE,
            "min_chunk_size": MIN_CHUNK_SIZE,
        }
    elif strategy_name == "recursive":
        return {
            "name": "recursive",
            "description": CHUNKING_STRATEGIES["recursive"],
            "delimiters": RECURSIVE_CHUNKING_DELIMITERS,
            "max_chunk_size": MAX_CHUNK_SIZE,
            "min_chunk_size": MIN_CHUNK_SIZE,
        }
    elif strategy_name == "token":
        return {
            "name": "token",
            "description": CHUNKING_STRATEGIES["token"],
            "token_limit": DEFAULT_TOKEN_LIMIT,
            "token_overlap": TOKEN_OVERLAP,
            "tokenizer_name": TOKENIZER_NAME,
        }
    elif strategy_name == "sliding_window":
        return {
            "name": "sliding_window",
            "description": CHUNKING_STRATEGIES["sliding_window"],
            "window_size": SLIDING_WINDOW_SIZE,
            "window_step": SLIDING_WINDOW_STEP,
        }
    else:
        # Return default strategy if strategy name is not found
        return get_chunking_strategy_config(DEFAULT_CHUNKING_STRATEGY)

# Function to get smoothing strategy configuration
def get_smoothing_strategy_config(strategy_name: str) -> Dict[str, Union[str, int, float, bool]]:
    """
    Get configuration for a specific smoothing strategy.
    
    Args:
        strategy_name: Name of the smoothing strategy
        
    Returns:
        Dictionary of smoothing strategy configuration
    """
    if strategy_name == "boundary_adjustment":
        return {
            "name": "boundary_adjustment",
            "description": SMOOTHING_STRATEGIES["boundary_adjustment"],
            "max_adjustment": MAX_BOUNDARY_ADJUSTMENT,
            "delimiters": BOUNDARY_ADJUSTMENT_DELIMITERS,
        }
    elif strategy_name == "context_retention":
        return {
            "name": "context_retention",
            "description": SMOOTHING_STRATEGIES["context_retention"],
            "retention_size": CONTEXT_RETENTION_SIZE,
            "separator": CONTEXT_SEPARATOR,
        }
    elif strategy_name == "semantic_coherence":
        return {
            "name": "semantic_coherence",
            "description": SMOOTHING_STRATEGIES["semantic_coherence"],
            "threshold": SEMANTIC_COHERENCE_THRESHOLD,
            "model": SEMANTIC_COHERENCE_MODEL,
        }
    elif strategy_name == "length_normalization":
        return {
            "name": "length_normalization",
            "description": SMOOTHING_STRATEGIES["length_normalization"],
            "target_variation": TARGET_LENGTH_VARIATION,
            "min_chunk_size": MIN_NORMALIZED_CHUNK_SIZE,
        }
    else:
        # Return default strategy if strategy name is not found
        return get_smoothing_strategy_config(DEFAULT_SMOOTHING_STRATEGY)
