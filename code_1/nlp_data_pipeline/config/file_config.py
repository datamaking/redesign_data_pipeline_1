"""
File-specific configuration settings for the NLP data pipeline.
This includes settings for text files, HTML files, CSV, and JSON.
"""
from pathlib import Path
from typing import Dict, List, Optional, Set
import os

from nlp_data_pipeline.config.common_config import DATA_DIR

# Base directories for different file types
TEXT_FILES_DIR = DATA_DIR / "text"
HTML_FILES_DIR = DATA_DIR / "html"
CSV_FILES_DIR = DATA_DIR / "csv"
JSON_FILES_DIR = DATA_DIR / "json"

# Create directories if they don't exist
for directory in [TEXT_FILES_DIR, HTML_FILES_DIR, CSV_FILES_DIR, JSON_FILES_DIR]:
    directory.mkdir(exist_ok=True, parents=True)

# File encoding settings
DEFAULT_ENCODING = "utf-8"
FALLBACK_ENCODINGS = ["latin-1", "cp1252", "iso-8859-1"]

# File extension filters
TEXT_FILE_EXTENSIONS: Set[str] = {".txt", ".md", ".rst", ".log"}
HTML_FILE_EXTENSIONS: Set[str] = {".html", ".htm", ".xhtml"}
CSV_FILE_EXTENSIONS: Set[str] = {".csv", ".tsv"}
JSON_FILE_EXTENSIONS: Set[str] = {".json", ".jsonl"}

# CSV parsing settings
CSV_DELIMITER = ","
TSV_DELIMITER = "\t"
CSV_QUOTE_CHAR = '"'
CSV_ESCAPE_CHAR = "\\"
CSV_HEADER = True  # Whether CSV files have a header row

# JSON parsing settings
JSON_LINES = False  # Whether JSON files are in JSON Lines format
JSON_ALLOW_NAN = True

# HTML parsing settings
HTML_PARSER = "html.parser"  # Options: "html.parser", "lxml", "html5lib"
HTML_CLEAN_TAGS = True  # Whether to clean HTML tags
HTML_EXTRACT_LINKS = True  # Whether to extract links from HTML
HTML_EXTRACT_IMAGES = True  # Whether to extract image information
HTML_EXTRACT_TABLES = True  # Whether to extract tables from HTML
HTML_EXTRACT_METADATA = True  # Whether to extract metadata from HTML

# HTML elements to extract text from (if HTML_CLEAN_TAGS is True)
HTML_TEXT_ELEMENTS = [
    "p", "h1", "h2", "h3", "h4", "h5", "h6", 
    "div", "span", "article", "section", "main",
    "li", "td", "th", "blockquote", "pre", "code"
]

# HTML elements to exclude when extracting text
HTML_EXCLUDE_ELEMENTS = [
    "script", "style", "nav", "footer", "header", 
    "aside", "noscript", "iframe", "svg", "canvas"
]

# File reading settings
FILE_CHUNK_SIZE = 1024 * 1024  # 1MB chunks for reading large files
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB maximum file size

# File output settings
OUTPUT_TEXT_DIR = DATA_DIR / "processed" / "text"
OUTPUT_HTML_DIR = DATA_DIR / "processed" / "html"
OUTPUT_CSV_DIR = DATA_DIR / "processed" / "csv"
OUTPUT_JSON_DIR = DATA_DIR / "processed" / "json"

# Create output directories if they don't exist
for directory in [OUTPUT_TEXT_DIR, OUTPUT_HTML_DIR, OUTPUT_CSV_DIR, OUTPUT_JSON_DIR]:
    directory.mkdir(exist_ok=True, parents=True)

# File metadata extraction settings
EXTRACT_FILE_METADATA = True
FILE_METADATA_FIELDS = [
    "filename", "filepath", "filesize", "created_time", 
    "modified_time", "accessed_time", "file_extension"
]

# Function to get file paths recursively
def get_file_paths(
    directory: Path, 
    extensions: Optional[Set[str]] = None,
    recursive: bool = True
) -> List[Path]:
    """
    Get all file paths in a directory with specified extensions.
    
    Args:
        directory: Directory to search in
        extensions: Set of file extensions to filter by
        recursive: Whether to search recursively
        
    Returns:
        List of file paths
    """
    if not directory.exists():
        return []
    
    if recursive:
        files = [p for p in directory.glob("**/*") if p.is_file()]
    else:
        files = [p for p in directory.glob("*") if p.is_file()]
    
    if extensions:
        files = [f for f in files if f.suffix.lower() in extensions]
    
    return files

# Function to get file metadata
def get_file_metadata(file_path: Path) -> Dict[str, str]:
    """
    Get metadata for a file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Dictionary of file metadata
    """
    if not file_path.exists():
        return {}
    
    stat = file_path.stat()
    return {
        "filename": file_path.name,
        "filepath": str(file_path),
        "filesize": stat.st_size,
        "created_time": stat.st_ctime,
        "modified_time": stat.st_mtime,
        "accessed_time": stat.st_atime,
        "file_extension": file_path.suffix.lower(),
    }
