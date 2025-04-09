"""
Preprocessing configuration settings for the NLP data pipeline.
"""
from typing import Dict, List, Optional, Set, Union
import re
import os

# Text cleaning settings
REMOVE_HTML_TAGS = True
REMOVE_URLS = True
REMOVE_EMAIL_ADDRESSES = True
REMOVE_PHONE_NUMBERS = True
REMOVE_SPECIAL_CHARACTERS = False  # Keep special characters by default
REMOVE_EXTRA_WHITESPACE = True
REMOVE_EXTRA_NEWLINES = True
CONVERT_TO_LOWERCASE = True
EXPAND_CONTRACTIONS = True

# Regular expressions for text cleaning
HTML_TAG_PATTERN = re.compile(r'<.*?>')
URL_PATTERN = re.compile(r'https?://\S+|www\.\S+')
EMAIL_PATTERN = re.compile(r'\S+@\S+\.\S+')
PHONE_PATTERN = re.compile(r'\+?[\d\s()-]{7,}')
SPECIAL_CHAR_PATTERN = re.compile(r'[^\w\s]')
EXTRA_WHITESPACE_PATTERN = re.compile(r'\s+')
EXTRA_NEWLINE_PATTERN = re.compile(r'\n+')

# Common contractions (abbreviated version)
CONTRACTIONS_MAP = {
    "ain't": "am not",
    "aren't": "are not",
    "can't": "cannot",
    "don't": "do not",
    "isn't": "is not",
    "it's": "it is",
    "i'm": "i am",
    "i've": "i have",
    "won't": "will not",
    "wouldn't": "would not"
}

# Language detection settings
DETECT_LANGUAGE = True
DEFAULT_LANGUAGE = "en"
LANGUAGE_DETECTION_CONFIDENCE = 0.8

# Stopwords settings
REMOVE_STOPWORDS = False  # Keep stopwords by default
CUSTOM_STOPWORDS: Set[str] = set()
STOPWORDS_BY_LANGUAGE = {
    "en": {"a", "an", "the", "and", "or", "but", "is", "are", "was", "were", "be", "been", "being"},
    "es": {"el", "la", "los", "las", "un", "una", "unos", "unas", "y", "o", "pero", "es", "son", "era", "eran"},
    "fr": {"le", "la", "les", "un", "une", "des", "et", "ou", "mais", "est", "sont", "était", "étaient"}
}

# Tokenization settings
TOKENIZE_TEXT = True
TOKENIZER_TYPE = "word"  # Options: word, sentence, subword
PRESERVE_CASE = False
PRESERVE_PUNCTUATION = True

# Stemming and lemmatization settings
APPLY_STEMMING = False
APPLY_LEMMATIZATION = True
STEMMER_TYPE = "porter"  # Options: porter, snowball, lancaster
LEMMATIZER_TYPE = "wordnet"  # Options: wordnet, spacy

# Named entity recognition settings
EXTRACT_ENTITIES = True
ENTITY_TYPES = ["PERSON", "ORGANIZATION", "LOCATION", "DATE", "TIME", "MONEY", "PERCENT"]

# Text normalization settings
NORMALIZE_UNICODE = True
UNICODE_NORMALIZATION_FORM = "NFKC"  # Options: NFC, NFD, NFKC, NFKD
NORMALIZE_WHITESPACE = True
NORMALIZE_QUOTES = True
NORMALIZE_NUMBERS = True
NORMALIZE_DATES = True

# Spell checking settings
APPLY_SPELL_CHECKING = False
SPELL_CHECKER_LANGUAGE = "en"
SPELL_CHECKER_DISTANCE = 2  # Maximum edit distance for spell correction

# Text augmentation settings
APPLY_TEXT_AUGMENTATION = False
AUGMENTATION_TECHNIQUES = ["synonym_replacement", "random_insertion", "random_swap", "random_deletion"]
AUGMENTATION_PROBABILITY = 0.1
MAX_AUGMENTATIONS_PER_TEXT = 5

# Language-specific preprocessing settings
LANGUAGE_SPECIFIC_PREPROCESSING = {
    "en": {
        "remove_stopwords": True,
        "apply_stemming": False,
        "apply_lemmatization": True
    },
    "es": {
        "remove_stopwords": True,
        "apply_stemming": False,
        "apply_lemmatization": True
    },
    "fr": {
        "remove_stopwords": True,
        "apply_stemming": False,
        "apply_lemmatization": True
    }
}

# Preprocessing pipeline configuration
DEFAULT_PREPROCESSING_PIPELINE = [
    "clean_html",
    "clean_urls",
    "clean_email_addresses",
    "clean_phone_numbers",
    "normalize_unicode",
    "normalize_whitespace",
    "expand_contractions",
    "convert_to_lowercase",
    "tokenize",
    "lemmatize",
    "extract_entities"
]

# Function to get language-specific preprocessing settings
def get_language_specific_settings(language: str) -> Dict[str, bool]:
    """
    Get language-specific preprocessing settings.
    
    Args:
        language: Language code (e.g., 'en', 'es', 'fr')
        
    Returns:
        Dictionary of language-specific preprocessing settings
    """
    return LANGUAGE_SPECIFIC_PREPROCESSING.get(language, LANGUAGE_SPECIFIC_PREPROCESSING[DEFAULT_LANGUAGE])

# Function to get stopwords for a specific language
def get_stopwords(language: str) -> Set[str]:
    """
    Get stopwords for a specific language.
    
    Args:
        language: Language code (e.g., 'en', 'es', 'fr')
        
    Returns:
        Set of stopwords for the specified language
    """
    return STOPWORDS_BY_LANGUAGE.get(language, set()).union(CUSTOM_STOPWORDS)
