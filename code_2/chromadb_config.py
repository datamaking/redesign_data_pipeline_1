"""
ChromaDB data source configuration.
"""
from typing import Dict, Any, Optional, List
from ..base_config import BaseConfig

class ChromaDBSourceConfig(BaseConfig):
    """Configuration for ChromaDB data source."""
    
    def __init__(self, config_path: Optional[str] = None):
        super().__init__(config_path)
        
        # Set default ChromaDB-specific configurations
        self._set_defaults()
        
    def _set_defaults(self) -> None:
        """Set default values for ChromaDB configuration."""
        defaults = {
            'chromadb': {
                'host': 'localhost',
                'port': 8000,
                'collection_name': '',
                'persist_directory': '.chromadb',
                'client_type': 'http',  # Options: 'http', 'persistent'
                'embedding_function': 'default',  # Options: 'default', 'openai', 'sentence_transformers'
                'embedding_model': 'all-MiniLM-L6-v2',  # For sentence_transformers
                'openai_api_key': '',  # For OpenAI embeddings
                'batch_size': 100,
                'where_filter': {},
                'include_embeddings': True,
                'include_documents': True,
                'include_metadatas': True
            }
        }
        
        # Only set defaults if not already set
        for key, value in defaults.items():
            if isinstance(value, dict):
                for subkey, subvalue in value.items():
                    full_key = f"{key}.{subkey}"
                    if self.get(full_key) is None:
                        self.set(full_key, subvalue)
            elif self.get(key) is None:
                self.set(key, value)
                
    def get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters for ChromaDB."""
        client_type = self.get('chromadb.client_type', 'http')
        
        if client_type == 'http':
            return {
                'client_type': 'http',
                'host': self.get('chromadb.host', 'localhost'),
                'port': self.get('chromadb.port', 8000)
            }
        else:  # persistent
            return {
                'client_type': 'persistent',
                'persist_directory': self.get('chromadb.persist_directory', '.chromadb')
            }
        
    def get_collection_name(self) -> str:
        """Get collection name."""
        return self.get('chromadb.collection_name', '')
        
    def get_embedding_config(self) -> Dict[str, Any]:
        """Get embedding function configuration."""
        embedding_function = self.get('chromadb.embedding_function', 'default')
        
        if embedding_function == 'openai':
            return {
                'type': 'openai',
                'api_key': self.get('chromadb.openai_api_key', '')
            }
        elif embedding_function == 'sentence_transformers':
            return {
                'type': 'sentence_transformers',
                'model': self.get('chromadb.embedding_model', 'all-MiniLM-L6-v2')
            }
        else:  # default
            return {
                'type': 'default'
            }
        
    def get_batch_size(self) -> int:
        """Get batch size for reading from ChromaDB."""
        return self.get('chromadb.batch_size', 100)
        
    def get_where_filter(self) -> Dict[str, Any]:
        """Get where filter for querying ChromaDB."""
        return self.get('chromadb.where_filter', {})
        
    def get_include_config(self) -> Dict[str, bool]:
        """Get include configuration for ChromaDB queries."""
        return {
            'include_embeddings': self.get('chromadb.include_embeddings', True),
            'include_documents': self.get('chromadb.include_documents', True),
            'include_metadatas': self.get('chromadb.include_metadatas', True)
        }