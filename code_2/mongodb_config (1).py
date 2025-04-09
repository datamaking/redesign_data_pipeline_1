"""
MongoDB data target configuration.
"""
from typing import Dict, Any, Optional
from ..base_config import BaseConfig

class MongoDBTargetConfig(BaseConfig):
    """Configuration for MongoDB data target."""
    
    def __init__(self, config_path: Optional[str] = None):
        super().__init__(config_path)
        
        # Set default MongoDB-specific configurations
        self._set_defaults()
        
    def _set_defaults(self) -> None:
        """Set default values for MongoDB configuration."""
        defaults = {
            'mongodb': {
                'uri': 'mongodb://localhost:27017',
                'host': 'localhost',
                'port': 27017,
                'database': '',
                'collection': '',
                'username': '',
                'password': '',
                'auth_source': 'admin',
                'auth_mechanism': 'SCRAM-SHA-1',
                'mode': 'append',  # Options: append, overwrite, ignore, error
                'batch_size': 1000,
                'ordered': True,
                'replace_document': False,
                'max_batch_size': 512,
                'write_concern': {
                    'w': 1,
                    'j': False,
                    'wtimeout': 10000
                },
                'ssl': False,
                'ssl_ca_file': '',
                'ssl_cert_file': '',
                'ssl_key_file': '',
                'ssl_key_password': '',
                'connection_timeout_ms': 10000,
                'max_pool_size': 100
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
        """Get connection parameters for MongoDB."""
        uri = self.get('mongodb.uri')
        if uri:
            return {'uri': uri}
        
        return {
            'host': self.get('mongodb.host'),
            'port': self.get('mongodb.port'),
            'username': self.get('mongodb.username'),
            'password': self.get('mongodb.password'),
            'auth_source': self.get('mongodb.auth_source'),
            'auth_mechanism': self.get('mongodb.auth_mechanism'),
            'ssl': self.get('mongodb.ssl'),
            'ssl_ca_file': self.get('mongodb.ssl_ca_file'),
            'ssl_cert_file': self.get('mongodb.ssl_cert_file'),
            'ssl_key_file': self.get('mongodb.ssl_key_file'),
            'ssl_key_password': self.get('mongodb.ssl_key_password'),
            'connection_timeout_ms': self.get('mongodb.connection_timeout_ms'),
            'max_pool_size': self.get('mongodb.max_pool_size')
        }
        
    def get_database(self) -> str:
        """Get database name."""
        return self.get('mongodb.database', '')
        
    def get_collection(self) -> str:
        """Get collection name."""
        return self.get('mongodb.collection', '')
        
    def get_mode(self) -> str:
        """Get write mode."""
        return self.get('mongodb.mode', 'append')
        
    def get_batch_size(self) -> int:
        """Get batch size for writing to MongoDB."""
        return self.get('mongodb.batch_size', 1000)
        
    def is_ordered(self) -> bool:
        """Check if writes should be ordered."""
        return self.get('mongodb.ordered', True)
        
    def should_replace_document(self) -> bool:
        """Check if documents should be replaced instead of updated."""
        return self.get('mongodb.replace_document', False)
        
    def get_write_concern(self) -> Dict[str, Any]:
        """Get write concern configuration."""
        return self.get('mongodb.write_concern', {'w': 1, 'j': False, 'wtimeout': 10000})