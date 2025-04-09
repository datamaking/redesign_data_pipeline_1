"""
MongoDB data source configuration.
"""
from typing import Dict, Any, Optional, List
from ..base_config import BaseConfig

class MongoDBSourceConfig(BaseConfig):
    """Configuration for MongoDB data source."""
    
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
                'read_preference': 'primaryPreferred',
                'query': {},
                'projection': {},
                'batch_size': 1000,
                'partition_key': '_id',
                'partition_size': 10,
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
        
    def get_query(self) -> Dict[str, Any]:
        """Get query filter."""
        return self.get('mongodb.query', {})
        
    def get_projection(self) -> Dict[str, Any]:
        """Get projection."""
        return self.get('mongodb.projection', {})
        
    def get_batch_size(self) -> int:
        """Get batch size for reading from MongoDB."""
        return self.get('mongodb.batch_size', 1000)
        
    def get_partition_info(self) -> Dict[str, Any]:
        """Get partitioning information for MongoDB reads."""
        return {
            'partition_key': self.get('mongodb.partition_key', '_id'),
            'partition_size': self.get('mongodb.partition_size', 10)
        }