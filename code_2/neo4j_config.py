"""
Neo4j data source configuration.
"""
from typing import Dict, Any, Optional, List
from ..base_config import BaseConfig

class Neo4jSourceConfig(BaseConfig):
    """Configuration for Neo4j data source."""
    
    def __init__(self, config_path: Optional[str] = None):
        super().__init__(config_path)
        
        # Set default Neo4j-specific configurations
        self._set_defaults()
        
    def _set_defaults(self) -> None:
        """Set default values for Neo4j configuration."""
        defaults = {
            'neo4j': {
                'url': 'bolt://localhost:7687',
                'host': 'localhost',
                'port': 7687,
                'protocol': 'bolt',  # Options: bolt, bolt+s, bolt+ssc, neo4j, neo4j+s, neo4j+ssc
                'database': 'neo4j',
                'username': 'neo4j',
                'password': '',
                'query': '',
                'params': {},
                'batch_size': 1000,
                'partition_size': 10000,
                'partition_key': 'id',
                'connection_timeout': 30,  # seconds
                'max_connection_lifetime': 3600,  # seconds
                'max_connection_pool_size': 100,
                'encryption': False,
                'trust_strategy': 'TRUST_SYSTEM_CA_SIGNED_CERTIFICATES',  # Options: TRUST_ALL_CERTIFICATES, TRUST_SYSTEM_CA_SIGNED_CERTIFICATES
                'trusted_certificates': '',
                'connection_acquisition_timeout': 60  # seconds
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
        """Get connection parameters for Neo4j."""
        url = self.get('neo4j.url')
        if not url:
            protocol = self.get('neo4j.protocol', 'bolt')
            host = self.get('neo4j.host', 'localhost')
            port = self.get('neo4j.port', 7687)
            url = f"{protocol}://{host}:{port}"
            
        return {
            'url': url,
            'database': self.get('neo4j.database', 'neo4j'),
            'auth': (
                self.get('neo4j.username', 'neo4j'),
                self.get('neo4j.password', '')
            ),
            'encryption': self.get('neo4j.encryption', False),
            'trust': self.get('neo4j.trust_strategy', 'TRUST_SYSTEM_CA_SIGNED_CERTIFICATES'),
            'trusted_certificates': self.get('neo4j.trusted_certificates', ''),
            'connection_timeout': self.get('neo4j.connection_timeout', 30),
            'max_connection_lifetime': self.get('neo4j.max_connection_lifetime', 3600),
            'max_connection_pool_size': self.get('neo4j.max_connection_pool_size', 100),
            'connection_acquisition_timeout': self.get('neo4j.connection_acquisition_timeout', 60)
        }
        
    def get_query(self) -> str:
        """Get Cypher query."""
        return self.get('neo4j.query', '')
        
    def get_params(self) -> Dict[str, Any]:
        """Get query parameters."""
        return self.get('neo4j.params', {})
        
    def get_batch_size(self) -> int:
        """Get batch size for reading from Neo4j."""
        return self.get('neo4j.batch_size', 1000)
        
    def get_partition_info(self) -> Dict[str, Any]:
        """Get partitioning information for Neo4j reads."""
        return {
            'partition_size': self.get('neo4j.partition_size', 10000),
            'partition_key': self.get('neo4j.partition_key', 'id')
        }