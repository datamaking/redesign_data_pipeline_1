"""
Neo4j data target configuration.
"""
from typing import Dict, Any, Optional, List
from ..base_config import BaseConfig

class Neo4jTargetConfig(BaseConfig):
    """Configuration for Neo4j data target."""
    
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
                'mode': 'append',  # Options: append, overwrite, ignore, error
                'batch_size': 1000,
                'node_label': '',
                'relationship_type': '',
                'node_keys': [],
                'relationship_source_node_keys': [],
                'relationship_target_node_keys': [],
                'relationship_properties': [],
                'connection_timeout': 30,  # seconds
                'max_connection_lifetime': 3600,  # seconds
                'max_connection_pool_size': 100,
                'encryption': False,
                'trust_strategy': 'TRUST_SYSTEM_CA_SIGNED_CERTIFICATES',  # Options: TRUST_ALL_CERTIFICATES, TRUST_SYSTEM_CA_SIGNED_CERTIFICATES
                'trusted_certificates': '',
                'connection_acquisition_timeout': 60,  # seconds
                'vector_property': '',
                'vector_dimension': 0,
                'create_vector_index': False,
                'vector_index_type': 'cosine',  # Options: cosine, euclidean
                'vector_index_options': {}
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
        
    def get_mode(self) -> str:
        """Get write mode."""
        return self.get('neo4j.mode', 'append')
        
    def get_batch_size(self) -> int:
        """Get batch size for writing to Neo4j."""
        return self.get('neo4j.batch_size', 1000)
        
    def get_node_config(self) -> Dict[str, Any]:
        """Get node configuration for Neo4j."""
        return {
            'node_label': self.get('neo4j.node_label', ''),
            'node_keys': self.get('neo4j.node_keys', [])
        }
        
    def get_relationship_config(self) -> Dict[str, Any]:
        """Get relationship configuration for Neo4j."""
        return {
            'relationship_type': self.get('neo4j.relationship_type', ''),
            'relationship_source_node_keys': self.get('neo4j.relationship_source_node_keys', []),
            'relationship_target_node_keys': self.get('neo4j.relationship_target_node_keys', []),
            'relationship_properties': self.get('neo4j.relationship_properties', [])
        }
        
    def get_vector_config(self) -> Dict[str, Any]:
        """Get vector configuration for Neo4j."""
        return {
            'vector_property': self.get('neo4j.vector_property', ''),
            'vector_dimension': self.get('neo4j.vector_dimension', 0),
            'create_vector_index': self.get('neo4j.create_vector_index', False),
            'vector_index_type': self.get('neo4j.vector_index_type', 'cosine'),
            'vector_index_options': self.get('neo4j.vector_index_options', {})
        }