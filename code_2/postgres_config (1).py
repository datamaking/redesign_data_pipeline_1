"""
PostgreSQL data target configuration.
"""
from typing import Dict, Any, Optional, List
from ..base_config import BaseConfig

class PostgresTargetConfig(BaseConfig):
    """Configuration for PostgreSQL data target."""
    
    def __init__(self, config_path: Optional[str] = None):
        super().__init__(config_path)
        
        # Set default PostgreSQL-specific configurations
        self._set_defaults()
        
    def _set_defaults(self) -> None:
        """Set default values for PostgreSQL configuration."""
        defaults = {
            'postgres': {
                'host': 'localhost',
                'port': 5432,
                'database': 'postgres',
                'schema': 'public',
                'table': '',
                'username': '',
                'password': '',
                'url': '',  # If provided, overrides host/port/database
                'driver': 'org.postgresql.Driver',
                'mode': 'append',  # Options: append, overwrite, ignore, error
                'batch_size': 1000,
                'isolation_level': 'READ_COMMITTED',  # Options: READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE
                'truncate': False,
                'create_table_options': '',
                'create_table_column_types': {},
                'ssl_mode': 'prefer',  # Options: disable, allow, prefer, require, verify-ca, verify-full
                'ssl_root_cert': '',
                'ssl_cert': '',
                'ssl_key': '',
                'properties': {},
                'vector_column': '',
                'vector_dimension': 0,
                'create_vector_index': False,
                'vector_index_type': 'ivfflat',  # Options: ivfflat, hnsw
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
        """Get connection parameters for PostgreSQL."""
        url = self.get('postgres.url')
        if not url:
            url = f"jdbc:postgresql://{self.get('postgres.host')}:{self.get('postgres.port')}/{self.get('postgres.database')}"
            
        properties = self.get('postgres.properties', {}).copy()
        
        # Add SSL properties if needed
        ssl_mode = self.get('postgres.ssl_mode')
        if ssl_mode and ssl_mode != 'disable':
            properties['ssl'] = 'true'
            properties['sslmode'] = ssl_mode
            
            ssl_root_cert = self.get('postgres.ssl_root_cert')
            if ssl_root_cert:
                properties['sslrootcert'] = ssl_root_cert
                
            ssl_cert = self.get('postgres.ssl_cert')
            if ssl_cert:
                properties['sslcert'] = ssl_cert
                
            ssl_key = self.get('postgres.ssl_key')
            if ssl_key:
                properties['sslkey'] = ssl_key
        
        return {
            'url': url,
            'driver': self.get('postgres.driver', 'org.postgresql.Driver'),
            'user': self.get('postgres.username', ''),
            'password': self.get('postgres.password', ''),
            'properties': properties
        }
        
    def get_schema(self) -> str:
        """Get schema name."""
        return self.get('postgres.schema', 'public')
        
    def get_table(self) -> str:
        """Get table name."""
        return self.get('postgres.table', '')
        
    def get_mode(self) -> str:
        """Get write mode."""
        return self.get('postgres.mode', 'append')
        
    def get_batch_size(self) -> int:
        """Get batch size for writing to PostgreSQL."""
        return self.get('postgres.batch_size', 1000)
        
    def get_isolation_level(self) -> str:
        """Get transaction isolation level."""
        return self.get('postgres.isolation_level', 'READ_COMMITTED')
        
    def should_truncate(self) -> bool:
        """Check if table should be truncated before writing."""
        return self.get('postgres.truncate', False)
        
    def get_create_table_options(self) -> str:
        """Get options for table creation."""
        return self.get('postgres.create_table_options', '')
        
    def get_create_table_column_types(self) -> Dict[str, str]:
        """Get column type mappings for table creation."""
        return self.get('postgres.create_table_column_types', {})
        
    def get_vector_config(self) -> Dict[str, Any]:
        """Get vector configuration for PostgreSQL."""
        return {
            'vector_column': self.get('postgres.vector_column', ''),
            'vector_dimension': self.get('postgres.vector_dimension', 0),
            'create_vector_index': self.get('postgres.create_vector_index', False),
            'vector_index_type': self.get('postgres.vector_index_type', 'ivfflat'),
            'vector_index_options': self.get('postgres.vector_index_options', {})
        }