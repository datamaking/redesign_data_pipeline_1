"""
PostgreSQL data source configuration.
"""
from typing import Dict, Any, Optional, List
from ..base_config import BaseConfig

class PostgresSourceConfig(BaseConfig):
    """Configuration for PostgreSQL data source."""
    
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
                'query': '',
                'fetch_size': 1000,
                'partition_column': '',
                'lower_bound': '',
                'upper_bound': '',
                'num_partitions': 10,
                'ssl_mode': 'prefer',  # Options: disable, allow, prefer, require, verify-ca, verify-full
                'ssl_root_cert': '',
                'ssl_cert': '',
                'ssl_key': '',
                'properties': {}
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
        
    def get_query(self) -> str:
        """Get SQL query."""
        return self.get('postgres.query', '')
        
    def get_fetch_size(self) -> int:
        """Get fetch size for reading from PostgreSQL."""
        return self.get('postgres.fetch_size', 1000)
        
    def get_partition_info(self) -> Dict[str, Any]:
        """Get partitioning information for PostgreSQL reads."""
        return {
            'partition_column': self.get('postgres.partition_column', ''),
            'lower_bound': self.get('postgres.lower_bound', ''),
            'upper_bound': self.get('postgres.upper_bound', ''),
            'num_partitions': self.get('postgres.num_partitions', 10)
        }