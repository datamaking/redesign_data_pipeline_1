"""
RDBMS data source configuration.
"""
from typing import Dict, Any, Optional, List
from ..base_config import BaseConfig

class RdbmsSourceConfig(BaseConfig):
    """Configuration for RDBMS data source."""
    
    def __init__(self, config_path: Optional[str] = None):
        super().__init__(config_path)
        
        # Set default RDBMS-specific configurations
        self._set_defaults()
        
    def _set_defaults(self) -> None:
        """Set default values for RDBMS configuration."""
        defaults = {
            'rdbms': {
                'driver': 'com.mysql.jdbc.Driver',
                'url': 'jdbc:mysql://localhost:3306/database',
                'host': 'localhost',
                'port': 3306,
                'database': 'database',
                'table': '',
                'username': '',
                'password': '',
                'query': '',
                'fetch_size': 1000,
                'partition_column': '',
                'lower_bound': '',
                'upper_bound': '',
                'num_partitions': 10,
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
        """Get connection parameters for RDBMS."""
        return {
            'driver': self.get('rdbms.driver'),
            'url': self.get('rdbms.url'),
            'host': self.get('rdbms.host'),
            'port': self.get('rdbms.port'),
            'database': self.get('rdbms.database'),
            'user': self.get('rdbms.username'),
            'password': self.get('rdbms.password'),
            'properties': self.get('rdbms.properties', {})
        }
        
    def get_table(self) -> str:
        """Get table name."""
        return self.get('rdbms.table', '')
        
    def get_query(self) -> str:
        """Get SQL query."""
        return self.get('rdbms.query', '')
        
    def get_fetch_size(self) -> int:
        """Get fetch size for reading from RDBMS."""
        return self.get('rdbms.fetch_size', 1000)
        
    def get_partition_info(self) -> Dict[str, Any]:
        """Get partitioning information for RDBMS reads."""
        return {
            'partition_column': self.get('rdbms.partition_column', ''),
            'lower_bound': self.get('rdbms.lower_bound', ''),
            'upper_bound': self.get('rdbms.upper_bound', ''),
            'num_partitions': self.get('rdbms.num_partitions', 10)
        }