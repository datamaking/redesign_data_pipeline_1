"""
Hive data source configuration.
"""
from typing import Dict, Any, Optional
from ..base_config import BaseConfig

class HiveSourceConfig(BaseConfig):
    """Configuration for Hive data source."""
    
    def __init__(self, config_path: Optional[str] = None):
        super().__init__(config_path)
        
        # Set default Hive-specific configurations
        self._set_defaults()
        
    def _set_defaults(self) -> None:
        """Set default values for Hive configuration."""
        defaults = {
            'hive': {
                'host': 'localhost',
                'port': 10000,
                'database': 'default',
                'auth_mechanism': 'PLAIN',
                'username': '',
                'password': '',
                'timeout': 30,
                'batch_size': 1000
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
        """Get connection parameters for Hive."""
        return {
            'host': self.get('hive.host'),
            'port': self.get('hive.port'),
            'database': self.get('hive.database'),
            'auth_mechanism': self.get('hive.auth_mechanism'),
            'username': self.get('hive.username'),
            'password': self.get('hive.password'),
            'timeout': self.get('hive.timeout')
        }
        
    def get_batch_size(self) -> int:
        """Get batch size for reading from Hive."""
        return self.get('hive.batch_size', 1000)