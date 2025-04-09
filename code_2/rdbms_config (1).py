"""
RDBMS data target configuration.
"""
from typing import Dict, Any, Optional
from ..base_config import BaseConfig

class RdbmsTargetConfig(BaseConfig):
    """Configuration for RDBMS data target."""
    
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
                'mode': 'append',  # Options: append, overwrite, ignore, error
                'batch_size': 1000,
                'isolation_level': 'READ_COMMITTED',  # Options: READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE
                'truncate': False,
                'create_table_options': '',
                'create_table_column_types': {},
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
        url = self.get('rdbms.url')
        if not url:
            driver_prefix = self.get('rdbms.driver', 'com.mysql.jdbc.Driver')
            if 'mysql' in driver_prefix.lower():
                jdbc_prefix = 'mysql'
            elif 'postgresql' in driver_prefix.lower():
                jdbc_prefix = 'postgresql'
            elif 'sqlserver' in driver_prefix.lower():
                jdbc_prefix = 'sqlserver'
            elif 'oracle' in driver_prefix.lower():
                jdbc_prefix = 'oracle'
            else:
                jdbc_prefix = 'jdbc'
                
            host = self.get('rdbms.host', 'localhost')
            port = self.get('rdbms.port', 3306)
            database = self.get('rdbms.database', 'database')
            url = f"jdbc:{jdbc_prefix}://{host}:{port}/{database}"
            
        return {
            'driver': self.get('rdbms.driver', 'com.mysql.jdbc.Driver'),
            'url': url,
            'user': self.get('rdbms.username', ''),
            'password': self.get('rdbms.password', ''),
            'properties': self.get('rdbms.properties', {})
        }
        
    def get_table(self) -> str:
        """Get table name."""
        return self.get('rdbms.table', '')
        
    def get_mode(self) -> str:
        """Get write mode."""
        return self.get('rdbms.mode', 'append')
        
    def get_batch_size(self) -> int:
        """Get batch size for writing to RDBMS."""
        return self.get('rdbms.batch_size', 1000)
        
    def get_isolation_level(self) -> str:
        """Get transaction isolation level."""
        return self.get('rdbms.isolation_level', 'READ_COMMITTED')
        
    def should_truncate(self) -> bool:
        """Check if table should be truncated before writing."""
        return self.get('rdbms.truncate', False)
        
    def get_create_table_options(self) -> str:
        """Get options for table creation."""
        return self.get('rdbms.create_table_options', '')
        
    def get_create_table_column_types(self) -> Dict[str, str]:
        """Get column type mappings for table creation."""
        return self.get('rdbms.create_table_column_types', {})