"""
Hive data target configuration.
"""
from typing import Dict, Any, Optional
from ..base_config import BaseConfig

class HiveTargetConfig(BaseConfig):
    """Configuration for Hive data target."""
    
    def __init__(self, config_path: Optional[str] = None):
        super().__init__(config_path)
        
        # Set default Hive-specific configurations
        self._set_defaults()
        
    def _set_defaults(self) -> None:
        """Set default values for Hive configuration."""
        defaults = {
            'hive': {
                'database': 'default',
                'table': '',
                'partition_by': [],
                'mode': 'append',  # Options: append, overwrite, ignore, error
                'format': 'parquet',  # Options: parquet, orc, avro, csv, json
                'compression': 'snappy',  # Options: none, snappy, gzip, lzo
                'batch_size': 10000,
                'options': {}
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
                
    def get_database(self) -> str:
        """Get database name."""
        return self.get('hive.database', 'default')
        
    def get_table(self) -> str:
        """Get table name."""
        return self.get('hive.table', '')
        
    def get_partition_by(self) -> list:
        """Get partition columns."""
        return self.get('hive.partition_by', [])
        
    def get_mode(self) -> str:
        """Get write mode."""
        return self.get('hive.mode', 'append')
        
    def get_format(self) -> str:
        """Get storage format."""
        return self.get('hive.format', 'parquet')
        
    def get_compression(self) -> str:
        """Get compression codec."""
        return self.get('hive.compression', 'snappy')
        
    def get_batch_size(self) -> int:
        """Get batch size for writing to Hive."""
        return self.get('hive.batch_size', 10000)
        
    def get_options(self) -> Dict[str, Any]:
        """Get additional options for Hive writes."""
        return self.get('hive.options', {})