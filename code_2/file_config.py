"""
File data target configuration.
"""
from typing import Dict, Any, Optional, List
from ..base_config import BaseConfig

class FileTargetConfig(BaseConfig):
    """Configuration for file data target."""
    
    def __init__(self, config_path: Optional[str] = None):
        super().__init__(config_path)
        
        # Set default file-specific configurations
        self._set_defaults()
        
    def _set_defaults(self) -> None:
        """Set default values for file configuration."""
        defaults = {
            'file': {
                'path': 'data/output/',
                'format': 'parquet',  # Options: parquet, orc, avro, csv, json, text
                'mode': 'overwrite',  # Options: append, overwrite, ignore, error
                'partition_by': [],
                'compression': 'snappy',  # Options: none, snappy, gzip, bzip2, lz4, zstd
                'header': True,  # For CSV
                'delimiter': ',',  # For CSV
                'quote': '"',  # For CSV
                'escape': '\\',  # For CSV
                'encoding': 'UTF-8',
                'date_format': 'yyyy-MM-dd',
                'timestamp_format': 'yyyy-MM-dd HH:mm:ss',
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
                
    def get_path(self) -> str:
        """Get output file path."""
        return self.get('file.path', 'data/output/')
        
    def get_format(self) -> str:
        """Get file format."""
        return self.get('file.format', 'parquet')
        
    def get_mode(self) -> str:
        """Get write mode."""
        return self.get('file.mode', 'overwrite')
        
    def get_partition_by(self) -> List[str]:
        """Get partition columns."""
        return self.get('file.partition_by', [])
        
    def get_compression(self) -> str:
        """Get compression codec."""
        return self.get('file.compression', 'sn  -> str:
        """Get compression codec."""
        return self.get('file.compression', 'snappy')
        
    def get_csv_options(self) -> Dict[str, Any]:
        """Get CSV-specific options."""
        return {
            'header': self.get('file.header', True),
            'delimiter': self.get('file.delimiter', ','),
            'quote': self.get('file.quote', '"'),
            'escape': self.get('file.escape', '\\'),
            'encoding': self.get('file.encoding', 'UTF-8')
        }
        
    def get_date_format(self) -> str:
        """Get date format."""
        return self.get('file.date_format', 'yyyy-MM-dd')
        
    def get_timestamp_format(self) -> str:
        """Get timestamp format."""
        return self.get('file.timestamp_format', 'yyyy-MM-dd HH:mm:ss')
        
    def get_batch_size(self) -> int:
        """Get batch size for writing to files."""
        return self.get('file.batch_size', 10000)
        
    def get_options(self) -> Dict[str, Any]:
        """Get additional options for file writes."""
        return self.get('file.options', {})