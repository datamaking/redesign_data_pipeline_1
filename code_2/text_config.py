"""
Text file data source configuration.
"""
from typing import Dict, Any, Optional
from ..base_config import BaseConfig

class TextSourceConfig(BaseConfig):
    """Configuration for text file data source."""
    
    def __init__(self, config_path: Optional[str] = None):
        super().__init__(config_path)
        
        # Set default text-specific configurations
        self._set_defaults()
        
    def _set_defaults(self) -> None:
        """Set default values for text file configuration."""
        defaults = {
            'text': {
                'path': 'data/input/*.txt',
                'recursive': True,
                'format': 'text',
                'encoding': 'utf-8',
                'include_path': True,
                'include_filename': True,
                'include_modification_time': True,
                'max_file_size': 0,  # 0 means no limit
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
                
    def get_file_path(self) -> str:
        """Get file path pattern."""
        return self.get('text.path', 'data/input/*.txt')
        
    def is_recursive(self) -> bool:
        """Check if file search should be recursive."""
        return self.get('text.recursive', True)
        
    def get_encoding(self) -> str:
        """Get file encoding."""
        return self.get('text.encoding', 'utf-8')
        
    def get_batch_size(self) -> int:
        """Get batch size for reading files."""
        return self.get('text.batch_size', 1000)