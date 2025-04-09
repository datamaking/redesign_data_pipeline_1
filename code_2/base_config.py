"""
Base configuration module for the PySpark NLP pipeline.
"""
import os
import yaml
from typing import Dict, Any, Optional
import logging

class BaseConfig:
    """Base configuration class that loads config from YAML files or environment variables."""
    
    _instance = None  # Singleton pattern
    
    def __new__(cls, config_path: Optional[str] = None):
        if cls._instance is None:
            cls._instance = super(BaseConfig, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config_path: Optional[str] = None):
        if self._initialized:
            return
            
        self.config_path = config_path
        self.config: Dict[str, Any] = {}
        
        # Load configuration
        if config_path and os.path.exists(config_path):
            self._load_from_yaml(config_path)
        else:
            self._load_from_env()
            
        self._initialized = True
    
    def _load_from_yaml(self, config_path: str) -> None:
        """Load configuration from a YAML file."""
        try:
            with open(config_path, 'r') as file:
                self.config = yaml.safe_load(file)
            logging.info(f"Configuration loaded from {config_path}")
        except Exception as e:
            logging.error(f"Error loading configuration from {config_path}: {str(e)}")
            raise
    
    def _load_from_env(self) -> None:
        """Load configuration from environment variables."""
        # This is a placeholder. Implement according to your environment variable naming convention
        self.config = {key: value for key, value in os.environ.items() if key.startswith('NLP_PIPELINE_')}
        logging.info("Configuration loaded from environment variables")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value by key."""
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
                
        return value
    
    def set(self, key: str, value: Any) -> None:
        """Set a configuration value."""
        keys = key.split('.')
        config = self.config
        
        for i, k in enumerate(keys[:-1]):
            if k not in config:
                config[k] = {}
            config = config[k]
            
        config[keys[-1]] = value
        
    def save(self, config_path: Optional[str] = None) -> None:
        """Save the configuration to a YAML file."""
        path = config_path or self.config_path
        if not path:
            logging.warning("No config path specified, configuration not saved")
            return
            
        try:
            with open(path, 'w') as file:
                yaml.dump(self.config, file, default_flow_style=False)
            logging.info(f"Configuration saved to {path}")
        except Exception as e:
            logging.error(f"Error saving configuration to {path}: {str(e)}")
            raise