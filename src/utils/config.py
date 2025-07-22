"""
Configuration management module for the ETL pipeline.
Handles loading and validation of configuration from YAML files.
"""

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path


class ConfigManager:
    """Manages configuration loading and validation for the ETL pipeline."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_path: Path to the configuration file. If None, looks for config.yaml in current directory.
        """
        self.config_path = config_path or "config.yaml"
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Returns:
            Dictionary containing configuration settings.
            
        Raises:
            FileNotFoundError: If config file doesn't exist.
            yaml.YAMLError: If config file is invalid YAML.
        """
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
            return config
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Invalid YAML in configuration file: {e}")
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration settings."""
        return self.config.get('database', {})
    
    def get_data_sources_config(self) -> Dict[str, Any]:
        """Get data sources configuration settings."""
        return self.config.get('data_sources', {})
    
    def get_currency_api_config(self) -> Dict[str, Any]:
        """Get currency API configuration settings."""
        return self.config.get('currency_api', {})
    
    def get_processing_config(self) -> Dict[str, Any]:
        """Get data processing configuration settings."""
        return self.config.get('processing', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration settings."""
        return self.config.get('logging', {})
    
    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration settings."""
        return self.config.get('spark', {})
    
    def get_validation_config(self) -> Dict[str, Any]:
        """Get data validation configuration settings."""
        return self.config.get('validation', {})
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value by key.
        
        Args:
            key: Configuration key (supports dot notation like 'database.server')
            default: Default value if key doesn't exist
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def validate_config(self) -> bool:
        """
        Validate that all required configuration sections are present.
        
        Returns:
            True if configuration is valid, False otherwise.
        """
        required_sections = [
            'database', 'data_sources', 'currency_api', 
            'processing', 'logging', 'spark', 'validation'
        ]
        
        for section in required_sections:
            if section not in self.config:
                print(f"Warning: Missing required configuration section: {section}")
                return False
        
        return True


# Global configuration instance
config_manager = None


def get_config() -> ConfigManager:
    """
    Get the global configuration manager instance.
    
    Returns:
        ConfigManager instance
    """
    global config_manager
    if config_manager is None:
        config_manager = ConfigManager()
    return config_manager


def reload_config() -> ConfigManager:
    """
    Reload configuration from file.
    
    Returns:
        Updated ConfigManager instance
    """
    global config_manager
    config_manager = ConfigManager()
    return config_manager 