"""
Configuration management for Qdrant Manager using YAML.
"""

import os
import sys
import yaml
from pathlib import Path
import appdirs

__all__ = ['load_config', 'get_profiles', 'update_config', 'create_default_config', 'get_config_dir']

CONFIG_FILENAME = "config.yaml"
DEFAULT_PROFILE = "default"

def get_config_dir():
    """Get the configuration directory."""
    return Path(appdirs.user_config_dir("qdrant-manager"))

def get_config_file():
    """Get the configuration file path."""
    return get_config_dir() / CONFIG_FILENAME

def ensure_config_dir():
    """Ensure the configuration directory exists."""
    config_dir = get_config_dir()
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir

def create_default_config():
    """Create a default configuration file if it doesn't exist."""
    config_file = get_config_file()
    
    if not config_file.exists():
        ensure_config_dir()
        
        # Default configuration
        config = {
            DEFAULT_PROFILE: {
                "connection": {
                    "url": "localhost",
                    "port": 6333,
                    "api_key": "",
                    "collection": "my-collection"
                },
                "vectors": {
                    "size": 256,
                    "distance": "cosine",
                    "indexing_threshold": 0
                },
                "payload_indices": [
                    {"field": "example_field", "type": "keyword"},
                ]
            },
            "production": {
                "connection": {
                    "url": "your-qdrant-instance.region.cloud.qdrant.io",
                    "port": 6333,
                    "api_key": "your-api-key-here",
                    "collection": "production-collection"
                },
                "vectors": {
                    "size": 1536,
                    "distance": "cosine",
                    "indexing_threshold": 1000
                },
                "payload_indices": [
                    {"field": "category", "type": "keyword"},
                    {"field": "created_at", "type": "datetime"}
                ]
            }
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
            
        print(f"Created default configuration file at {config_file}")
        print("Please edit this file with your Qdrant connection details.")
    
    return config_file

def _convert_config(profile_config):
    """Convert the profile configuration to the expected format."""
    if not profile_config:
        return {}
        
    # Extract connection details
    connection = profile_config.get("connection", {})
    
    # Extract vector configuration
    vectors = profile_config.get("vectors", {})
    
    # Build the configuration dictionary
    config = {
        "url": connection.get("url"),
        "port": connection.get("port"),
        "api_key": connection.get("api_key"),
        "collection": connection.get("collection"),
        "vector_size": vectors.get("size", 256),
        "distance": vectors.get("distance", "cosine"),
        "indexing_threshold": vectors.get("indexing_threshold", 0),
        "payload_indices": profile_config.get("payload_indices", [])
    }
    
    return config

def load_config(profile=None):
    """
    Load configuration from the config file.
    
    Args:
        profile: The profile to load. If None, use the default profile.
        
    Returns:
        dict: The configuration as a dictionary.
    """
    config_file = get_config_file()
    
    if not config_file.exists():
        config_file = create_default_config()
        # Exit after creating the default config
        print("Please edit the configuration file and run the command again.")
        sys.exit(1)
    
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading configuration file: {e}")
        sys.exit(1)
    
    # Use the specified profile or the default
    section = profile or DEFAULT_PROFILE
    
    if section not in config:
        print(f"Error: Profile '{section}' not found in the configuration file.")
        print(f"Available profiles: {', '.join(config.keys())}")
        sys.exit(1)
    
    # Convert the profile's config to our expected format
    return _convert_config(config[section])

def get_profiles():
    """Get a list of available profiles."""
    config_file = get_config_file()
    
    if not config_file.exists():
        return [DEFAULT_PROFILE]
    
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
            return list(config.keys())
    except Exception:
        return [DEFAULT_PROFILE]

def update_config(profile, section, key, value):
    """Update a configuration value.
    
    Args:
        profile: The profile to update
        section: The section within the profile (connection, vectors, etc.)
        key: The key to update
        value: The new value
    """
    config_file = get_config_file()
    
    if not config_file.exists():
        config_file = create_default_config()
    
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading configuration file: {e}")
        sys.exit(1)
    
    # Make sure the profile exists
    if profile not in config:
        config[profile] = {}
    
    # Make sure the section exists
    if section not in config[profile]:
        config[profile][section] = {}
    
    # Update the value
    config[profile][section][key] = value
    
    # Write the updated config
    with open(config_file, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)