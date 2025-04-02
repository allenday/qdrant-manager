"""
Configuration management for Solr Manager using YAML.
"""

import os
import sys
import yaml
import logging
from pathlib import Path
import appdirs
import shutil  # Import shutil for file copying
from typing import Dict, Any, Optional

__all__ = ['load_config', 'get_profiles', 'update_config', 'create_default_config', 'get_config_dir']

CONFIG_FILENAME = "config.yaml"
SAMPLE_CONFIG_FILENAME = "config.yaml.sample" # Name of the sample file in the project root
DEFAULT_PROFILE = "default"

logger = logging.getLogger(__name__)

def get_config_dir() -> Path:
    """Get the application configuration directory based on the OS."""
    # Use 'solr-manager' as the application name
    config_path = Path(appdirs.user_config_dir("solr-manager"))
    # Add debug print to stderr
    print(f"DEBUG: config.py attempting to use config directory: {config_path}", file=sys.stderr)
    return config_path

def get_config_file() -> Path:
    """Get the full configuration file path."""
    return get_config_dir() / CONFIG_FILENAME

def ensure_config_dir() -> Path:
    """Ensure the configuration directory exists."""
    config_dir = get_config_dir()
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir

def find_sample_config() -> Optional[Path]:
    """Try to find the config.yaml.sample file, searching upwards from this file's location."""
    current_dir = Path(__file__).parent
    # Look in the parent directory (project root relative to solr_manager/)
    project_root = current_dir.parent 
    sample_path = project_root / SAMPLE_CONFIG_FILENAME
    if sample_path.is_file():
        return sample_path
    
    # Fallback: Look in the current working directory (less reliable)
    if (Path.cwd() / SAMPLE_CONFIG_FILENAME).is_file():
        return Path.cwd() / SAMPLE_CONFIG_FILENAME
        
    logger.warning(f"Could not automatically locate {SAMPLE_CONFIG_FILENAME}")
    return None

def create_default_config() -> Optional[Path]:
    """
    Copies the sample configuration file to the user's config directory
    if the main config file doesn't exist.
    """
    config_file = get_config_file()
    
    if not config_file.exists():
        config_dir = ensure_config_dir()
        sample_config_path = find_sample_config()
        
        if sample_config_path:
            try:
                shutil.copy(sample_config_path, config_file)
                print(f"Sample configuration copied to {config_file}")
                print("Please edit this file with your Solr connection details.")
                return config_file
            except Exception as e:
                logger.error(f"Failed to copy sample configuration: {e}")
                return None
        else:
            # If sample can't be found, create a minimal structure
            # This is less ideal as it lacks comments/examples
            print(f"Could not find {SAMPLE_CONFIG_FILENAME}. Creating a minimal config.")
            minimal_config = {
                DEFAULT_PROFILE: {
                    "connection": {
                        "solr_url": "http://localhost:8983/solr",
                        "collection": "solr_manager_test"
                    }
                },
                "production": {
                     "connection": {
                        "solr_url": "REPLACE_WITH_PROD_URL",
                        "collection": "REPLACE_WITH_PROD_COLLECTION"
                    }
                }
            }
            try:
                with open(config_file, 'w') as f:
                    yaml.dump(minimal_config, f, default_flow_style=False, sort_keys=False)
                print(f"Created minimal configuration file at {config_file}")
                print("Please edit this file with your Solr connection details.")
                return config_file
            except Exception as e:
                 logger.error(f"Failed to create minimal configuration file: {e}")
                 return None
                 
    # Config file already exists
    return config_file

def load_config(profile: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Load connection configuration from the config file for a specific profile.
    
    Args:
        profile: The profile name to load. If None, uses the default profile.
        
    Returns:
        dict: The connection configuration dictionary for the profile, or None if loading fails.
    """
    config_file = get_config_file()
    
    if not config_file.exists():
        if create_default_config():
             # Ask user to edit the newly created file and exit
             print("Default configuration file created. Please edit it and run the command again.")
        else:
             print("Failed to create a default configuration file.")
        sys.exit(1)
    
    try:
        with open(config_file, 'r') as f:
            full_config = yaml.safe_load(f)
            if not isinstance(full_config, dict):
                raise yaml.YAMLError("Configuration file is not a valid YAML dictionary.")
    except FileNotFoundError:
         logger.error(f"Configuration file not found at {config_file}")
         return None
    except yaml.YAMLError as e:
        logger.error(f"Error parsing configuration file {config_file}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error loading configuration file {config_file}: {e}")
        return None
        
    if not full_config:
        logger.error(f"Configuration file {config_file} is empty or invalid.")
        return None

    # Use the specified profile or the default
    profile_name = profile or DEFAULT_PROFILE
    
    if profile_name not in full_config:
        available = ', '.join(full_config.keys())
        logger.error(f"Profile '{profile_name}' not found in {config_file}.")
        logger.error(f"Available profiles: {available}")
        return None
        
    profile_data = full_config.get(profile_name, {})
    if not isinstance(profile_data, dict):
         logger.error(f"Profile '{profile_name}' in {config_file} is not a valid dictionary.")
         return None

    connection_config = profile_data.get("connection")
    if not isinstance(connection_config, dict):
         logger.error(f"Profile '{profile_name}' in {config_file} is missing the 'connection' section or it's invalid.")
         return None

    # Return only the 'connection' dictionary
    return connection_config

def get_profiles() -> list[str]:
    """Get a list of available profile names from the configuration file."""
    config_file = get_config_file()
    
    if not config_file.exists():
        return [DEFAULT_PROFILE] # Indicate default is available even if file doesn't exist
    
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
            if isinstance(config, dict):
                return list(config.keys())
            else:
                logger.warning(f"Configuration file {config_file} is not a valid dictionary. Cannot list profiles.")
                return [DEFAULT_PROFILE]
    except Exception as e:
        logger.warning(f"Error reading profiles from {config_file}: {e}")
        return [DEFAULT_PROFILE]

def update_config(profile: str, key: str, value: Any):
    """
    Update a configuration value within the 'connection' section of a profile.
    
    Args:
        profile: The profile name to update.
        key: The key within the 'connection' section to update.
        value: The new value.
    """
    config_file = get_config_file()
    
    if not config_file.exists():
        if not create_default_config():
             logger.error("Cannot update configuration: Failed to create default config file.")
             return
             
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f) or {} # Load or initialize as empty dict
            if not isinstance(config, dict):
                 raise yaml.YAMLError("Config file is not a valid dictionary.")
    except Exception as e:
        logger.error(f"Error loading configuration file for update: {e}")
        return
    
    # Ensure the profile exists
    if profile not in config or not isinstance(config[profile], dict):
        config[profile] = {}
    
    # Ensure the 'connection' section exists within the profile
    if "connection" not in config[profile] or not isinstance(config[profile]["connection"], dict):
        config[profile]["connection"] = {}
    
    # Update the value within the 'connection' section
    config[profile]["connection"][key] = value
    
    # Write the updated config
    try:
        with open(config_file, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        logger.info(f"Updated '{key}' in profile '{profile}' connection settings.")
    except Exception as e:
        logger.error(f"Failed to write updated configuration to {config_file}: {e}")