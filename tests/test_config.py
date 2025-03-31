"""
Tests for the configuration management module.
"""
import pytest
import tempfile
import yaml
import os
from pathlib import Path
from unittest.mock import patch

from qdrant_manager.config import (
    get_config_dir, 
    get_config_file, 
    ensure_config_dir, 
    create_default_config, 
    _convert_config, 
    load_config, 
    get_profiles, 
    update_config,
    CONFIG_FILENAME,
    DEFAULT_PROFILE
)

def test_config_dir():
    """Test that config directory is a Path object."""
    config_dir = get_config_dir()
    assert isinstance(config_dir, Path)
    
def test_config_filename():
    """Test that config filename is defined."""
    assert isinstance(CONFIG_FILENAME, str)
    assert CONFIG_FILENAME.endswith('.yaml')

def test_get_config_file():
    """Test getting config file path."""
    config_file = get_config_file()
    assert isinstance(config_file, Path)
    assert config_file.name == CONFIG_FILENAME
    assert config_file.parent == get_config_dir()

def test_ensure_config_dir():
    """Test ensuring config directory exists."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create a temporary config path
        temp_path = Path(tmp_dir) / "test_config_dir"
        
        # Make sure it doesn't exist yet
        assert not temp_path.exists()
        
        # Patch get_config_dir to return our temp path
        with patch('qdrant_manager.config.get_config_dir', return_value=temp_path):
            # Call function to ensure directory
            result = ensure_config_dir()
            
            # Check results
            assert result == temp_path
            assert temp_path.exists()
            assert temp_path.is_dir()

def test_create_default_config():
    """Test creating default configuration file."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create a temporary config path
        temp_path = Path(tmp_dir) / "test_config_dir"
        temp_file = temp_path / CONFIG_FILENAME
        
        # Patch functions to use temp directory
        with patch('qdrant_manager.config.get_config_dir', return_value=temp_path):
            with patch('qdrant_manager.config.get_config_file', return_value=temp_file):
                # Create default config
                result = create_default_config()
                
                # Check results
                assert result == temp_file
                assert temp_file.exists()
                
                # Verify file content
                with open(temp_file, 'r') as f:
                    config = yaml.safe_load(f)
                    
                    # Check if default profile exists
                    assert DEFAULT_PROFILE in config
                    
                    # Check if connection section exists
                    assert "connection" in config[DEFAULT_PROFILE]
                    assert "url" in config[DEFAULT_PROFILE]["connection"]
                    
                    # Check if vectors section exists
                    assert "vectors" in config[DEFAULT_PROFILE]
                    assert "size" in config[DEFAULT_PROFILE]["vectors"]
                    
                    # Check if production profile exists
                    assert "production" in config

def test_convert_config():
    """Test configuration conversion function."""
    # Test with empty config
    assert _convert_config(None) == {}
    assert _convert_config({}) == {}
    
    # Test with minimal config
    minimal_config = {
        "connection": {
            "url": "test-url",
            "port": 1234
        }
    }
    converted = _convert_config(minimal_config)
    assert converted["url"] == "test-url"
    assert converted["port"] == 1234
    
    # Test with full config
    full_config = {
        "connection": {
            "url": "full-url",
            "port": 5678,
            "api_key": "test-key",
            "collection": "test-collection"
        },
        "vectors": {
            "size": 512,
            "distance": "euclid",
            "indexing_threshold": 100
        },
        "payload_indices": [
            {"field": "test_field", "type": "keyword"}
        ]
    }
    converted = _convert_config(full_config)
    assert converted["url"] == "full-url"
    assert converted["port"] == 5678
    assert converted["api_key"] == "test-key"
    assert converted["collection"] == "test-collection"
    assert converted["vector_size"] == 512
    assert converted["distance"] == "euclid"
    assert converted["indexing_threshold"] == 100
    assert len(converted["payload_indices"]) == 1
    assert converted["payload_indices"][0]["field"] == "test_field"

def test_get_profiles():
    """Test getting available profiles."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create a temporary config file
        temp_path = Path(tmp_dir) / "test_config_dir"
        temp_file = temp_path / CONFIG_FILENAME
        temp_path.mkdir(exist_ok=True)
        
        # Create a test config file
        test_config = {
            "profile1": {},
            "profile2": {}
        }
        with open(temp_file, 'w') as f:
            yaml.dump(test_config, f)
        
        # Patch get_config_file to return our temp file
        with patch('qdrant_manager.config.get_config_file', return_value=temp_file):
            profiles = get_profiles()
            assert len(profiles) == 2
            assert "profile1" in profiles
            assert "profile2" in profiles

def test_get_profiles_no_file():
    """Test getting profiles when config file doesn't exist."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create a temp path for a file that doesn't exist
        temp_path = Path(tmp_dir) / "nonexistent_dir"
        temp_file = temp_path / CONFIG_FILENAME
        
        # Patch get_config_file to return our temp file
        with patch('qdrant_manager.config.get_config_file', return_value=temp_file):
            profiles = get_profiles()
            assert len(profiles) == 1
            assert profiles[0] == DEFAULT_PROFILE

def test_get_profiles_with_yaml_error():
    """Test getting profiles with a YAML parsing error."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Create a temp directory and file
        temp_path = Path(tmp_dir) / "test_config_dir"
        temp_file = temp_path / CONFIG_FILENAME
        temp_path.mkdir(exist_ok=True)
        
        # Create an invalid YAML file
        with open(temp_file, 'w') as f:
            f.write("this is not valid yaml: [\n")
        
        # Patch get_config_file to return our temp file
        with patch('qdrant_manager.config.get_config_file', return_value=temp_file):
            # Function should handle the error and return default profile
            profiles = get_profiles()
            assert len(profiles) == 1
            assert profiles[0] == DEFAULT_PROFILE

def test_update_config():
    """Test updating configuration values."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Set up temp directory
        temp_path = Path(tmp_dir) / "test_config_dir"
        temp_file = temp_path / CONFIG_FILENAME
        temp_path.mkdir(exist_ok=True)
        
        # Create a test config file
        test_config = {
            "default": {
                "connection": {
                    "url": "original-url"
                }
            }
        }
        with open(temp_file, 'w') as f:
            yaml.dump(test_config, f)
        
        # Patch functions to use our temp file
        with patch('qdrant_manager.config.get_config_file', return_value=temp_file):
            with patch('qdrant_manager.config.create_default_config', return_value=temp_file):
                # Update existing value
                update_config("default", "connection", "url", "updated-url")
                
                # Read the updated config
                with open(temp_file, 'r') as f:
                    updated_config = yaml.safe_load(f)
                
                assert updated_config["default"]["connection"]["url"] == "updated-url"
                
                # Update with new profile
                update_config("new_profile", "connection", "url", "new-profile-url")
                
                # Read the updated config
                with open(temp_file, 'r') as f:
                    updated_config = yaml.safe_load(f)
                
                assert "new_profile" in updated_config
                assert updated_config["new_profile"]["connection"]["url"] == "new-profile-url"
                
                # Update with new section
                update_config("default", "new_section", "new_key", "new_value")
                
                # Read the updated config
                with open(temp_file, 'r') as f:
                    updated_config = yaml.safe_load(f)
                
                assert "new_section" in updated_config["default"]
                assert updated_config["default"]["new_section"]["new_key"] == "new_value"

def test_update_config_yaml_error():
    """Test handling of YAML errors in config file when updating."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Set up temp directory
        temp_path = Path(tmp_dir) / "test_config_dir"
        temp_file = temp_path / CONFIG_FILENAME
        temp_path.mkdir(exist_ok=True)
        
        # Create an invalid YAML file
        with open(temp_file, 'w') as f:
            f.write("this is not valid yaml: [\n")
        
        # Patch functions to use our temp file
        with patch('qdrant_manager.config.get_config_file', return_value=temp_file):
            # Updating should call sys.exit due to YAML error
            with patch('sys.exit') as mock_exit:
                try:
                    update_config("default", "connection", "url", "updated-url")
                except:
                    # Exception expected, don't fail test
                    pass
                
                # Verify sys.exit would be called
                mock_exit.assert_called()

def test_update_config_no_file():
    """Test updating configuration when file doesn't exist."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Set up temp directory for a file that doesn't exist
        temp_path = Path(tmp_dir) / "nonexistent_dir"
        temp_file = temp_path / CONFIG_FILENAME
        
        # Ensure the file doesn't exist
        assert not temp_file.exists()
        
        # Patch functions to use our temp file
        with patch('qdrant_manager.config.get_config_file', return_value=temp_file):
            # When file doesn't exist, create_default_config should be called
            with patch('qdrant_manager.config.create_default_config', return_value=temp_file) as mock_create:
                # Mock the file open operations so we don't need to actually create files
                with patch('builtins.open') as mock_open, \
                     patch('yaml.safe_load') as mock_safe_load, \
                     patch('yaml.dump') as mock_dump:
                    
                    # Set up mock to return dummy config from safe_load
                    mock_safe_load.return_value = {"default": {"connection": {}}}
                    
                    # Call the function
                    update_config("default", "connection", "url", "test-url")
                    
                    # Verify create_default_config was called
                    mock_create.assert_called_once()
                    
                    # Verify yaml.dump was called to write the config
                    assert mock_dump.called

def test_load_config_new_file_exit():
    """Test load_config creating new file and exiting."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Set up temp directory for non-existent file
        temp_path = Path(tmp_dir) / "test_config_dir"
        temp_file = temp_path / CONFIG_FILENAME
        
        # Ensure file doesn't exist yet
        assert not temp_file.exists()
        
        # Patch functions to use our temp file
        with patch('qdrant_manager.config.get_config_file', return_value=temp_file):
            # Should call create_default_config then exit
            with patch('qdrant_manager.config.create_default_config', return_value=temp_file) as mock_create:
                with patch('sys.exit') as mock_exit:
                    try:
                        load_config()
                    except:
                        # Exception expected, don't fail test
                        pass
                    
                    # Verify create_default_config was called
                    mock_create.assert_called_once()
                    
                    # Verify sys.exit was called with 1
                    mock_exit.assert_called_with(1)

def test_load_config_with_existing_profile():
    """Test loading config with an existing profile."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Set up temp directory
        temp_path = Path(tmp_dir) / "test_config_dir"
        temp_file = temp_path / CONFIG_FILENAME
        temp_path.mkdir(exist_ok=True)
        
        # Create a test config file
        test_config = {
            "default": {
                "connection": {
                    "url": "default-url",
                    "port": 6333,
                    "api_key": "default-key",
                    "collection": "default-collection"
                },
                "vectors": {
                    "size": 256,
                    "distance": "cosine",
                    "indexing_threshold": 0
                }
            },
            "test_profile": {
                "connection": {
                    "url": "test-url",
                    "port": 7000,
                    "api_key": "test-key",
                    "collection": "test-collection"
                },
                "vectors": {
                    "size": 512,
                    "distance": "euclid",
                    "indexing_threshold": 100
                },
                "payload_indices": [
                    {"field": "test_field", "type": "keyword"}
                ]
            }
        }
        with open(temp_file, 'w') as f:
            yaml.dump(test_config, f)
        
        # Patch functions to use our temp file
        with patch('qdrant_manager.config.get_config_file', return_value=temp_file):
            with patch('qdrant_manager.config.create_default_config', return_value=temp_file):
                # Mock sys.exit to prevent exit
                with patch('sys.exit'):
                    # Test loading with default profile
                    config = load_config()
                    assert config["url"] == "default-url"
                    assert config["port"] == 6333
                    assert config["vector_size"] == 256
                    assert config["distance"] == "cosine"
                    
                    # Test loading with specified profile
                    config = load_config("test_profile")
                    assert config["url"] == "test-url"
                    assert config["port"] == 7000
                    assert config["vector_size"] == 512
                    assert config["distance"] == "euclid"
                    assert len(config["payload_indices"]) == 1
                    assert config["payload_indices"][0]["field"] == "test_field"

def test_load_config_nonexistent_profile():
    """Test loading config with a profile that doesn't exist."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Set up temp directory
        temp_path = Path(tmp_dir) / "test_config_dir"
        temp_file = temp_path / CONFIG_FILENAME
        temp_path.mkdir(exist_ok=True)
        
        # Create a test config file with only default profile
        test_config = {
            "default": {
                "connection": {
                    "url": "default-url",
                    "port": 6333
                }
            }
        }
        with open(temp_file, 'w') as f:
            yaml.dump(test_config, f)
        
        # Patch functions to use our temp file
        with patch('qdrant_manager.config.get_config_file', return_value=temp_file):
            # Try to load a non-existent profile, should call sys.exit
            with patch('sys.exit') as mock_exit:
                try:
                    load_config("nonexistent_profile")
                except:
                    # We expect an exception here, don't let it fail the test
                    pass
                # Verify sys.exit would be called
                mock_exit.assert_called()

def test_load_config_yaml_error():
    """Test handling of YAML errors in config file."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Set up temp directory
        temp_path = Path(tmp_dir) / "test_config_dir"
        temp_file = temp_path / CONFIG_FILENAME
        temp_path.mkdir(exist_ok=True)
        
        # Create an invalid YAML file
        with open(temp_file, 'w') as f:
            f.write("this is not valid yaml: [\n")
        
        # Patch functions to use our temp file
        with patch('qdrant_manager.config.get_config_file', return_value=temp_file):
            # Loading should call sys.exit due to YAML error
            with patch('sys.exit') as mock_exit:
                try:
                    load_config()
                except:
                    # We expect an exception here, don't let it fail the test
                    pass
                # Verify sys.exit would be called
                mock_exit.assert_called()