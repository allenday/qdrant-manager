"""Tests for utility functions."""
import json
import pytest
from unittest.mock import patch, MagicMock, mock_open
import os
import yaml

from qdrant_manager.utils import load_configuration, initialize_qdrant_client
from qdrant_manager.config import create_default_config, get_config_dir, get_profiles, update_config

# Test cases for load_configuration
@pytest.fixture
def mock_args():
    """Fixture for mock arguments."""
    args = MagicMock()
    args.profile = None
    args.url = None
    args.port = None
    args.api_key = None
    args.collection = None
    return args

# Define standard mock config data for tests
MOCK_DEFAULT_CONFIG = {
    "url": "http://localhost_default",
    "port": 6333,
    "api_key": "default_key",
    "collection": "default_collection"
}
MOCK_PROFILE_CONFIG = {
    "url": "http://profile.host",
    "port": 1234,
    "api_key": "profile_key",
    "collection": "profile_collection"
}

@patch('qdrant_manager.utils.load_config') # Patch load_config where it's called
def test_load_configuration_default(mock_load_config_call, mock_args):
    """Test loading default configuration."""
    mock_load_config_call.return_value = MOCK_DEFAULT_CONFIG.copy()
    config = load_configuration(mock_args)
    assert config["url"] == "http://localhost_default"
    assert config["port"] == 6333
    assert config["api_key"] == "default_key"
    assert config["collection"] == "default_collection"
    mock_load_config_call.assert_called_once_with() # Called with no args for default

@patch('qdrant_manager.utils.load_config')
def test_load_configuration_profile(mock_load_config_call, mock_args):
    """Test loading configuration from a profile."""
    mock_args.profile = "myprofile"
    mock_load_config_call.return_value = MOCK_PROFILE_CONFIG.copy()
    config = load_configuration(mock_args)
    assert config["url"] == "http://profile.host"
    assert config["port"] == 1234
    assert config["api_key"] == "profile_key"
    assert config["collection"] == "profile_collection"
    mock_load_config_call.assert_called_once_with("myprofile")

@patch('qdrant_manager.utils.load_config')
def test_load_configuration_override_args(mock_load_config_call, mock_args):
    """Test overriding config with command-line arguments."""
    mock_load_config_call.return_value = MOCK_DEFAULT_CONFIG.copy()
    mock_args.url = "http://cmd.line"
    mock_args.port = 8888
    mock_args.api_key = "cmd_key"
    mock_args.collection = "cmd_collection"
    config = load_configuration(mock_args)
    assert config["url"] == "http://cmd.line"
    assert config["port"] == 8888
    assert config["api_key"] == "cmd_key"
    assert config["collection"] == "cmd_collection"
    mock_load_config_call.assert_called_once_with() # Called default config

@patch('qdrant_manager.utils.load_config')
def test_load_configuration_profile_override_args(mock_load_config_call, mock_args):
    """Test overriding profile config with command-line arguments."""
    mock_args.profile = "myprofile"
    mock_load_config_call.return_value = MOCK_PROFILE_CONFIG.copy()
    mock_args.url = "http://cmd.line.override"
    mock_args.collection = "cmd_collection_override"
    # Set port via CLI arg to override profile and satisfy requirement
    mock_args.port = 9999 
    config = load_configuration(mock_args)
    assert config["url"] == "http://cmd.line.override" # Overridden
    assert config["port"] == 9999 # Overridden from CLI
    assert config["api_key"] == "profile_key" # From profile (not overridden)
    assert config["collection"] == "cmd_collection_override" # Overridden
    mock_load_config_call.assert_called_once_with("myprofile")

# Keep the test for missing required, it should still work with mocked load_config
@patch('qdrant_manager.utils.load_config')
@patch('qdrant_manager.utils.logger')
@patch('sys.exit')
def test_load_configuration_missing_required(mock_exit, mock_logger, mock_load_config_call, mock_args):
    """Test load_configuration exits if required fields are missing."""
    # Simulate load_config returning a config missing 'url'
    mock_load_config_call.return_value = {"port": 1234} 
    load_configuration(mock_args)
    # Check for the specific error about url being missing
    mock_logger.error.assert_any_call("Missing required configuration: url")
    mock_logger.error.assert_any_call("Please update your configuration or provide command-line arguments.")
    mock_exit.assert_called_once_with(1)

@patch('qdrant_manager.utils.load_config')
@patch('qdrant_manager.utils.logger')
@patch('sys.exit')
def test_load_configuration_missing_required_port(mock_exit, mock_logger, mock_load_config_call, mock_args):
    """Test load_configuration exits if required port is missing."""
    # Simulate load_config returning a config missing 'port'
    mock_load_config_call.return_value = {"url": "http://test.com"} 
    load_configuration(mock_args)
    mock_logger.error.assert_any_call("Missing required configuration: port")
    mock_logger.error.assert_any_call("Please update your configuration or provide command-line arguments.")
    mock_exit.assert_called_once_with(1)

# You might want to add tests for initialize_qdrant_client here as well,
# similar to how it was tested in test_connection.py previously.
