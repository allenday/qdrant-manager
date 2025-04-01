"""
Reorganized tests for the CLI module.

This file can contain integration-style tests or specific tests for the main CLI entry point.
Other specific tests are in the tests/cli/ directory.
"""
import json
import pytest
from unittest.mock import MagicMock, patch
import subprocess
import os
import yaml
from pathlib import Path

from qdrant_manager.cli import main
from qdrant_manager.config import get_config_dir

# Remove imports of test functions from other files
# from tests.cli.test_utils import (...)
# from tests.cli.test_connection import ...
# from tests.cli.test_collections import (...)
# from tests.cli.test_batch_operations import (...)
# from tests.cli.test_main import (...)

# Add a simple test to check that importable modules are working
def test_cli_module_exists():
    """Test that the CLI module can be imported."""
    from qdrant_manager import cli
    assert cli is not None

# Fixture to create a dummy config file
@pytest.fixture(scope="function")
def dummy_config_file(tmp_path):
    config_dir = tmp_path / ".config" / "qdrant-manager"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "config.yaml"
    dummy_data = {
        "default": {
            "url": "http://localhost",
            "port": 6333,
            "collection": "default_test_collection",
            "vector_size": 4,
            "distance": "cosine"
        },
        "profile1": {
            "url": "http://profile1.host",
            "port": 1234,
            "api_key": "prof1-key",
            "collection": "profile1_collection"
        }
    }
    with open(config_path, 'w') as f:
        yaml.dump(dummy_data, f)
    
    # Patch get_config_dir to return the temp config directory
    with patch('qdrant_manager.config.get_config_dir', return_value=config_dir):
        yield config_path # Yield the path to the dummy config file

# Example test using the fixture and testing the main CLI entry point
@patch('qdrant_manager.cli.load_configuration') # Patch load_configuration
@patch('qdrant_manager.cli.initialize_qdrant_client') # Mock client init
@patch('qdrant_manager.cli.list_collections') # Mock the specific command function
def test_cli_list_command(mock_list_cmd, mock_init_client, mock_load_conf, dummy_config_file):
    """Test running the list command via the main CLI entry point."""
    # Set up mock return values
    mock_load_conf.return_value = {"url": "mock_url", "port": 1234} # Provide required config
    mock_client = MagicMock()
    mock_init_client.return_value = mock_client
    
    # Simulate command line arguments: qdrant-manager list
    test_args = ["qdrant-manager", "list"]
    with patch('sys.argv', test_args):
        main()
    
    mock_load_conf.assert_called_once()
    mock_init_client.assert_called_once_with(mock_load_conf.return_value)
    # Check that the list_collections function was called via the main entry point
    mock_list_cmd.assert_called_once_with(mock_client)

@patch('builtins.print')
@patch('sys.exit')
@patch('qdrant_manager.cli.get_config_dir') # Patch get_config_dir used by config cmd
@patch('qdrant_manager.cli.get_profiles') # Patch get_profiles used by config cmd
def test_cli_config_command_no_profile(mock_get_profiles, mock_get_cfg_dir, mock_exit, mock_print):
    """Test running the config command via the main CLI entry point (no profile)."""
    # Setup mocks for config command
    mock_get_profiles.return_value = ['default', 'profile1']
    mock_config_path = Path("/fake/config/dir/config.yaml")
    mock_get_cfg_dir.return_value = mock_config_path.parent
    expected_config_path_str = str(mock_config_path)

    test_args = ["qdrant-manager", "config"]
    with patch('sys.argv', test_args):
        try:
            main()
        except SystemExit:
            pass # Expected behavior

    mock_get_profiles.assert_called_once()
    mock_get_cfg_dir.assert_called_once()
    mock_print.assert_any_call("Available configuration profiles:")
    mock_print.assert_any_call("  - default")
    mock_print.assert_any_call("  - profile1")
    mock_print.assert_any_call(f"\nDefault configuration file: {expected_config_path_str}")
    assert mock_exit.call_count >= 1

# Add more integration-style tests for the main CLI entry point if needed
