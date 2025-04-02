import pytest
import os
import yaml
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open
import platform
import appdirs
import builtins

# Import functions under test
from solr_manager.config import (
    get_config_dir,
    get_profiles,
    load_config,
    DEFAULT_PROFILE,
    CONFIG_FILENAME,
    get_config_file
)

# === Test get_config_dir ===

@patch('appdirs.user_config_dir', return_value="/mock/appdir/path")
def test_get_config_dir_uses_appdirs(mock_user_config_dir):
    """Test get_config_dir calls appdirs.user_config_dir."""
    expected_path = Path("/mock/appdir/path")
    config_dir = get_config_dir()
    assert config_dir == expected_path
    mock_user_config_dir.assert_called_once_with("solr-manager")

# === Test get_profiles ===

@patch('solr_manager.config.get_config_file', return_value=Path("/fake/config.yaml"))
@patch('pathlib.Path.exists', return_value=True)
@patch('builtins.open', new_callable=mock_open, read_data='default:\n  conn: {}\nprod:\n  conn: {}')
@patch('yaml.safe_load', return_value={'default': {'conn': {}}, 'prod': {'conn': {}}})
def test_get_profiles_success(mock_safe_load, mock_open_file, mock_exists, mock_get_file):
    """Test get_profiles successfully reads profile names from config."""
    profiles = get_profiles()
    mock_get_file.assert_called_once()
    mock_exists.assert_called_once()
    mock_open_file.assert_called_once_with(Path("/fake/config.yaml"), 'r')
    mock_safe_load.assert_called_once()
    assert profiles == ["default", "prod"]

@patch('solr_manager.config.get_config_file', return_value=Path("/fake/config.yaml"))
@patch('pathlib.Path.exists', return_value=False)
def test_get_profiles_file_not_found(mock_exists, mock_get_file):
    """Test get_profiles returns default when config file doesn't exist."""
    profiles = get_profiles()
    mock_get_file.assert_called_once()
    mock_exists.assert_called_once()
    assert profiles == [DEFAULT_PROFILE]

@patch('solr_manager.config.get_config_file', return_value=Path("/fake/config.yaml"))
@patch('pathlib.Path.exists', return_value=True)
@patch('builtins.open', new_callable=mock_open, read_data='invalid yaml:')
@patch('yaml.safe_load', side_effect=yaml.YAMLError("Bad YAML"))
@patch('solr_manager.config.logger')
def test_get_profiles_yaml_error(mock_logger, mock_safe_load, mock_open_file, mock_exists, mock_get_file):
    """Test get_profiles handles YAML errors and returns default."""
    profiles = get_profiles()
    mock_get_file.assert_called_once()
    mock_exists.assert_called_once()
    mock_open_file.assert_called_once_with(Path("/fake/config.yaml"), 'r')
    mock_safe_load.assert_called_once()
    assert profiles == [DEFAULT_PROFILE]
    mock_logger.warning.assert_called_once()

@patch('solr_manager.config.get_config_file', return_value=Path("/fake/config.yaml"))
@patch('pathlib.Path.exists', return_value=True)
@patch('builtins.open', new_callable=mock_open, read_data='not_a_dict')
@patch('yaml.safe_load', return_value="not_a_dict")
@patch('solr_manager.config.logger')
def test_get_profiles_not_a_dict(mock_logger, mock_safe_load, mock_open_file, mock_exists, mock_get_file):
    """Test get_profiles handles config file not being a dictionary."""
    profiles = get_profiles()
    mock_get_file.assert_called_once()
    mock_exists.assert_called_once()
    mock_open_file.assert_called_once_with(Path("/fake/config.yaml"), 'r')
    mock_safe_load.assert_called_once()
    assert profiles == [DEFAULT_PROFILE]
    mock_logger.warning.assert_called_once()

# === Test load_config ===

# Mock the dependencies of load_config for all tests in this section
@patch('solr_manager.config.get_config_file')
@patch('pathlib.Path.exists')
@patch('builtins.open', new_callable=mock_open)
@patch('yaml.safe_load')
@patch('solr_manager.config.logger')
@patch('solr_manager.config.create_default_config') # Also mock this as load_config calls it
def test_load_config_default_profile(mock_create_default, mock_logger, mock_safe_load, mock_open_file, mock_exists, mock_get_file):
    """Test loading the default profile."""
    mock_config_path = Path("/fake/dir/config.yaml")
    mock_get_file.return_value = mock_config_path
    mock_exists.return_value = True
    mock_profiles = {
        "default": {"connection": {"solr_url": "http://default"}, "other": "setting"},
        "prod": {"connection": {"zk_hosts": "zk1"}}
    }
    mock_safe_load.return_value = mock_profiles

    config = load_config(profile=None) # Use profile=None

    mock_get_file.assert_called_once()
    mock_exists.assert_called_once()
    mock_open_file.assert_called_once_with(mock_config_path, 'r')
    mock_safe_load.assert_called_once()
    assert config == {"solr_url": "http://default"} # Only connection dict is returned
    mock_logger.error.assert_not_called()
    mock_create_default.assert_not_called()


@patch('solr_manager.config.get_config_file')
@patch('pathlib.Path.exists')
@patch('builtins.open', new_callable=mock_open)
@patch('yaml.safe_load')
@patch('solr_manager.config.logger')
@patch('solr_manager.config.create_default_config')
def test_load_config_specific_profile(mock_create_default, mock_logger, mock_safe_load, mock_open_file, mock_exists, mock_get_file):
    """Test loading a specific named profile."""
    mock_config_path = Path("/fake/dir/config.yaml")
    mock_get_file.return_value = mock_config_path
    mock_exists.return_value = True
    mock_profiles = {
        "default": {"connection": {"solr_url": "http://default"}},
        "prod": {"connection": {"zk_hosts": "zk1", "timeout": 60}}
    }
    mock_safe_load.return_value = mock_profiles

    config = load_config(profile="prod") # Use profile="prod"

    mock_get_file.assert_called_once()
    mock_exists.assert_called_once()
    mock_open_file.assert_called_once_with(mock_config_path, 'r')
    mock_safe_load.assert_called_once()
    assert config == {"zk_hosts": "zk1", "timeout": 60}
    mock_logger.error.assert_not_called()
    mock_create_default.assert_not_called()

# No test for merging needed, load_config just returns the specific profile's connection section

@patch('solr_manager.config.get_config_file')
@patch('pathlib.Path.exists')
@patch('builtins.open', new_callable=mock_open)
@patch('yaml.safe_load', return_value={}) # Empty config file
@patch('solr_manager.config.logger')
@patch('solr_manager.config.create_default_config')
def test_load_config_empty_file(mock_create_default, mock_logger, mock_safe_load, mock_open_file, mock_exists, mock_get_file):
    """Test load_config when config file is empty."""
    mock_config_path = Path("/fake/dir/config.yaml")
    mock_get_file.return_value = mock_config_path
    mock_exists.return_value = True

    config = load_config(profile=None)

    assert config is None
    mock_logger.error.assert_any_call(f"Configuration file {mock_config_path} is empty or invalid.")
    mock_create_default.assert_not_called()


@patch('solr_manager.config.get_config_file')
@patch('pathlib.Path.exists')
@patch('builtins.open', new_callable=mock_open)
@patch('yaml.safe_load', side_effect=yaml.YAMLError("Bad YAML"))
@patch('solr_manager.config.logger')
@patch('solr_manager.config.create_default_config')
def test_load_config_yaml_error(mock_create_default, mock_logger, mock_safe_load, mock_open_file, mock_exists, mock_get_file):
    """Test load_config handling YAML parse errors."""
    mock_config_path = Path("/fake/dir/config.yaml")
    mock_get_file.return_value = mock_config_path
    mock_exists.return_value = True

    config = load_config(profile=None)

    assert config is None
    mock_logger.error.assert_any_call(f"Error parsing configuration file {mock_config_path}: Bad YAML")
    mock_create_default.assert_not_called()


@patch('solr_manager.config.get_config_file')
@patch('pathlib.Path.exists', return_value=True) # Config exists
@patch('builtins.open', new_callable=mock_open)
@patch('yaml.safe_load')
@patch('solr_manager.config.logger')
@patch('solr_manager.config.create_default_config')
def test_load_config_default_profile_missing(mock_create_default, mock_logger, mock_safe_load, mock_open_file, mock_exists, mock_get_file):
    """Test load_config when default profile is requested but missing."""
    mock_config_path = Path("/fake/dir/config.yaml")
    mock_get_file.return_value = mock_config_path
    mock_profiles = {"prod": {}} # No default profile
    mock_safe_load.return_value = mock_profiles

    config = load_config(profile=None) # Request default

    assert config is None
    mock_logger.error.assert_any_call(f"Profile '{DEFAULT_PROFILE}' not found in {mock_config_path}.")
    mock_create_default.assert_not_called()


@patch('solr_manager.config.get_config_file')
@patch('pathlib.Path.exists', return_value=True) # Config exists
@patch('builtins.open', new_callable=mock_open)
@patch('yaml.safe_load')
@patch('solr_manager.config.logger')
@patch('solr_manager.config.create_default_config')
def test_load_config_specific_profile_missing(mock_create_default, mock_logger, mock_safe_load, mock_open_file, mock_exists, mock_get_file):
    """Test load_config when a specific profile is requested but missing."""
    mock_config_path = Path("/fake/dir/config.yaml")
    mock_get_file.return_value = mock_config_path
    mock_profiles = {"default": {}}
    mock_safe_load.return_value = mock_profiles
    profile_to_load = "staging"

    config = load_config(profile=profile_to_load)

    assert config is None
    mock_logger.error.assert_any_call(f"Profile '{profile_to_load}' not found in {mock_config_path}.")
    mock_create_default.assert_not_called()


@patch('solr_manager.config.get_config_file')
@patch('pathlib.Path.exists', return_value=True) # Config exists
@patch('builtins.open', new_callable=mock_open)
@patch('yaml.safe_load')
@patch('solr_manager.config.logger')
@patch('solr_manager.config.create_default_config')
def test_load_config_profile_not_dict(mock_create_default, mock_logger, mock_safe_load, mock_open_file, mock_exists, mock_get_file):
    """Test load_config when a profile's data is not a dictionary."""
    mock_config_path = Path("/fake/dir/config.yaml")
    mock_get_file.return_value = mock_config_path
    mock_profiles = {"default": "not_a_dict"}
    mock_safe_load.return_value = mock_profiles

    config = load_config(profile=None) # Request default

    assert config is None
    mock_logger.error.assert_any_call(f"Profile '{DEFAULT_PROFILE}' in {mock_config_path} is not a valid dictionary.")
    mock_create_default.assert_not_called()

@patch('solr_manager.config.get_config_file')
@patch('pathlib.Path.exists', return_value=True) # Config exists
@patch('builtins.open', new_callable=mock_open)
@patch('yaml.safe_load')
@patch('solr_manager.config.logger')
@patch('solr_manager.config.create_default_config')
def test_load_config_connection_section_missing(mock_create_default, mock_logger, mock_safe_load, mock_open_file, mock_exists, mock_get_file):
    """Test load_config when a profile is missing the 'connection' section."""
    mock_config_path = Path("/fake/dir/config.yaml")
    mock_get_file.return_value = mock_config_path
    mock_profiles = {"default": {"other_stuff": 123}} # No connection section
    mock_safe_load.return_value = mock_profiles

    config = load_config(profile=None) # Request default

    assert config is None
    mock_logger.error.assert_any_call(f"Profile '{DEFAULT_PROFILE}' in {mock_config_path} is missing the 'connection' section or it's invalid.")
    mock_create_default.assert_not_called()


@patch('solr_manager.config.get_config_file')
@patch('pathlib.Path.exists', return_value=False) # Config does NOT exist
@patch('builtins.open', new_callable=mock_open) # Need to mock open even if create_default is mocked for initial file check
@patch('yaml.safe_load') # Need to mock yaml even if create_default is mocked for initial file check
@patch('solr_manager.config.logger')
@patch('solr_manager.config.create_default_config', return_value=Path("/new/config.yaml")) # Creation succeeds
@patch('sys.exit') # Mock sys.exit
@patch('builtins.print') # Mock print
def test_load_config_creates_default_and_exits(mock_print, mock_exit, mock_create_default, mock_logger, mock_safe_load, mock_open_file, mock_exists, mock_get_file):
    """Test load_config calls create_default_config and exits if file is missing."""
    mock_config_path = Path("/fake/dir/config.yaml")
    mock_get_file.return_value = mock_config_path

    load_config(profile=None)

    mock_get_file.assert_called_once()
    mock_exists.assert_called_once()
    mock_create_default.assert_called_once()
    mock_print.assert_any_call("Default configuration file created. Please edit it and run the command again.")
    mock_exit.assert_called_once_with(1)
    # Don't check open/safe_load as it exits before trying to load


@patch('solr_manager.config.get_config_file')
@patch('pathlib.Path.exists', return_value=False) # Config does NOT exist
@patch('builtins.open', new_callable=mock_open) # Mock open
@patch('yaml.safe_load') # Mock yaml
@patch('solr_manager.config.logger')
@patch('solr_manager.config.create_default_config', return_value=None) # Creation fails
@patch('sys.exit') # Mock sys.exit
@patch('builtins.print') # Mock print
def test_load_config_create_default_fails_and_exits(mock_print, mock_exit, mock_create_default, mock_logger, mock_safe_load, mock_open_file, mock_exists, mock_get_file):
    """Test load_config handles failure of create_default_config and exits."""
    mock_config_path = Path("/fake/dir/config.yaml")
    mock_get_file.return_value = mock_config_path

    load_config(profile=None)

    mock_get_file.assert_called_once()
    mock_exists.assert_called_once()
    mock_create_default.assert_called_once()
    mock_print.assert_any_call("Failed to create a default configuration file.")
    mock_exit.assert_called_once_with(1)
    # Don't check open/safe_load as it exits before trying to load


# Remove old test_load_config_no_profiles_loaded - load_config now exits if file doesn't exist
# Remove old test_load_config_merging_with_default - load_config doesn't merge