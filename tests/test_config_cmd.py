import pytest
import io
import contextlib
from unittest.mock import patch, MagicMock
from pathlib import Path # Import Path

# Import the function under test
from solr_manager.commands.config import show_config_info

# Mock argparse Namespace
class MockArgs:
    def __init__(self, profile=None):
        self.profile = profile
        # Add other args if show_config_info uses them

@patch('solr_manager.commands.config.get_config_dir')
@patch('solr_manager.commands.config.get_profiles')
def test_show_config_info_default(mock_get_profiles, mock_get_config_dir):
    """Test showing config info with default profile."""
    mock_config_dir = Path("/fake/path/to/config") # Use Path object
    mock_profiles = {"default": {"connection": {"solr_url": "fake"}}, "prod": {}}
    
    mock_get_config_dir.return_value = mock_config_dir
    mock_get_profiles.return_value = mock_profiles
    args = MockArgs(profile=None) # Simulate no --profile arg
    
    stdout_capture = io.StringIO()
    with contextlib.redirect_stdout(stdout_capture):
        show_config_info(args)
    output = stdout_capture.getvalue()
    
    mock_get_config_dir.assert_called_once()
    mock_get_profiles.assert_called_once_with() # get_profiles takes no args now
    assert "Configuration Directory:" in output
    assert str(mock_config_dir) in output # Assert string representation
    assert "Available Profiles:" in output
    assert "- default" in output
    assert "- prod" in output
    assert "Current profile (default):" in output
    assert "solr_url: fake" in output # Check content of default profile

@patch('solr_manager.commands.config.get_config_dir')
@patch('solr_manager.commands.config.get_profiles')
def test_show_config_info_specific_profile(mock_get_profiles, mock_get_config_dir):
    """Test showing config info with a specific --profile."""
    mock_config_dir = Path("/fake/config") # Use Path object
    mock_profiles = {
        "default": {"connection": {"solr_url": "default_url"}},
        "staging": {"connection": {"zk_hosts": "zk1"}}
    }
    
    mock_get_config_dir.return_value = mock_config_dir
    mock_get_profiles.return_value = mock_profiles
    args = MockArgs(profile="staging") # Simulate --profile staging
    
    stdout_capture = io.StringIO()
    with contextlib.redirect_stdout(stdout_capture):
        show_config_info(args)
    output = stdout_capture.getvalue()
    
    mock_get_config_dir.assert_called_once()
    mock_get_profiles.assert_called_once_with() # get_profiles takes no args now
    assert "Configuration Directory:" in output
    assert str(mock_config_dir) in output # Assert string representation
    assert "Available Profiles:" in output
    assert "- default" in output
    assert "- staging" in output
    assert "Current profile (staging):" in output
    assert "zk_hosts: zk1" in output # Check content of staging profile

@patch('solr_manager.commands.config.get_config_dir')
@patch('solr_manager.commands.config.get_profiles')
def test_show_config_info_profile_not_found(mock_get_profiles, mock_get_config_dir):
    """Test showing config info when specified profile doesn't exist."""
    mock_config_dir = Path("/fake/config") # Use Path object
    mock_profiles = {"default": {}}
    
    mock_get_config_dir.return_value = mock_config_dir
    mock_get_profiles.return_value = mock_profiles
    args = MockArgs(profile="missing")
    
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    # Expect SystemExit(1) when profile not found
    with patch('sys.exit') as mock_exit, \
         contextlib.redirect_stdout(stdout_capture), \
         contextlib.redirect_stderr(stderr_capture):
        show_config_info(args)
        
    output = stdout_capture.getvalue()
    error_output = stderr_capture.getvalue() 
    
    mock_get_config_dir.assert_called_once()
    mock_get_profiles.assert_called_once_with() # get_profiles takes no args now
    assert "Configuration Directory:" in output # Still prints directory and profiles
    assert str(mock_config_dir) in output # Assert string representation
    assert "Available Profiles:" in output
    assert "- default" in output
    assert "Error: Profile 'missing' not found" in error_output
    mock_exit.assert_called_once_with(1)

@patch('solr_manager.commands.config.get_config_dir')
@patch('solr_manager.commands.config.get_profiles')
def test_show_config_info_no_profiles(mock_get_profiles, mock_get_config_dir):
    """Test showing config info when config file is empty or profiles missing."""
    mock_config_dir = Path("/fake/config") # Use Path object
    mock_profiles = None # Simulate loading returning None
    
    mock_get_config_dir.return_value = mock_config_dir
    mock_get_profiles.return_value = mock_profiles
    args = MockArgs(profile=None)
    
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    with patch('sys.exit') as mock_exit, \
         contextlib.redirect_stdout(stdout_capture), \
         contextlib.redirect_stderr(stderr_capture):
        show_config_info(args)
        
    output = stdout_capture.getvalue()
    error_output = stderr_capture.getvalue()

    mock_get_config_dir.assert_called_once()
    mock_get_profiles.assert_called_once_with() # get_profiles takes no args now
    assert "Configuration Directory:" in output
    assert str(mock_config_dir) in output # Assert string representation
    assert "Could not load profiles from" in error_output
    mock_exit.assert_called_once_with(1) 