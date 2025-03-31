"""Tests for CLI utility functions."""
import pytest
from unittest.mock import patch, MagicMock
from qdrant_manager.cli import resolve_json_path, load_configuration, parse_filter


def test_resolve_json_path():
    """Test the resolve_json_path function."""
    # Create a test nested object
    test_obj = {
        "level1": {
            "level2": {
                "level3": "value"
            },
            "array": [1, 2, 3]
        },
        "root_key": "root_value"
    }
    
    # Test resolving paths
    parent, key, success = resolve_json_path(test_obj, "level1.level2.level3")
    assert success is True
    assert parent == test_obj["level1"]["level2"]
    assert key == "level3"
    
    # Test with root path
    parent, key, success = resolve_json_path(test_obj, "root_key")
    assert success is True
    assert parent == test_obj
    assert key == "root_key"
    
    # Test with empty path
    parent, key, success = resolve_json_path(test_obj, "")
    assert success is True
    assert parent == test_obj
    assert key is None
    
    # Test with path "/"
    parent, key, success = resolve_json_path(test_obj, "/")
    assert success is True
    assert parent == test_obj
    assert key is None
    
    # Test with path starting with "/"
    parent, key, success = resolve_json_path(test_obj, "/level1/level2/level3")
    assert success is True
    assert parent == test_obj["level1"]["level2"]
    assert key == "level3"
    
    # Test with nonexistent path without create_missing
    parent, key, success = resolve_json_path(test_obj, "nonexistent.path", create_missing=False)
    assert success is False
    assert parent is None
    assert key is None
    
    # Test with nonexistent path with create_missing
    parent, key, success = resolve_json_path(test_obj, "new_key.subkey", create_missing=True)
    assert success is True
    assert parent == test_obj["new_key"]
    assert key == "subkey"
    assert "new_key" in test_obj
    assert isinstance(test_obj["new_key"], dict)
    
    # Test with path to non-dict element
    parent, key, success = resolve_json_path(test_obj, "level1.array.invalid", create_missing=False)
    assert success is False


def test_load_configuration():
    """Test loading configuration from args."""
    # Create mock args
    mock_args = MagicMock()
    mock_args.profile = None
    mock_args.url = "test-url"
    mock_args.port = 1234
    mock_args.api_key = "test-key"
    mock_args.collection = "test-collection"
    
    # Mock the load_config function
    with patch('qdrant_manager.cli.load_config') as mock_load_config:
        mock_load_config.return_value = {
            "url": "default-url",
            "port": 6333,
            "api_key": "",
            "collection": "default-collection"
        }
        
        # Call the function
        config = load_configuration(mock_args)
        
        # Assert load_config was called
        mock_load_config.assert_called_once()
        
        # Check if args override config values
        assert config["url"] == "test-url"
        assert config["port"] == 1234
        assert config["api_key"] == "test-key"
        assert config["collection"] == "test-collection"
        
    # Test with no command line overrides
    mock_args = MagicMock()
    mock_args.profile = "test-profile"
    mock_args.url = None
    mock_args.port = None
    mock_args.api_key = None
    mock_args.collection = None
    
    with patch('qdrant_manager.cli.load_config') as mock_load_config:
        mock_load_config.return_value = {
            "url": "profile-url",
            "port": 7000,
            "api_key": "profile-key",
            "collection": "profile-collection"
        }
        
        # Call the function
        config = load_configuration(mock_args)
        
        # Assert load_config was called with profile
        mock_load_config.assert_called_once_with("test-profile")
        
        # Check if values from profile are used
        assert config["url"] == "profile-url"
        assert config["port"] == 7000
        assert config["api_key"] == "profile-key"
        assert config["collection"] == "profile-collection"


def test_parse_filter():
    """Test parsing a JSON filter string."""
    import json
    
    # Test valid JSON
    filter_str = '{"key": "category", "match": {"value": "product"}}'
    filter_dict = parse_filter(filter_str)
    
    expected_dict = json.loads(filter_str)
    assert filter_dict == expected_dict
    
    # Test invalid JSON
    with patch('qdrant_manager.cli.logger') as mock_logger:
        with patch('sys.exit') as mock_exit:
            try:
                parse_filter("invalid json")
            except:
                pass
            
            # Check that error was logged
            mock_logger.error.assert_called()
            mock_exit.assert_called_once()


def test_load_document_ids():
    """Test loading document IDs from a file."""
    from qdrant_manager.cli import load_document_ids
    
    # Create a temporary file with document IDs
    import tempfile
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
        temp_file.write("id1\nid2\nid3\n")
        temp_file_path = temp_file.name
    
    try:
        # Test loading IDs
        with patch('qdrant_manager.cli.logger') as mock_logger:
            doc_ids = load_document_ids(temp_file_path)
            
            # Check that IDs were loaded correctly
            assert doc_ids == ["id1", "id2", "id3"]
            
            # Check that we logged the count
            mock_logger.info.assert_called()
        
        # Test with whitespace and empty lines
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            temp_file.write("id1\n  id2  \n\nid3")
            temp_file_path = temp_file.name
        
        with patch('qdrant_manager.cli.logger') as mock_logger:
            doc_ids = load_document_ids(temp_file_path)
            
            # Check that IDs were loaded correctly with whitespace stripped
            assert doc_ids == ["id1", "id2", "id3"]
    
    finally:
        # Clean up
        import os
        os.unlink(temp_file_path)
    
    # Test file not found
    with patch('qdrant_manager.cli.logger') as mock_logger:
        with patch('sys.exit') as mock_exit:
            try:
                load_document_ids("/nonexistent/file/path")
            except:
                pass
            
            # Check that error was logged
            mock_logger.error.assert_called()
            
            # Check that we exited the program
            mock_exit.assert_called_once()


def test_load_configuration_config_validation():
    """Test configuration validation in load_configuration."""
    # Create mock args with missing port and url
    mock_args = MagicMock()
    mock_args.profile = None
    mock_args.url = None
    mock_args.port = None
    mock_args.api_key = "test-key"
    mock_args.collection = "test-collection"
    
    # Mock the load_config function to return incomplete config
    with patch('qdrant_manager.cli.load_config') as mock_load_config:
        mock_load_config.return_value = {
            # Missing url and port
            "api_key": "",
            "collection": "default-collection"
        }
        
        # Call should fail due to missing required keys
        with patch('qdrant_manager.cli.logger') as mock_logger:
            with patch('sys.exit') as mock_exit:
                try:
                    load_configuration(mock_args)
                except:
                    # Expected exception, don't fail test
                    pass
                
                # Errors should be logged
                assert mock_logger.error.call_count >= 1
                
                # First call should list missing keys
                missing_keys_call = mock_logger.error.call_args_list[0]
                missing_keys_msg = missing_keys_call[0][0]
                assert "Missing required configuration" in missing_keys_msg
                assert "url" in missing_keys_msg
                assert "port" in missing_keys_msg
                
                # sys.exit should be called with code 1
                mock_exit.assert_called_with(1)
