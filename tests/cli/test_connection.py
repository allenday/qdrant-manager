"""Tests for Qdrant connection functionality."""
import pytest
from unittest.mock import patch, MagicMock

from qdrant_manager.utils import initialize_qdrant_client
from qdrant_client import QdrantClient


def test_initialize_qdrant_client():
    """Test initializing Qdrant client."""
    # Create test environment variables
    env_vars = {
        "url": "test-url",
        "port": 1234,
        "api_key": "test-key"
    }
    
    # Mock QdrantClient class within the correct module (utils)
    with patch('qdrant_manager.utils.QdrantClient') as mock_client_class:
        # Create a mock client instance
        mock_client = MagicMock()
        # Mock the get_collections call used for connection testing
        mock_client.get_collections.return_value = MagicMock() 
        mock_client_class.return_value = mock_client
        
        # Call the function, patching the logger in utils
        with patch('qdrant_manager.utils.logger') as mock_logger:
            with patch('sys.exit') as mock_exit:
                client = initialize_qdrant_client(env_vars)
                
                # Check that client was properly initialized
                mock_client_class.assert_called_once_with(
                    url="test-url",
                    port=1234,
                    api_key="test-key"
                )
                
                # Check that we tested the connection
                mock_client.get_collections.assert_called_once()
                
                # Client should be returned
                assert client == mock_client
                
                # sys.exit should not be called
                mock_exit.assert_not_called()
    
    # Test connection failure
    with patch('qdrant_manager.utils.QdrantClient') as mock_client_class:
        # Make the client raise an exception during connection test
        mock_client = MagicMock()
        mock_client.get_collections.side_effect = Exception("Connection failed")
        mock_client_class.return_value = mock_client
        
        # Call the function, patching the logger in utils
        with patch('qdrant_manager.utils.logger') as mock_logger:
            with patch('sys.exit') as mock_exit:
                # initialize_qdrant_client calls sys.exit on failure
                initialize_qdrant_client(env_vars)
                 
                # Check that error was logged
                mock_logger.error.assert_called_with("Failed to connect to Qdrant: Connection failed")
                
                # Check that we exited the program
                mock_exit.assert_called_once_with(1)
