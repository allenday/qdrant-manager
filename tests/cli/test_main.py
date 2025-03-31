"""Tests for the CLI main function."""
import pytest
from unittest.mock import patch, MagicMock
from qdrant_manager.cli import main
from pathlib import Path


def test_main_config():
    """Test the main function with the config command."""
    with patch('sys.argv', ['qdrant-manager', 'config']):
        with patch('qdrant_manager.cli.get_profiles') as mock_get_profiles:
            mock_get_profiles.return_value = ['default', 'production']
            with patch('qdrant_manager.cli.get_config_dir') as mock_get_config_dir:
                mock_get_config_dir.return_value = Path("/fake/config/dir")
                with patch('builtins.print') as mock_print:
                    with patch('sys.exit') as mock_exit:
                        # Since sys.exit is called twice (once to exit after printing profiles,
                        # and once because we're mocking the exit function), we'll need to
                        # catch the exception and verify exit was called at least once
                        try:
                            main()
                        except SystemExit:
                            pass
                        
                        # Check that profiles were printed
                        mock_print.assert_any_call("Available configuration profiles:")
                        mock_print.assert_any_call("  - default")
                        mock_print.assert_any_call("  - production")
                        assert mock_exit.call_count >= 1


def test_main_list():
    """Test the main function with the list command."""
    with patch('sys.argv', ['qdrant-manager', 'list']):
        with patch('qdrant_manager.cli.load_configuration') as mock_load_config:
            mock_load_config.return_value = {
                "url": "test-url", 
                "port": 1234,
                "api_key": "test-key"
            }
            with patch('qdrant_manager.cli.initialize_qdrant_client') as mock_init_client:
                mock_client = MagicMock()
                mock_init_client.return_value = mock_client
                with patch('qdrant_manager.cli.list_collections') as mock_list_collections:
                    mock_list_collections.return_value = ["collection1", "collection2"]
                    main()
                    # Check that list_collections was called
                    mock_list_collections.assert_called_once_with(mock_client)


def test_main_create():
    """Test the main function with the create command."""
    with patch('sys.argv', ['qdrant-manager', 'create', '--collection', 'test-collection']):
        with patch('qdrant_manager.cli.load_configuration') as mock_load_config:
            mock_load_config.return_value = {
                "url": "test-url", 
                "port": 1234,
                "api_key": "test-key",
                "collection": "default-collection",
                "vector_size": 256,
                "distance": "cosine",
                "indexing_threshold": 0,
                "payload_indices": []
            }
            with patch('qdrant_manager.cli.initialize_qdrant_client') as mock_init_client:
                mock_client = MagicMock()
                mock_init_client.return_value = mock_client
                with patch('qdrant_manager.cli.create_collection') as mock_create_collection:
                    with patch('qdrant_manager.cli.models.Distance') as mock_distance:
                        mock_distance.COSINE = "cosine"
                        main()
                        # Check that create_collection was called with the right parameters
                        mock_create_collection.assert_called_once_with(
                            mock_client, 
                            "test-collection", 
                            False, 
                            256, 
                            mock_distance.COSINE, 
                            0
                        )


def test_main_delete():
    """Test the main function with the delete command."""
    with patch('sys.argv', ['qdrant-manager', 'delete', '--collection', 'test-collection']):
        with patch('qdrant_manager.cli.load_configuration') as mock_load_config:
            mock_load_config.return_value = {
                "url": "test-url", 
                "port": 1234,
                "api_key": "test-key",
                "collection": "default-collection"
            }
            with patch('qdrant_manager.cli.initialize_qdrant_client') as mock_init_client:
                mock_client = MagicMock()
                mock_init_client.return_value = mock_client
                with patch('qdrant_manager.cli.delete_collection') as mock_delete_collection:
                    main()
                    # Check that delete_collection was called with the right parameters
                    mock_delete_collection.assert_called_once_with(mock_client, "test-collection")


def test_main_info():
    """Test the main function with the info command."""
    with patch('sys.argv', ['qdrant-manager', 'info', '--collection', 'test-collection']):
        with patch('qdrant_manager.cli.load_configuration') as mock_load_config:
            mock_load_config.return_value = {
                "url": "test-url", 
                "port": 1234,
                "api_key": "test-key",
                "collection": "default-collection"
            }
            with patch('qdrant_manager.cli.initialize_qdrant_client') as mock_init_client:
                mock_client = MagicMock()
                mock_init_client.return_value = mock_client
                with patch('qdrant_manager.cli.collection_info') as mock_collection_info:
                    main()
                    # Check that collection_info was called with the right parameters
                    mock_collection_info.assert_called_once_with(mock_client, "test-collection")


def test_main_batch():
    """Test the main function with the batch command."""
    with patch('sys.argv', ['qdrant-manager', 'batch', '--collection', 'test-collection', 
                            '--ids', 'doc1,doc2', '--add', '--doc', '{"field":"value"}']):
        with patch('qdrant_manager.cli.load_configuration') as mock_load_config:
            mock_load_config.return_value = {
                "url": "test-url", 
                "port": 1234,
                "api_key": "test-key",
                "collection": "default-collection"
            }
            with patch('qdrant_manager.cli.initialize_qdrant_client') as mock_init_client:
                mock_client = MagicMock()
                mock_init_client.return_value = mock_client
                with patch('qdrant_manager.cli.batch_operations') as mock_batch_operations:
                    main()
                    # Check that batch_operations was called
                    mock_batch_operations.assert_called_once()
                    # First argument should be client
                    assert mock_batch_operations.call_args[0][0] == mock_client
                    # Second argument should be collection name
                    assert mock_batch_operations.call_args[0][1] == "test-collection"
