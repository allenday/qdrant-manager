"""Tests for collection management functionality."""
import pytest
from unittest.mock import patch, MagicMock
from qdrant_manager.cli import (
    create_collection,
    delete_collection,
    list_collections,
    collection_info
)


def test_create_collection_new():
    """Test creating a new collection when it doesn't exist."""
    # Mock the Qdrant client
    mock_client = MagicMock()
    
    # Set up mock collections
    mock_collections_response = MagicMock()
    mock_collections_response.collections = []  # No collections exist yet
    mock_client.get_collections.return_value = mock_collections_response
    
    # For our test to pass, we need to implement the create_collection logic
    # Let's create a mock function that will update based on the test case
    def success_response(*args, **kwargs):
        # Return something to simulate success
        return True
    
    # Set the mock to use our function
    mock_client.create_collection.side_effect = success_response
    
    # Mock the models module and global args variable
    with patch('qdrant_manager.cli.models') as mock_models:
        # Set up the required models
        mock_models.Distance = MagicMock()
        mock_models.Distance.COSINE = MagicMock()
        mock_models.Distance.COSINE.name = "COSINE"
        mock_models.VectorParams = MagicMock()
        mock_models.OptimizersConfigDiff = MagicMock()
        
        # The global args variable needs to be patched
        with patch('qdrant_manager.cli.args', create=True) as mock_args:
            mock_args.payload_indices = []
            
            # Test creating a new collection
            with patch('qdrant_manager.cli.logger') as mock_logger:
                # Call the function we're testing
                result = create_collection(
                    mock_client,
                    "test-collection",
                    vector_size=256,
                    distance=mock_models.Distance.COSINE,
                    indexing_threshold=0
                )
                
                # The function should have called create_collection on the client
                mock_client.create_collection.assert_called_once()
                assert result is True

def test_create_collection_with_payload_indices():
    """Test creating a collection with payload indices."""
    # Mock the Qdrant client
    mock_client = MagicMock()
    
    # Set up mock collections
    mock_collections_response = MagicMock()
    mock_collections_response.collections = []  # No collections exist yet
    mock_client.get_collections.return_value = mock_collections_response
    
    # Mock the models module with schema types
    with patch('qdrant_manager.cli.models') as mock_models:
        # Set up the models with schema types
        mock_models.Distance = MagicMock()
        mock_models.Distance.COSINE = MagicMock()
        mock_models.Distance.COSINE.name = "COSINE"
        mock_models.VectorParams = MagicMock()
        mock_models.OptimizersConfigDiff = MagicMock()
        
        # Set up payload schema types
        mock_models.PayloadSchemaType = MagicMock()
        mock_models.PayloadSchemaType.KEYWORD = "keyword_type"
        mock_models.PayloadSchemaType.INTEGER = "integer_type"
        mock_models.PayloadSchemaType.FLOAT = "float_type"
        mock_models.PayloadSchemaType.GEO = "geo_type"
        mock_models.PayloadSchemaType.TEXT = "text_type"
        mock_models.PayloadSchemaType.DATETIME = "datetime_type"
        
        # Set up payload indices in args
        with patch('qdrant_manager.cli.args', create=True) as mock_args:
            mock_args.payload_indices = [
                {"field": "tag", "type": "keyword"},
                {"field": "count", "type": "integer"},
                {"field": "rating", "type": "float"},
                {"field": "location", "type": "geo"},
                {"field": "description", "type": "text"},
                {"field": "created_at", "type": "datetime"},
                {"field": "custom", "type": "unknown_type"},  # Tests default handling
                {"type": "keyword"}  # Missing field
            ]
            
            # Test creating a collection with indices
            with patch('qdrant_manager.cli.logger') as mock_logger:
                result = create_collection(
                    mock_client,
                    "test-collection"
                )
                
                # Check collection creation was called
                mock_client.create_collection.assert_called_once()
                
                # Check that create_payload_index was called for each valid index
                assert mock_client.create_payload_index.call_count == 7  # 7 valid indices
                
                # Check specific index creations
                mock_client.create_payload_index.assert_any_call(
                    collection_name="test-collection",
                    field_name="tag",
                    field_schema="keyword_type"
                )
                mock_client.create_payload_index.assert_any_call(
                    collection_name="test-collection",
                    field_name="count",
                    field_schema="integer_type"
                )
                
                # Verify warning for missing field
                mock_logger.warning.assert_called_once()
                
                assert result is True

def test_create_collection_with_payload_indices_error():
    """Test error handling when creating payload indices."""
    # Mock the Qdrant client
    mock_client = MagicMock()
    
    # Set up mock collections
    mock_collections_response = MagicMock()
    mock_collections_response.collections = []  # No collections exist yet
    mock_client.get_collections.return_value = mock_collections_response
    
    # Configure create_payload_index to raise an exception
    mock_client.create_payload_index.side_effect = Exception("Index creation failed")
    
    # Mock the models module with schema types
    with patch('qdrant_manager.cli.models') as mock_models:
        # Set up the models
        mock_models.Distance = MagicMock()
        mock_models.Distance.COSINE = MagicMock()
        mock_models.Distance.COSINE.name = "COSINE"
        mock_models.VectorParams = MagicMock()
        mock_models.OptimizersConfigDiff = MagicMock()
        mock_models.PayloadSchemaType = MagicMock()
        mock_models.PayloadSchemaType.KEYWORD = "keyword_type"
        
        # Set up payload indices in args
        with patch('qdrant_manager.cli.args', create=True) as mock_args:
            mock_args.payload_indices = [
                {"field": "tag", "type": "keyword"}
            ]
            
            # Test creating a collection with indices that fail
            with patch('qdrant_manager.cli.logger') as mock_logger:
                result = create_collection(
                    mock_client,
                    "test-collection"
                )
                
                # Check collection creation was called
                mock_client.create_collection.assert_called_once()
                
                # Check that create_payload_index was called
                mock_client.create_payload_index.assert_called_once()
                
                # Verify error was logged
                mock_logger.error.assert_called_with(
                    "Failed to create index for field 'tag': Index creation failed"
                )
                
                # Collection creation should still succeed even if index fails
                assert result is True

def test_create_collection_with_error():
    """Test error handling when creating a collection."""
    # Mock the Qdrant client
    mock_client = MagicMock()
    
    # Set up mock collections
    mock_collections_response = MagicMock()
    mock_collections_response.collections = []  # No collections exist yet
    mock_client.get_collections.return_value = mock_collections_response
    
    # Make create_collection fail
    mock_client.create_collection.side_effect = Exception("Collection creation failed")
    
    # Mock the models module
    with patch('qdrant_manager.cli.models') as mock_models:
        # Set up the required models
        mock_models.Distance = MagicMock()
        mock_models.Distance.COSINE = MagicMock()
        mock_models.Distance.COSINE.name = "COSINE"
        mock_models.VectorParams = MagicMock()
        mock_models.OptimizersConfigDiff = MagicMock()
        
        # Set up args
        with patch('qdrant_manager.cli.args', create=True) as mock_args:
            mock_args.payload_indices = []
            
            # Test creating a collection that fails
            with patch('qdrant_manager.cli.logger') as mock_logger:
                result = create_collection(
                    mock_client,
                    "test-collection"
                )
                
                # Check collection creation was attempted
                mock_client.create_collection.assert_called_once()
                
                # Verify error was logged
                mock_logger.error.assert_called_with(
                    "Error creating collection: Collection creation failed"
                )
                
                # Should return False on error
                assert result is False


def test_create_collection_existing():
    """Test creating a collection that already exists."""
    # Mock the Qdrant client
    mock_client = MagicMock()
    
    # Set up mock collections - collection exists this time
    mock_collection = MagicMock()
    mock_collection.name = "test-collection"
    mock_collections_response = MagicMock()
    mock_collections_response.collections = [mock_collection]  # Collection exists
    mock_client.get_collections.return_value = mock_collections_response
    
    # Mock the models module and global args variable
    with patch('qdrant_manager.cli.models') as mock_models:
        with patch('qdrant_manager.cli.args', create=True) as mock_args:
            mock_args.payload_indices = []
            with patch('qdrant_manager.cli.logger') as mock_logger:
                # Test with no overwrite
                result = create_collection(
                    mock_client, 
                    "test-collection",
                    overwrite=False
                )
                
                # Should not create or delete anything
                mock_client.create_collection.assert_not_called()
                mock_client.delete_collection.assert_not_called()
                assert result is False


def test_create_collection_overwrite():
    """Test creating a collection with overwrite when it already exists."""
    # Mock the Qdrant client
    mock_client = MagicMock()
    
    # Set up mock collections - collection exists
    mock_collection = MagicMock()
    mock_collection.name = "test-collection"
    mock_collections_response = MagicMock()
    mock_collections_response.collections = [mock_collection]  # Collection exists
    mock_client.get_collections.return_value = mock_collections_response
    
    # For our test to pass, we need to implement the create_collection logic
    # Let's create a mock function that will update based on the test case
    def success_response(*args, **kwargs):
        # Return something to simulate success
        return True
    
    # Set the mock to use our function
    mock_client.create_collection.side_effect = success_response
    
    with patch('qdrant_manager.cli.models') as mock_models:
        mock_models.VectorParams = MagicMock()
        mock_models.OptimizersConfigDiff = MagicMock()
        mock_models.Distance = MagicMock()
        mock_models.Distance.COSINE = MagicMock()
        mock_models.Distance.COSINE.name = "COSINE"
        
        with patch('qdrant_manager.cli.args', create=True) as mock_args:
            mock_args.payload_indices = []
            with patch('qdrant_manager.cli.logger') as mock_logger:
                # Test with overwrite
                result = create_collection(
                    mock_client, 
                    "test-collection",
                    overwrite=True
                )
                
                # Check that existing collection was deleted and a new one created
                mock_client.delete_collection.assert_called_once_with(collection_name="test-collection")
                mock_client.create_collection.assert_called_once()
                assert result is True


def test_delete_collection():
    """Test deleting a collection."""
    # Mock the Qdrant client
    mock_client = MagicMock()
    
    # Set up mock collection response
    mock_collection = MagicMock()
    mock_collection.name = "test-collection"
    mock_collections_response = MagicMock()
    mock_collections_response.collections = [mock_collection]
    mock_client.get_collections.return_value = mock_collections_response
    
    # Test successful deletion
    with patch('qdrant_manager.cli.logger') as mock_logger:
        result = delete_collection(mock_client, "test-collection")
        
        # Check that the collection was deleted
        mock_client.delete_collection.assert_called_once_with(collection_name="test-collection")
        assert result is True
        
    # Test deleting a non-existent collection
    with patch('qdrant_manager.cli.logger') as mock_logger:
        result = delete_collection(mock_client, "nonexistent-collection")
        
        # Check that nothing was deleted
        assert mock_client.delete_collection.call_count == 1  # Still only called once from previous test
        assert result is False
        
    # Test exception during deletion
    mock_client.delete_collection.side_effect = Exception("Deletion failed")
    
    with patch('qdrant_manager.cli.logger') as mock_logger:
        result = delete_collection(mock_client, "test-collection")
        
        # Check that error was logged
        mock_logger.error.assert_called()
        assert result is False


def test_list_collections():
    """Test listing collections."""
    # Mock the Qdrant client
    mock_client = MagicMock()
    
    # Set up mock collections
    mock_collection1 = MagicMock()
    mock_collection1.name = "collection1"
    mock_collection2 = MagicMock()
    mock_collection2.name = "collection2"
    
    mock_collections_response = MagicMock()
    mock_collections_response.collections = [mock_collection1, mock_collection2]
    mock_client.get_collections.return_value = mock_collections_response
    
    # Set up mock collection info
    mock_info1 = MagicMock()
    mock_info1.vectors_count = 100
    mock_info1.creation_time = "2023-01-01"
    
    mock_info2 = MagicMock()
    mock_info2.vectors_count = 200
    mock_info2.creation_time = "2023-01-02"
    
    mock_client.get_collection.side_effect = [mock_info1, mock_info2]
    
    # Test listing collections
    with patch('qdrant_manager.cli.logger') as mock_logger:
        result = list_collections(mock_client)
        
        # Check that collections were returned
        assert result == ["collection1", "collection2"]
        
        # Check that collection info was retrieved
        assert mock_client.get_collection.call_count == 2
        mock_client.get_collection.assert_any_call(collection_name="collection1")
        mock_client.get_collection.assert_any_call(collection_name="collection2")
    
    # Test with no collections
    mock_collections_response.collections = []
    
    with patch('qdrant_manager.cli.logger') as mock_logger:
        result = list_collections(mock_client)
        
        # Check that empty list was returned
        assert result == []
    
    # Test with exception
    mock_client.get_collections.side_effect = Exception("List failed")
    
    with patch('qdrant_manager.cli.logger') as mock_logger:
        result = list_collections(mock_client)
        
        # Check that error was logged
        mock_logger.error.assert_called()
        assert result == []


def test_collection_info():
    """Test getting collection info."""
    # Mock the Qdrant client
    mock_client = MagicMock()
    
    # Set up mock collections
    mock_collection = MagicMock()
    mock_collection.name = "test-collection"
    mock_collections_response = MagicMock()
    mock_collections_response.collections = [mock_collection]
    mock_client.get_collections.return_value = mock_collections_response
    
    # Set up mock collection info
    mock_info = MagicMock()
    mock_info.vectors_count = 100
    mock_info.creation_time = "2023-01-01"
    
    # Set up config and vector params
    mock_params = MagicMock()
    mock_params.size = 256
    mock_params.distance = "cosine"
    
    mock_config = MagicMock()
    mock_config.params = mock_params
    
    mock_info.config = mock_config
    
    # Set up mock count response
    mock_count = MagicMock()
    mock_count.count = 100
    
    # Set up mock scroll response
    mock_point = MagicMock()
    mock_point.payload = {"field1": "value1", "tags": {"tag1": 0.9, "tag2": 0.8}}
    
    # Configure the mocks
    mock_client.get_collection.return_value = mock_info
    mock_client.count.return_value = mock_count
    mock_client.scroll.return_value = ([mock_point], None)
    
    # Test getting collection info
    with patch('qdrant_manager.cli.logger') as mock_logger:
        result = collection_info(mock_client, "test-collection")
        
        # Check that collection info was retrieved
        mock_client.get_collection.assert_called_once_with(collection_name="test-collection")
        assert result == mock_info
    
    # Test with a non-existent collection
    mock_collections_response.collections = []  # No collections exist
    
    with patch('qdrant_manager.cli.logger') as mock_logger:
        result = collection_info(mock_client, "nonexistent-collection")
        
        # Check that no extra calls were made
        assert mock_client.get_collection.call_count == 1  # Still only called once
        assert result is None
    
    # Test with an exception
    mock_collections_response.collections = [mock_collection]  # Collection exists again
    mock_client.get_collection.side_effect = Exception("Info retrieval failed")
    
    # Import traceback directly in the test
    import traceback
    
    with patch('qdrant_manager.cli.logger') as mock_logger:
        # We no longer need to patch traceback - the function imports it internally
        result = collection_info(mock_client, "test-collection")
        
        # Check that error was logged
        mock_logger.error.assert_called()
        assert result is None


def test_list_collections_with_error_getting_info():
    """Test list collections with error when getting info for a collection."""
    # Mock the Qdrant client
    mock_client = MagicMock()
    
    # Set up mock collections
    mock_collection1 = MagicMock()
    mock_collection1.name = "collection1"
    
    mock_collections_response = MagicMock()
    mock_collections_response.collections = [mock_collection1]
    mock_client.get_collections.return_value = mock_collections_response
    
    # Set up get_collection to fail with exception
    mock_client.get_collection.side_effect = Exception("Error getting collection info")
    
    # Test listing collections with info error
    with patch('qdrant_manager.cli.logger') as mock_logger:
        result = list_collections(mock_client)
        
        # Should still return the collection list despite the error
        assert result == ["collection1"]
        
        # Should have called get_collection
        mock_client.get_collection.assert_called_once()
        
        # Should have logged the error
        mock_logger.error.assert_called()
