"""Tests for point retrieval operations."""
import pytest
from unittest.mock import MagicMock, patch
import json
import csv
from io import StringIO

from qdrant_manager.commands.get import get_points, _parse_ids_for_get, _parse_filter_for_get
from qdrant_client.http.models import PointStruct, Filter, FieldCondition, MatchValue, ScoredPoint


def test_get_points_by_ids():
    """Test retrieving points by IDs."""
    # Mock the Qdrant client
    mock_client = MagicMock()

    # Set up mock points
    mock_point1 = MagicMock()
    mock_point1.id = 1
    mock_point1.payload = {"field1": "value1"}
    mock_point1.vector = [0.1, 0.2, 0.3]

    mock_point2 = MagicMock()
    mock_point2.id = 2
    mock_point2.payload = {"field1": "value2"}
    mock_point2.vector = [0.4, 0.5, 0.6]

    # Configure retrieve_points to return mock points
    mock_client.retrieve.return_value = [mock_point1, mock_point2]

    # Test retrieving points
    with patch('qdrant_manager.commands.get.logger') as mock_logger:
        mock_args_ids = MagicMock()
        mock_args_ids.id_file = None
        mock_args_ids.ids = "1,2"
        mock_args_ids.filter = None
        mock_args_ids.with_vectors = False # Assume default
        mock_args_ids.format = "json"
        mock_args_ids.output = None
        mock_args_ids.limit = 10 # Default for get

        with patch('builtins.print') as mock_print, patch('qdrant_manager.commands.get.json.dump') as mock_json_dump:
             get_points(mock_client, "test-collection", mock_args_ids)

             # Check that points were retrieved using retrieve
             mock_client.retrieve.assert_called_once()
             mock_client.scroll.assert_not_called()
             # Check output (assuming JSON)
             mock_json_dump.assert_called_once()

    # Test with missing points (retrieve handles this, get_points logs info)
    mock_client.reset_mock()
    mock_client.retrieve.return_value = [mock_point1]  # Only one point found
    with patch('qdrant_manager.commands.get.logger') as mock_logger:
        mock_args_ids_missing = MagicMock(ids="1,99", id_file=None, filter=None, with_vectors=False, format="json", output=None, limit=10)
        with patch('builtins.print'), patch('qdrant_manager.commands.get.json.dump') as mock_json_dump:
            get_points(mock_client, "test-collection", mock_args_ids_missing)
            mock_client.retrieve.assert_called_once()
            mock_json_dump.assert_called_once() # Should still output found points
            # The function get_points itself doesn't log warnings for missing IDs
            # mock_logger.warning.assert_called_with("Document ID 99 not found")

    # Test with exception
    mock_client.reset_mock()
    mock_client.retrieve.side_effect = Exception("Retrieval failed")
    with patch('qdrant_manager.commands.get.logger') as mock_logger:
         mock_args_ids_err = MagicMock(ids="1,2", id_file=None, filter=None, with_vectors=False, format="json", output=None, limit=10)
         with patch('builtins.print'), patch('qdrant_manager.commands.get.json.dump'):
            get_points(mock_client, "test-collection", mock_args_ids_err)
            # Check that error was logged
            mock_logger.error.assert_called_with("Failed to retrieve points: Retrieval failed")


def test_get_points_by_filter():
    """Test retrieving points by filter."""
    # Mock the Qdrant client
    mock_client = MagicMock()

    # Set up mock points
    mock_point1 = MagicMock()
    mock_point1.id = 1
    mock_point1.payload = {"field1": "value1"}
    mock_point1.vector = [0.1, 0.2, 0.3]

    mock_point2 = MagicMock()
    mock_point2.id = 2
    mock_point2.payload = {"field1": "value2"}
    mock_point2.vector = [0.4, 0.5, 0.6]

    # Configure scroll to return mock points
    mock_client.scroll.side_effect = [
        ([mock_point1, mock_point2], None),  # First call returns points and no offset
        ([], None)  # Second call returns no points (end of results)
    ]

    # Test retrieving points with filter
    with patch('qdrant_manager.commands.get.logger') as mock_logger:
        mock_args_filter = MagicMock()
        mock_args_filter.id_file = None
        mock_args_filter.ids = None
        mock_args_filter.filter = '{"key":"field1", "match":{"value":"value1"}}'
        mock_args_filter.with_vectors = False
        mock_args_filter.format = "json"
        mock_args_filter.output = None
        mock_args_filter.limit = 10 # Default for get

        with patch('builtins.print') as mock_print, patch('qdrant_manager.commands.get.json.dump') as mock_json_dump:
            get_points(mock_client, "test-collection", mock_args_filter)

            # Check that points were retrieved using scroll
            mock_client.scroll.assert_called_once()
            mock_client.retrieve.assert_not_called()
            # Check output
            mock_json_dump.assert_called_once()

    # Test with no points found
    mock_client.reset_mock()
    mock_client.scroll.side_effect = [([], None)]
    with patch('qdrant_manager.commands.get.logger') as mock_logger:
        mock_args_filter_none = MagicMock(ids=None, id_file=None, filter='{"key":"field1", "match":{"value":"nonexistent"}}', 
                                          with_vectors=False, format="json", output=None, limit=10)
        with patch('builtins.print'), patch('qdrant_manager.commands.get.json.dump') as mock_json_dump:
            get_points(mock_client, "test-collection", mock_args_filter_none)
            mock_client.scroll.assert_called_once()
            # Check logger info message
            mock_logger.info.assert_called_with("No points found matching the criteria.")
            mock_json_dump.assert_not_called()

    # Test with exception
    mock_client.reset_mock()
    mock_client.scroll.side_effect = Exception("Scroll failed")
    with patch('qdrant_manager.commands.get.logger') as mock_logger:
        mock_args_filter_err = MagicMock(ids=None, id_file=None, filter='{"key":"f", "match":{"value":"v"}}', 
                                         with_vectors=False, format="json", output=None, limit=10)
        with patch('builtins.print'), patch('qdrant_manager.commands.get.json.dump'):
            get_points(mock_client, "test-collection", mock_args_filter_err)
            # Check that error was logged
            mock_logger.error.assert_called_with("Failed to retrieve points: Scroll failed") 


def test_parse_ids_for_get():
    """Test parsing document IDs for get operation."""
    import tempfile

    # Test parsing from args.ids string
    mock_args_ids = MagicMock()
    mock_args_ids.ids = "1,2,3" 
    mock_args_ids.id_file = None
    
    ids = _parse_ids_for_get(mock_args_ids)
    assert ids == ["1", "2", "3"]
    
    # Test parsing with whitespace and empty elements
    mock_args_whitespace = MagicMock()
    mock_args_whitespace.ids = "1, 2, , 3  "
    mock_args_whitespace.id_file = None
    
    ids = _parse_ids_for_get(mock_args_whitespace)
    assert ids == ["1", "2", "3"]  # Empty elements should be filtered out
    
    # Test parsing from ID file
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        temp_file.write("10\n20\n30\n")
        temp_file_path = temp_file.name
    
    try:
        mock_args_file = MagicMock()
        mock_args_file.ids = None
        mock_args_file.id_file = temp_file_path
        
        ids = _parse_ids_for_get(mock_args_file)
        assert ids == ["10", "20", "30"]
        
        # Test with file not found
        mock_args_missing_file = MagicMock()
        mock_args_missing_file.ids = None
        mock_args_missing_file.id_file = "nonexistent_file.txt"
        
        with patch('qdrant_manager.commands.get.logger') as mock_logger:
            ids = _parse_ids_for_get(mock_args_missing_file)
            assert ids is None
            mock_logger.error.assert_called_once()
        
        # Test with neither ids nor id_file
        mock_args_none = MagicMock()
        mock_args_none.ids = None
        mock_args_none.id_file = None
        
        ids = _parse_ids_for_get(mock_args_none)
        assert ids is None
        
    finally:
        import os
        os.unlink(temp_file_path)  # Clean up


def test_parse_filter_for_get():
    """Test parsing filter for get operation."""
    # Test valid filter
    mock_args_valid = MagicMock()
    mock_args_valid.filter = '{"key":"field1", "match":{"value":"value1"}}'
    
    filter_obj = _parse_filter_for_get(mock_args_valid)
    assert isinstance(filter_obj, Filter)
    assert len(filter_obj.must) == 1
    assert filter_obj.must[0].key == "field1"
    assert filter_obj.must[0].match.value == "value1"
    
    # Test missing match.value
    mock_args_no_value = MagicMock()
    mock_args_no_value.filter = '{"key":"field1", "match":{}}'
    
    with patch('qdrant_manager.commands.get.logger') as mock_logger:
        filter_obj = _parse_filter_for_get(mock_args_no_value)
        assert filter_obj is None
        mock_logger.error.assert_called()
    
    # Test invalid structure
    mock_args_invalid = MagicMock()
    mock_args_invalid.filter = '{"invalid_key":"value"}'
    
    with patch('qdrant_manager.commands.get.logger') as mock_logger:
        filter_obj = _parse_filter_for_get(mock_args_invalid)
        assert filter_obj is None
        mock_logger.error.assert_called_with("Invalid filter structure. Must contain 'key' and 'match'.")
    
    # Test invalid JSON
    mock_args_bad_json = MagicMock()
    mock_args_bad_json.filter = '{"key":"value", invalid json'
    
    with patch('qdrant_manager.commands.get.logger') as mock_logger:
        filter_obj = _parse_filter_for_get(mock_args_bad_json)
        assert filter_obj is None
        mock_logger.error.assert_called_with(f"Invalid JSON in filter: {mock_args_bad_json.filter}")
    
    # Test None filter
    mock_args_none = MagicMock()
    mock_args_none.filter = None
    
    filter_obj = _parse_filter_for_get(mock_args_none)
    assert filter_obj is None


def test_get_points_csv_output():
    """Test retrieving points with CSV output."""
    # Mock the Qdrant client
    mock_client = MagicMock()
    
    # Create a PointStruct for more realistic testing
    point1 = PointStruct(
        id=1,
        payload={"name": "Item 1", "price": 10.5},
        vector=[0.1, 0.2, 0.3]
    )
    
    point2 = PointStruct(
        id=2,
        payload={"name": "Item 2", "price": 20.5},
        vector=[0.4, 0.5, 0.6]
    )
    
    # Test CSV output to stdout
    mock_client.retrieve.return_value = [point1, point2]
    
    with patch('qdrant_manager.commands.get.logger'), \
         patch('sys.stdout', new_callable=StringIO) as mock_stdout, \
         patch('qdrant_manager.commands.get.csv.DictWriter.writerow') as mock_writerow, \
         patch('qdrant_manager.commands.get.csv.DictWriter.writeheader') as mock_writeheader:
        
        mock_args_csv = MagicMock()
        mock_args_csv.ids = "1,2"
        mock_args_csv.id_file = None
        mock_args_csv.filter = None
        mock_args_csv.with_vectors = True
        mock_args_csv.format = "csv"
        mock_args_csv.output = None
        mock_args_csv.limit = 10
        
        get_points(mock_client, "test-collection", mock_args_csv)
        
        # Check that CSV format was used
        mock_writeheader.assert_called_once()
        assert mock_writerow.call_count == 2
    
    # Test CSV output to file
    with patch('qdrant_manager.commands.get.logger') as mock_logger, \
         patch('builtins.open', new_callable=MagicMock) as mock_open, \
         patch('qdrant_manager.commands.get.csv.DictWriter') as mock_dictwriter:
        
        mock_args_csv_file = MagicMock()
        mock_args_csv_file.ids = "1,2"
        mock_args_csv_file.id_file = None
        mock_args_csv_file.filter = None
        mock_args_csv_file.with_vectors = False
        mock_args_csv_file.format = "csv"
        mock_args_csv_file.output = "output.csv"
        mock_args_csv_file.limit = 10
        
        mock_client.retrieve.return_value = [point1, point2]
        
        get_points(mock_client, "test-collection", mock_args_csv_file)
        
        # Check that file was opened for writing
        mock_open.assert_called_once_with("output.csv", 'w', newline='')
        # Check logger message for file output
        mock_logger.info.assert_called_with("Output written to output.csv")

def test_get_points_with_named_vectors():
    """Test retrieving points with named vectors."""
    # Mock the Qdrant client
    mock_client = MagicMock()
    
    # Create points with named vectors
    point_with_named_vectors = PointStruct(
        id=1,
        payload={"name": "Item 1"},
        vector={"text": [0.1, 0.2], "image": [0.3, 0.4]}
    )
    
    mock_client.retrieve.return_value = [point_with_named_vectors]
    
    # Test JSON output with named vectors
    with patch('qdrant_manager.commands.get.logger'), \
         patch('builtins.print'), \
         patch('qdrant_manager.commands.get.json.dump') as mock_json_dump:
        
        mock_args = MagicMock()
        mock_args.ids = "1"
        mock_args.id_file = None
        mock_args.filter = None
        mock_args.with_vectors = True
        mock_args.format = "json"
        mock_args.output = None
        mock_args.limit = 10
        
        get_points(mock_client, "test-collection", mock_args)
        
        # Check that json.dump was called with the right data
        args, _ = mock_json_dump.call_args
        points_list = args[0]
        assert len(points_list) == 1
        assert "vector" in points_list[0]
        assert "text" in points_list[0]["vector"]
        assert "image" in points_list[0]["vector"]
    
    # Test CSV output with named vectors
    with patch('qdrant_manager.commands.get.logger'), \
         patch('sys.stdout', new_callable=StringIO) as mock_stdout, \
         patch('qdrant_manager.commands.get.csv.DictWriter.writerow') as mock_writerow, \
         patch('qdrant_manager.commands.get.csv.DictWriter.writeheader') as mock_writeheader:
        
        mock_args_csv = MagicMock()
        mock_args_csv.ids = "1"
        mock_args_csv.id_file = None
        mock_args_csv.filter = None
        mock_args_csv.with_vectors = True
        mock_args_csv.format = "csv"
        mock_args_csv.output = None
        mock_args_csv.limit = 10
        
        get_points(mock_client, "test-collection", mock_args_csv)
        
        # Check that CSV headers include vector names
        mock_writeheader.assert_called_once()
        mock_writerow.assert_called_once()

def test_get_points_empty_collection_name():
    """Test handling of empty collection name."""
    mock_client = MagicMock()
    
    with patch('qdrant_manager.commands.get.logger') as mock_logger:
        mock_args = MagicMock()
        
        # Call with empty collection name
        get_points(mock_client, "", mock_args)
        
        # Check that error was logged and no further calls were made
        mock_logger.error.assert_called_once_with("Collection name is required for 'get' command.")
        mock_client.retrieve.assert_not_called()
        mock_client.scroll.assert_not_called() 