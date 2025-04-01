"""Tests for batch operations."""
import pytest
from unittest.mock import patch, MagicMock, mock_open
import json

# Import the main batch function and helpers
from qdrant_manager.commands.batch import batch_operations, _parse_ids, _parse_filter, _parse_doc
from qdrant_client.http.models import PointIdsList, Filter, FieldCondition, MatchValue, UpdateStatus, UpdateResult

def test_batch_operations():
    """Test the main batch operations function."""
    # Mock the Qdrant client
    mock_client = MagicMock()

    # Set up mock points
    mock_point1 = MagicMock()
    mock_point1.id = 1
    mock_point1.payload = {"field1": "value1"}

    mock_point2 = MagicMock()
    mock_point2.id = 2
    mock_point2.payload = {"field1": "value2"}

    # Configure mock client responses (adjust based on actual batch.py implementation)
    mock_client.retrieve.return_value = [mock_point1, mock_point2]
    mock_client.scroll.return_value = ([mock_point1, mock_point2], None)
    # Mock the blocking operations
    mock_client.set_payload_blocking.return_value = UpdateResult(operation_id=0, status=UpdateStatus.COMPLETED)
    mock_client.delete_payload_blocking.return_value = UpdateResult(operation_id=1, status=UpdateStatus.COMPLETED)
    mock_client.overwrite_payload_blocking.return_value = UpdateResult(operation_id=2, status=UpdateStatus.COMPLETED)

    # --- Test add operation with IDs --- 
    mock_args_add = MagicMock()
    mock_args_add.id_file = None
    mock_args_add.ids = "1,2"
    mock_args_add.filter = None
    mock_args_add.add = True
    mock_args_add.delete = False
    mock_args_add.replace = False
    mock_args_add.doc = '{"new_field": "new_value"}'
    mock_args_add.selector = None 
    mock_args_add.limit = 10000 # Default

    with patch('qdrant_manager.commands.batch.logger') as mock_logger_add:
        batch_operations(mock_client, "test-collection", mock_args_add)
        mock_client.set_payload_blocking.assert_called_once()
        # Verify points selector was PointIdsList
        call_args, call_kwargs = mock_client.set_payload_blocking.call_args
        assert isinstance(call_kwargs['points'], PointIdsList)
        assert call_kwargs['points'].points == ['1', '2']
        assert call_kwargs['payload'] == {"new_field": "new_value"}

    # --- Test delete operation with filter --- 
    mock_client.reset_mock()
    mock_args_delete = MagicMock()
    mock_args_delete.id_file = None
    mock_args_delete.ids = None
    mock_args_delete.filter = '{"key":"field1", "match":{"value":"value1"}}'
    mock_args_delete.add = False
    mock_args_delete.delete = True
    mock_args_delete.replace = False
    mock_args_delete.doc = None
    mock_args_delete.selector = "field1" 
    mock_args_delete.limit = 10000 # Default

    with patch('qdrant_manager.commands.batch.logger') as mock_logger_delete:
        batch_operations(mock_client, "test-collection", mock_args_delete)
        mock_client.delete_payload_blocking.assert_called_once()
        call_args, call_kwargs = mock_client.delete_payload_blocking.call_args
        # Verify points selector was Filter
        assert isinstance(call_kwargs['points'], Filter)
        assert call_kwargs['keys'] == ["field1"]

    # --- Test replace operation with IDs --- 
    mock_client.reset_mock()
    mock_args_replace = MagicMock()
    mock_args_replace.id_file = None
    mock_args_replace.ids = "1"
    mock_args_replace.filter = None
    mock_args_replace.add = False
    mock_args_replace.delete = False
    mock_args_replace.replace = True
    mock_args_replace.doc = '{"new_field": "replace_value"}'
    mock_args_replace.selector = "metadata" 
    mock_args_replace.limit = 10000 # Default

    with patch('qdrant_manager.commands.batch.logger') as mock_logger_replace:
        batch_operations(mock_client, "test-collection", mock_args_replace)
        mock_client.overwrite_payload_blocking.assert_called_once()
        call_args, call_kwargs = mock_client.overwrite_payload_blocking.call_args
        # Verify points selector was PointIdsList
        assert isinstance(call_kwargs['points'], PointIdsList)
        assert call_kwargs['points'].points == ['1']
        assert call_kwargs['payload'] == {"metadata": {"new_field": "replace_value"}}

    # --- Test invalid operation (no add/delete/replace) --- 
    mock_args_invalid_op = MagicMock()
    mock_args_invalid_op.id_file = None # Ensure all relevant attrs are set
    mock_args_invalid_op.ids = "1"
    mock_args_invalid_op.filter = None # Explicitly set filter to None
    mock_args_invalid_op.add = False
    mock_args_invalid_op.delete = False
    mock_args_invalid_op.replace = False
    mock_args_invalid_op.doc = None # Explicitly set doc to None
    
    with patch('qdrant_manager.commands.batch.logger') as mock_logger_invalid_op:
        batch_operations(mock_client, "test-collection", mock_args_invalid_op)
        mock_logger_invalid_op.error.assert_any_call("Batch command requires an operation type: --add, --delete, or --replace.")

    # --- Test no points selector --- 
    mock_args_no_points = MagicMock()
    mock_args_no_points.id_file = None
    mock_args_no_points.ids = None
    mock_args_no_points.filter = None # Explicitly set filter to None
    mock_args_no_points.add = True
    mock_args_no_points.doc = '{}'

    with patch('qdrant_manager.commands.batch.logger') as mock_logger_no_points:
        batch_operations(mock_client, "test-collection", mock_args_no_points)
        mock_logger_no_points.error.assert_any_call("Batch command requires --ids, --id-file, or --filter.")

# Mock Qdrant client and args
@pytest.fixture
def mock_qdrant_client():
    return MagicMock()

def test_parse_ids_file(tmp_path):
    """Test parsing IDs from a file."""
    id_file = tmp_path / "ids.txt"
    id_file.write_text("id1\nid2\n\nid3")
    args = MagicMock(id_file=str(id_file), ids=None)
    ids = _parse_ids(args)
    assert ids == ["id1", "id2", "id3"]

def test_parse_ids_args():
    """Test parsing IDs from comma-separated string."""
    args = MagicMock(id_file=None, ids="id1, id2 ,, id3")
    ids = _parse_ids(args)
    assert ids == ["id1", "id2", "id3"]

def test_parse_ids_none():
    """Test parsing IDs when neither file nor string is provided."""
    args = MagicMock(id_file=None, ids=None)
    ids = _parse_ids(args)
    assert ids == []

def test_parse_filter_valid():
    """Test parsing a valid filter JSON."""
    args = MagicMock(filter='{"key":"category", "match":{"value":"product"}}')
    q_filter = _parse_filter(args)
    assert isinstance(q_filter, Filter)
    assert len(q_filter.must) == 1
    assert q_filter.must[0].key == "category"
    assert isinstance(q_filter.must[0].match, MatchValue)
    assert q_filter.must[0].match.value == "product"

def test_parse_filter_invalid_json():
    """Test parsing invalid filter JSON."""
    args = MagicMock(filter='{"key":"category", }')
    with patch('qdrant_manager.commands.batch.logger') as mock_logger:
        q_filter = _parse_filter(args)
        assert q_filter is None
        mock_logger.error.assert_called_with('Invalid JSON in filter: {"key":"category", }')

def test_parse_filter_invalid_structure():
    """Test parsing filter JSON with incorrect structure."""
    args = MagicMock(filter='{"field":"category"}')
    with patch('qdrant_manager.commands.batch.logger') as mock_logger:
        q_filter = _parse_filter(args)
        assert q_filter is None
        mock_logger.error.assert_called_with("Invalid filter structure. Must contain 'key' and 'match'.")

def test_parse_filter_none():
    """Test parsing filter when arg is None."""
    args = MagicMock(filter=None)
    q_filter = _parse_filter(args)
    assert q_filter is None

def test_parse_doc_valid():
    """Test parsing valid document JSON."""
    args = MagicMock(doc='{"field1": "value1", "nested": {"key": 1}}')
    doc = _parse_doc(args)
    assert doc == {"field1": "value1", "nested": {"key": 1}}

def test_parse_doc_invalid():
    """Test parsing invalid document JSON."""
    args = MagicMock(doc='{"field1": }')
    with patch('qdrant_manager.commands.batch.logger') as mock_logger:
        doc = _parse_doc(args)
        assert doc is None
        mock_logger.error.assert_called_with('Invalid JSON in doc: {"field1": }')

def test_parse_doc_none():
    """Test parsing doc when arg is None."""
    args = MagicMock(doc=None)
    doc = _parse_doc(args)
    assert doc is None


# Keep test_batch_operations_with_mock_client if it tests batch_operations correctly
# It might need updates based on the changes in batch.py (e.g., using *_payload_blocking)
def test_batch_operations_with_mock_client(mock_qdrant_client):
    """Test batch operations with a mock Qdrant client (using *_payload_blocking)."""
    
    # Set up mock client methods used by batch_operations
    mock_qdrant_client.set_payload_blocking.return_value = UpdateResult(operation_id=0, status=UpdateStatus.COMPLETED)
    mock_qdrant_client.delete_payload_blocking.return_value = UpdateResult(operation_id=1, status=UpdateStatus.COMPLETED)
    mock_qdrant_client.overwrite_payload_blocking.return_value = UpdateResult(operation_id=2, status=UpdateStatus.COMPLETED)

    # --- Test Add Operation --- 
    mock_args_add = MagicMock()
    mock_args_add.id_file = None
    mock_args_add.ids = "1,2"
    mock_args_add.filter = None
    mock_args_add.add = True
    mock_args_add.delete = False
    mock_args_add.replace = False
    mock_args_add.doc = '{"new_field": "new_value"}'
    mock_args_add.selector = None
    mock_args_add.limit = 10000

    with patch('qdrant_manager.commands.batch.logger') as mock_logger_add:
        batch_operations(mock_qdrant_client, "test-collection", mock_args_add)
        mock_qdrant_client.set_payload_blocking.assert_called_once()
        call_args, call_kwargs = mock_qdrant_client.set_payload_blocking.call_args
        assert isinstance(call_kwargs['points'], PointIdsList)
        assert call_kwargs['points'].points == ['1', '2']
        assert call_kwargs['payload'] == {"new_field": "new_value"}
        mock_qdrant_client.delete_payload_blocking.assert_not_called()
        mock_qdrant_client.overwrite_payload_blocking.assert_not_called()

    # --- Test Delete Operation --- 
    mock_qdrant_client.reset_mock()
    mock_args_delete = MagicMock()
    mock_args_delete.id_file = None
    mock_args_delete.ids = None
    mock_args_delete.filter = '{"key":"field1", "match":{"value":"val"}}'
    mock_args_delete.add = False
    mock_args_delete.delete = True
    mock_args_delete.replace = False
    mock_args_delete.doc = None
    mock_args_delete.selector = "metadata.field_to_delete"
    mock_args_delete.limit = 50

    with patch('qdrant_manager.commands.batch.logger') as mock_logger_delete:
        batch_operations(mock_qdrant_client, "test-collection", mock_args_delete)
        mock_qdrant_client.delete_payload_blocking.assert_called_once()
        call_args, call_kwargs = mock_qdrant_client.delete_payload_blocking.call_args
        assert isinstance(call_kwargs['points'], Filter) # Should use filter
        assert call_kwargs['keys'] == ["metadata.field_to_delete"]
        mock_qdrant_client.set_payload_blocking.assert_not_called()
        mock_qdrant_client.overwrite_payload_blocking.assert_not_called()

    # --- Test Replace Operation (requires IDs) --- 
    mock_qdrant_client.reset_mock()
    mock_args_replace = MagicMock()
    mock_args_replace.id_file = None
    mock_args_replace.ids = "3"
    mock_args_replace.filter = None
    mock_args_replace.add = False
    mock_args_replace.delete = False
    mock_args_replace.replace = True
    mock_args_replace.doc = '{"new_data": true}'
    mock_args_replace.selector = "payload_root"
    mock_args_replace.limit = 10000

    with patch('qdrant_manager.commands.batch.logger') as mock_logger_replace:
        batch_operations(mock_qdrant_client, "test-collection", mock_args_replace)
        mock_qdrant_client.overwrite_payload_blocking.assert_called_once()
        call_args, call_kwargs = mock_qdrant_client.overwrite_payload_blocking.call_args
        assert isinstance(call_kwargs['points'], PointIdsList)
        assert call_kwargs['points'].points == ['3']
        assert call_kwargs['payload'] == {"payload_root": {"new_data": True}}
        mock_qdrant_client.set_payload_blocking.assert_not_called()
        mock_qdrant_client.delete_payload_blocking.assert_not_called()

    # --- Test Replace Operation with Filter (should log error) --- 
    mock_qdrant_client.reset_mock()
    mock_args_replace_filter = MagicMock()
    mock_args_replace_filter.id_file = None
    mock_args_replace_filter.ids = None
    mock_args_replace_filter.filter = '{"key":"field1", "match":{"value":"val"}}'
    mock_args_replace_filter.add = False
    mock_args_replace_filter.delete = False
    mock_args_replace_filter.replace = True
    mock_args_replace_filter.doc = '{"new_data": true}'
    mock_args_replace_filter.selector = "payload_root"
    mock_args_replace_filter.limit = 10000

    with patch('qdrant_manager.commands.batch.logger') as mock_logger_replace_filter:
        batch_operations(mock_qdrant_client, "test-collection", mock_args_replace_filter)
        mock_qdrant_client.overwrite_payload_blocking.assert_not_called()
        mock_logger_replace_filter.error.assert_any_call("Overwrite/Replace operation currently only supports --ids or --id-file, not --filter.") 