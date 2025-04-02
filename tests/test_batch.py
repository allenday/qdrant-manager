import pytest
import json
import io
from unittest.mock import patch, MagicMock, call, mock_open
import pysolr

from solr_manager.commands.batch import batch_operations

# Mock argparse Namespace
class MockBatchArgs:
    def __init__(self, action=None, file=None, profile=None, id_field='id', commit=False, add_update=False, delete_docs=False, ids=None, id_file=None, query=None, doc=None, batch_size=500):
        self.action = action
        self.file = file
        self.profile = profile
        self.id_field = id_field
        self.commit = commit
        self.add_update = add_update 
        self.delete_docs = delete_docs
        self.ids = ids
        self.id_file = id_file
        self.query = query
        self.doc = doc
        self.batch_size = batch_size
        if add_update and delete_docs:
             raise ValueError("Cannot use --add-update and --delete-docs together")

# --- Test batch_operations --- 

@patch('solr_manager.commands.batch.logger')
@patch('solr_manager.commands.batch._parse_docs_from_arg') 
@patch('solr_manager.commands.batch._parse_ids') 
def test_batch_add_success(mock_parse_ids, mock_parse_docs, mock_logger):
    """Test successful batch add operation."""
    # Instantiate the mock client inside the test
    mock_solr_client = MagicMock(spec=pysolr.Solr)
    
    mock_file_content = [
        {'id': 'doc1', 'field_a': 'value1'},
        {'id': 'doc2', 'field_a': 'value2'}
    ]
    # Mock the return value of the *parsing function*
    mock_parse_docs.return_value = mock_file_content
    mock_parse_ids.return_value = [] # No IDs needed for add
        
    # Use the args expected by batch_operations
    args = MockBatchArgs(add_update=True, doc='dummy_json_string', commit=True)
    # Config is no longer used directly by batch_operations
    mock_config = {} 
    collection_name = "test_add_coll" # Provide a collection name
    
    # Call batch_operations with the correct arguments
    batch_operations(client=mock_solr_client, collection_name=collection_name, args=args, config=mock_config)
    
    # Assert the helper functions were called
    mock_parse_docs.assert_called_once_with(args.doc)
    mock_parse_ids.assert_called_once_with(args)
    
    # Assert Solr client methods were called correctly
    # Note: commit=False is passed to add, final commit is separate
    mock_solr_client.add.assert_called_once_with(mock_file_content, commit=False) 
    mock_solr_client.commit.assert_called_once() # Assert final commit call
    
    # Check logs
    mock_logger.info.assert_any_call(f"Executing 'batch' command for collection: {collection_name}")
    mock_logger.info.assert_any_call(f"Batch add/update operation completed successfully for {len(mock_file_content)} documents.")

@patch('solr_manager.commands.batch.logger')
@patch('solr_manager.commands.batch._parse_docs_from_arg') 
@patch('solr_manager.commands.batch._parse_ids') 
@patch('solr_manager.commands.batch.tqdm', new=lambda x, **kwargs: x)  # Disable tqdm progress bar
def test_batch_delete_by_id_success(mock_parse_ids, mock_parse_docs, mock_logger):
    """Test successful batch delete by ID operation."""
    mock_solr_client = MagicMock(spec=pysolr.Solr)
    # _parse_docs_from_arg is NOT called if args.doc is None (default)
    # mock_parse_docs.return_value = None 

    mock_ids_to_delete = ['doc1', 'doc2']
    mock_parse_ids.return_value = mock_ids_to_delete
    
    # Test with commit=True
    args_commit = MockBatchArgs(delete_docs=True, ids=",".join(mock_ids_to_delete), commit=True)
    mock_config = {}
    collection_name = "test_del_id_coll"
    
    batch_operations(client=mock_solr_client, collection_name=collection_name, args=args_commit, config=mock_config)
    
    # Assertions:
    mock_parse_docs.assert_not_called() # Not called because args_commit.doc is None
    mock_parse_ids.assert_called_once_with(args_commit)
    mock_solr_client.delete.assert_called_once_with(id=mock_ids_to_delete, commit=True)
    mock_logger.info.assert_any_call(f"Executing 'batch' command for collection: {collection_name}")
    mock_logger.info.assert_any_call(f"Batch delete operation (IDs) completed successfully via pysolr.")
    
    # Reset mocks and test with commit=False
    mock_solr_client.reset_mock()
    mock_logger.reset_mock()
    mock_parse_ids.reset_mock()
    mock_parse_docs.reset_mock()
    mock_parse_ids.return_value = mock_ids_to_delete # Reset return value
    
    args_no_commit = MockBatchArgs(delete_docs=True, ids=",".join(mock_ids_to_delete), commit=False)
    batch_operations(client=mock_solr_client, collection_name=collection_name, args=args_no_commit, config=mock_config)
    
    # Assertions:
    mock_parse_docs.assert_not_called() # Not called because args_no_commit.doc is None
    mock_parse_ids.assert_called_once_with(args_no_commit)
    mock_solr_client.delete.assert_called_once_with(id=mock_ids_to_delete, commit=False)
    mock_logger.info.assert_any_call("Commit was skipped as per --no-commit flag.")

@patch('solr_manager.commands.batch.logger')
@patch('solr_manager.commands.batch._parse_docs_from_arg') 
@patch('solr_manager.commands.batch._parse_ids') 
@patch('solr_manager.commands.batch.tqdm', new=lambda x, **kwargs: x)  # Disable tqdm progress bar
def test_batch_delete_by_query_success(mock_parse_ids, mock_parse_docs, mock_logger):
    """Test successful batch delete by query operation."""
    mock_solr_client = MagicMock(spec=pysolr.Solr)
    # _parse_docs_from_arg is NOT called if args.doc is None (default)
    # mock_parse_docs.return_value = None 
    mock_parse_ids.return_value = [] # No IDs for query delete

    mock_query = "field_a:value1"
    
    # Args for delete by query
    args = MockBatchArgs(delete_docs=True, query=mock_query, commit=False) # Test no commit
    mock_config = {}
    collection_name = "test_del_q_coll"
    
    batch_operations(client=mock_solr_client, collection_name=collection_name, args=args, config=mock_config)
    
    # Assertions:
    mock_parse_docs.assert_not_called() # Not called because args.doc is None
    mock_parse_ids.assert_called_once_with(args)
    mock_solr_client.delete.assert_called_once_with(q=mock_query, commit=False)
    mock_logger.info.assert_any_call(f"Executing 'batch' command for collection: {collection_name}")
    mock_logger.info.assert_any_call(f"Batch delete operation (query) completed successfully via pysolr.")
    mock_logger.info.assert_any_call("Commit was skipped as per --no-commit flag.")

@patch('solr_manager.commands.batch.logger')
@patch('solr_manager.commands.batch._parse_docs_from_arg') 
@patch('solr_manager.commands.batch._parse_ids', side_effect=FileNotFoundError("File not found"))
@patch('solr_manager.commands.batch.tqdm', new=lambda x, **kwargs: x)  # Disable tqdm progress bar
def test_batch_file_not_found_during_id_parse(mock_parse_ids, mock_parse_docs, mock_logger):
    """Test batch operation when ID file does not exist (error during parsing)."""
    mock_solr_client = MagicMock(spec=pysolr.Solr)
    # _parse_docs_from_arg is NOT called if _parse_ids fails first
    
    args = MockBatchArgs(delete_docs=True, id_file='/fake/nonexistent.ids')
    mock_config = {}
    collection_name = "test_fnf_coll"
    
    # Call the function - should catch, log, and return
    batch_operations(client=mock_solr_client, collection_name=collection_name, args=args, config=mock_config)
        
    mock_parse_ids.assert_called_once_with(args)
    mock_parse_docs.assert_not_called() # Should not be called
    mock_solr_client.delete.assert_not_called()
    mock_solr_client.add.assert_not_called()
    # Check the new error log from batch_operations catching the exception
    mock_logger.error.assert_called_once_with(f"Error during ID parsing: File not found")

@patch('solr_manager.commands.batch.logger')
@patch('solr_manager.commands.batch._parse_docs_from_arg', side_effect=json.JSONDecodeError("Bad JSON", "", 0))
@patch('solr_manager.commands.batch._parse_ids') 
@patch('solr_manager.commands.batch.tqdm', new=lambda x, **kwargs: x)  # Disable tqdm progress bar
def test_batch_invalid_json_during_doc_parse(mock_parse_ids, mock_parse_docs, mock_logger):
    """Test batch operation with invalid JSON (error during doc parsing)."""
    mock_solr_client = MagicMock(spec=pysolr.Solr)
    mock_parse_ids.return_value = [] # Assume ID parsing is fine

    args = MockBatchArgs(add_update=True, doc='triggers_bad_json')
    mock_config = {}
    collection_name = "test_badjson_coll"

    # Call - should catch, log, and return
    batch_operations(client=mock_solr_client, collection_name=collection_name, args=args, config=mock_config)

    mock_parse_ids.assert_called_once_with(args)
    mock_parse_docs.assert_called_once_with(args.doc)
    mock_solr_client.add.assert_not_called()
    mock_solr_client.delete.assert_not_called()
    # Check the new error log from batch_operations catching the exception
    mock_logger.error.assert_called_once_with(f"Error during document/query parsing: Bad JSON: line 1 column 1 (char 0)")

@patch('solr_manager.commands.batch.logger')
@patch('solr_manager.commands.batch._parse_docs_from_arg') 
@patch('solr_manager.commands.batch._parse_ids') 
@patch('solr_manager.commands.batch.tqdm', new=lambda x, **kwargs: x)  # Disable tqdm progress bar
def test_batch_solr_error(mock_parse_ids, mock_parse_docs, mock_logger):
    """Test handling of pysolr.SolrError during batch operation."""
    mock_solr_client = MagicMock(spec=pysolr.Solr)
    mock_solr_client.add.side_effect = pysolr.SolrError("Solr commit failed")
    
    mock_docs = [{'id': 'doc1'}] 
    mock_parse_docs.return_value = mock_docs
    mock_parse_ids.return_value = []
    
    args = MockBatchArgs(add_update=True, doc='dummy')
    mock_config = {}
    collection_name = "test_solr_err_coll"
    
    batch_operations(client=mock_solr_client, collection_name=collection_name, args=args, config=mock_config)
        
    mock_parse_ids.assert_called_once_with(args)
    mock_parse_docs.assert_called_once_with(args.doc)
    mock_solr_client.add.assert_called_once_with(mock_docs, commit=False) 
    mock_logger.error.assert_called_once_with(f"Solr error during add/update operation: Solr commit failed")
    mock_solr_client.commit.assert_not_called() 

@patch('solr_manager.commands.batch.logger')
@patch('solr_manager.commands.batch._parse_docs_from_arg') 
@patch('solr_manager.commands.batch._parse_ids') 
@patch('solr_manager.commands.batch.tqdm', new=lambda x, **kwargs: x)  # Disable tqdm progress bar
def test_batch_general_exception(mock_parse_ids, mock_parse_docs, mock_logger):
    """Test handling of unexpected exceptions during batch operation."""
    mock_solr_client = MagicMock(spec=pysolr.Solr)
    mock_solr_client.delete.side_effect = ValueError("Unexpected issue")
    
    mock_query = "*:*"
    mock_parse_docs.return_value = None 
    mock_parse_ids.return_value = []
    
    # Args default to commit=True, so the code calls delete with commit=True
    args = MockBatchArgs(delete_docs=True, query=mock_query, commit=True) 
    mock_config = {}
    collection_name = "test_gen_exc_coll"
    
    batch_operations(client=mock_solr_client, collection_name=collection_name, args=args, config=mock_config)
        
    mock_parse_ids.assert_called_once_with(args)
    mock_parse_docs.assert_not_called() 
    # Assert based on args.commit = True
    mock_solr_client.delete.assert_called_once_with(q=mock_query, commit=True) 
    mock_logger.error.assert_called_once_with(f"An unexpected error occurred during delete (query): Unexpected issue")

@patch('solr_manager.commands.batch.logger')
@patch('solr_manager.commands.batch._parse_docs_from_arg') 
@patch('solr_manager.commands.batch._parse_ids') 
@patch('solr_manager.commands.batch.tqdm', new=lambda x, **kwargs: x)  # Disable tqdm progress bar
def test_batch_no_action_specified(mock_parse_ids, mock_parse_docs, mock_logger):
    """Test calling batch_operations without specifying an action."""
    mock_solr_client = MagicMock(spec=pysolr.Solr)
    mock_parse_ids.return_value = [] 
    # mock_parse_docs is NOT called if args.doc is None
    # mock_parse_docs.return_value = None

    args = MockBatchArgs() # No action flags set
    mock_config = {}
    collection_name = "test_no_action_coll"

    batch_operations(client=mock_solr_client, collection_name=collection_name, args=args, config=mock_config)
    
    mock_parse_ids.assert_called_once_with(args) 
    mock_parse_docs.assert_not_called() # Not called
    mock_logger.error.assert_called_once_with("No batch operation specified (use --add-update or --delete-docs).")
    mock_solr_client.add.assert_not_called()
    mock_solr_client.delete.assert_not_called()

# Removed test_batch_invalid_action as argparse handles this 

@patch('solr_manager.commands.batch.tqdm', new=lambda x, **kwargs: x)  # Disable tqdm progress bar
def test_parse_ids_file_not_found():
    """Test _parse_ids when the ID file doesn't exist."""
    from solr_manager.commands.batch import _parse_ids
    args = MagicMock()
    args.id_file = 'nonexistent_file.txt'
    args.ids = None
    
    result = _parse_ids(args)
    assert result is None

@patch('solr_manager.commands.batch.tqdm', new=lambda x, **kwargs: x)  # Disable tqdm progress bar
def test_parse_ids_file_read_error():
    """Test _parse_ids when there's an error reading the file."""
    from solr_manager.commands.batch import _parse_ids
    args = MagicMock()
    args.id_file = 'test_file.txt'
    args.ids = None

    mock_file = mock_open()
    mock_file.side_effect = IOError("Failed to read file")

    with patch('builtins.open', mock_file):
        result = _parse_ids(args)
        assert result is None

@patch('solr_manager.commands.batch.tqdm', new=lambda x, **kwargs: x)  # Disable tqdm progress bar
def test_parse_docs_invalid_json_structure():
    """Test _parse_docs_from_arg with invalid JSON structure."""
    from solr_manager.commands.batch import _parse_docs_from_arg
    
    # Test with a JSON array of non-objects
    result = _parse_docs_from_arg('[1, 2, 3]')
    assert result is None

@patch('solr_manager.commands.batch.tqdm', new=lambda x, **kwargs: x)  # Disable tqdm progress bar
def test_parse_docs_general_error():
    """Test _parse_docs_from_arg with a general error."""
    from solr_manager.commands.batch import _parse_docs_from_arg
    
    with patch('json.loads', side_effect=Exception("Mock error")):
        result = _parse_docs_from_arg('{"id": 1}')
    
    assert result is None

@patch('solr_manager.commands.batch.tqdm', new=lambda x, **kwargs: x)  # Disable tqdm progress bar
def test_batch_operations_id_parse_error():
    """Test batch_operations when ID parsing fails."""
    from solr_manager.commands.batch import batch_operations
    
    mock_client = MagicMock()
    args = MagicMock()
    args.delete_docs = True
    args.add_update = False
    args.id_file = 'test.txt'
    args.ids = None
    args.query = None
    
    with patch('solr_manager.commands.batch._parse_ids', side_effect=Exception("Mock parse error")):
        batch_operations(mock_client, 'test_collection', args, {})
    
    mock_client.delete.assert_not_called()

@patch('solr_manager.commands.batch.tqdm', new=lambda x, **kwargs: x)  # Disable tqdm progress bar
def test_batch_operations_delete_solr_error():
    """Test batch_operations when Solr returns an error during delete."""
    from solr_manager.commands.batch import batch_operations
    import pysolr
    
    mock_client = MagicMock()
    mock_client.delete.side_effect = pysolr.SolrError("Mock Solr error")
    
    args = MagicMock()
    args.delete_docs = True
    args.add_update = False
    args.ids = "1,2,3"
    args.id_file = None
    args.query = None
    args.commit = True
    
    batch_operations(mock_client, 'test_collection', args, {})
    
    mock_client.delete.assert_called_once()

@patch('solr_manager.commands.batch.tqdm', new=lambda x, **kwargs: x)  # Disable tqdm progress bar
def test_batch_operations_delete_unexpected_error():
    """Test batch_operations when an unexpected error occurs during delete."""
    from solr_manager.commands.batch import batch_operations
    
    mock_client = MagicMock()
    mock_client.delete.side_effect = ValueError("Unexpected issue")
    
    args = MagicMock()
    args.delete_docs = True
    args.add_update = False
    args.ids = "1,2,3"
    args.id_file = None
    args.query = None
    args.commit = True
    
    batch_operations(mock_client, 'test_collection', args, {})
    
    mock_client.delete.assert_called_once() 