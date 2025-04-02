import pytest
import io
import csv
import json
import contextlib
from unittest.mock import patch, MagicMock, call
import pysolr

from solr_manager.commands.get import get_documents

# Mock argparse Namespace specific to 'get' command
class MockGetArgs:
    def __init__(self, ids=None, id_file=None, query=None, fields='*', limit=10, sort=None, format='json', output=None):
        self.ids = ids
        self.id_file = id_file
        self.query = query
        self.fields = fields
        self.limit = limit
        self.sort = sort
        self.format = format
        self.output = output

# --- Test get_documents --- 

@patch('solr_manager.commands.get.logger')
@patch('solr_manager.commands.get._parse_ids')
@patch('builtins.open')
def test_get_documents_by_query_json_stdout(mock_open, mock_parse_ids, mock_logger):
    """Test successful get by query, JSON format, stdout."""
    mock_solr_client = MagicMock(spec=pysolr.Solr)
    mock_search_results = MagicMock()
    mock_docs = [
        {'id': 'doc1', 'title': 'Title 1'},
        {'id': 'doc2', 'title': 'Title 2'}
    ]
    mock_search_results.docs = mock_docs
    mock_search_results.hits = len(mock_docs)
    mock_solr_client.search.return_value = mock_search_results
    mock_parse_ids.return_value = [] # No IDs provided

    collection_name = "test_get_coll"
    query = "status:active"
    fields = "id,title"
    limit = 5
    sort = "id asc"
    args = MockGetArgs(query=query, fields=fields, limit=limit, sort=sort, format='json', output=None)

    stdout_capture = io.StringIO()
    with contextlib.redirect_stdout(stdout_capture):
        get_documents(client=mock_solr_client, collection_name=collection_name, args=args)
    
    output = stdout_capture.getvalue()
    
    mock_parse_ids.assert_called_once_with(args)
    # Check solr_client.search call
    expected_search_params = {
        'fl': fields,
        'rows': limit,
        'sort': sort
    }
    mock_solr_client.search.assert_called_once_with(q=query, **expected_search_params)
    # Check stdout output (JSON)
    assert output.strip() == json.dumps(mock_docs, indent=4)
    mock_logger.info.assert_any_call(f"Retrieved {len(mock_docs)} documents (limited by --limit={limit}).")
    mock_open.assert_not_called() # No file output

@patch('solr_manager.commands.get.logger')
@patch('solr_manager.commands.get._parse_ids')
@patch('builtins.open')
def test_get_documents_by_ids_csv_file(mock_open, mock_parse_ids, mock_logger):
    """Test successful get by IDs, CSV format, file output."""
    mock_solr_client = MagicMock(spec=pysolr.Solr)
    mock_search_results = MagicMock()
    mock_docs = [
        {'id': 'doc3', 'field_a': 'val_a', 'field_b': 10},
        {'id': 'doc4', 'field_a': 'val_b', 'field_b': 20}
    ]
    mock_search_results.docs = mock_docs
    mock_search_results.hits = len(mock_docs)
    mock_solr_client.search.return_value = mock_search_results
    
    mock_ids = ['doc3', 'doc4']
    mock_parse_ids.return_value = mock_ids
    output_file = '/fake/output.csv'
    
    args = MockGetArgs(ids=",".join(mock_ids), format='csv', output=output_file)

    # Simplify the open mock: return the handle directly
    mock_file_handle = io.StringIO()
    mock_open.return_value = mock_file_handle 
    # Also mock the close method on the handle so getvalue() works after
    mock_file_handle.close = MagicMock() 
    
    get_documents(client=mock_solr_client, collection_name="test_get_coll", args=args)
        
    mock_parse_ids.assert_called_once_with(args)
    # Check search query for IDs
    expected_id_query = f"id:(doc3 OR doc4)"
    expected_search_params = {
        'fl': '*',
        'rows': 10
    }
    mock_solr_client.search.assert_called_once_with(q=expected_id_query, **expected_search_params)
    
    # Check file write
    mock_open.assert_called_once_with(output_file, 'w', newline='', encoding='utf-8')
    # Assert close was called by the finally block
    mock_file_handle.close.assert_called_once()
    
    output_csv = mock_file_handle.getvalue()
    # Simple check for header and rows
    assert "id,field_a,field_b" in output_csv
    assert "doc3,val_a,10" in output_csv
    assert "doc4,val_b,20" in output_csv
    mock_logger.info.assert_any_call(f"Retrieved {len(mock_docs)} documents (limited by --limit={args.limit}).")
    mock_logger.info.assert_any_call(f"Successfully wrote {len(mock_docs)} documents to {output_file}")

@patch('solr_manager.commands.get.logger')
@patch('solr_manager.commands.get._parse_ids')
def test_get_documents_no_results(mock_parse_ids, mock_logger):
    """Test get documents when search returns no results."""
    mock_solr_client = MagicMock(spec=pysolr.Solr)
    mock_search_results = MagicMock()
    mock_search_results.docs = []
    mock_search_results.hits = 0
    mock_solr_client.search.return_value = mock_search_results
    mock_parse_ids.return_value = []

    args = MockGetArgs(query="non_existent_field:value")

    stdout_capture = io.StringIO()
    with contextlib.redirect_stdout(stdout_capture):
        get_documents(client=mock_solr_client, collection_name="test_get_coll", args=args)
    
    output = stdout_capture.getvalue()
    
    mock_parse_ids.assert_called_once_with(args)
    mock_solr_client.search.assert_called_once()
    assert output.strip() == "[]" # Expect empty JSON array
    mock_logger.info.assert_any_call("No documents found matching the criteria.")

@patch('solr_manager.commands.get.logger')
@patch('solr_manager.commands.get._parse_ids', side_effect=FileNotFoundError("ID file missing"))
def test_get_documents_id_file_not_found(mock_parse_ids, mock_logger):
    """Test get documents when the ID file is not found."""
    mock_solr_client = MagicMock(spec=pysolr.Solr)
    args = MockGetArgs(id_file="/fake/missing.ids")

    # Expect the function to log error and return
    get_documents(client=mock_solr_client, collection_name="test_get_coll", args=args)
    
    mock_parse_ids.assert_called_once_with(args)
    mock_solr_client.search.assert_not_called()
    # Check logger for the error caught by the main func
    mock_logger.error.assert_called_once_with("Error during ID parsing: ID file missing")

@patch('solr_manager.commands.get.logger')
@patch('solr_manager.commands.get._parse_ids')
def test_get_documents_solr_error(mock_parse_ids, mock_logger):
    """Test get documents when pysolr raises an error."""
    mock_solr_client = MagicMock(spec=pysolr.Solr)
    mock_solr_client.search.side_effect = pysolr.SolrError("Search failed")
    mock_parse_ids.return_value = []

    args = MockGetArgs(query="*:*")

    get_documents(client=mock_solr_client, collection_name="test_get_coll", args=args)
    
    mock_parse_ids.assert_called_once_with(args)
    mock_solr_client.search.assert_called_once()
    # Match the actual error log format
    mock_logger.error.assert_called_once_with("Solr search failed: Search failed")

@patch('solr_manager.commands.get.logger')
@patch('solr_manager.commands.get._parse_ids')
@patch('builtins.open', side_effect=IOError("Permission denied"))
def test_get_documents_output_file_error(mock_open, mock_parse_ids, mock_logger):
    """Test get documents when writing to output file fails."""
    mock_solr_client = MagicMock(spec=pysolr.Solr)
    mock_search_results = MagicMock()
    mock_docs = [{'id': 'doc1'}]
    mock_search_results.docs = mock_docs
    mock_search_results.hits = len(mock_docs)
    mock_solr_client.search.return_value = mock_search_results
    mock_parse_ids.return_value = []
    
    output_file = '/restricted/output.json'
    args = MockGetArgs(query="*:*", format='json', output=output_file)

    get_documents(client=mock_solr_client, collection_name="test_get_coll", args=args)
    
    mock_parse_ids.assert_called_once_with(args)
    mock_solr_client.search.assert_called_once()
    # Assert open call with correct args including encoding and newline
    mock_open.assert_called_once_with(output_file, 'w', newline='', encoding='utf-8')
    mock_logger.error.assert_called_once_with(f"Error writing to output file {output_file}: Permission denied") 