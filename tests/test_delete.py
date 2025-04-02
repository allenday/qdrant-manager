import pytest
from unittest.mock import patch, MagicMock
import requests # For exceptions

from solr_manager.commands.delete import delete_collection

# --- Test delete_collection ---

@patch('solr_manager.commands.delete.requests.get')
@patch('solr_manager.commands.delete.get_admin_base_url')
@patch('solr_manager.commands.delete.logger')
def test_delete_collection_success(mock_logger, mock_get_admin_url, mock_requests_get):
    """Test successful deletion of a collection."""
    collection = "test_delete_me"
    mock_config = {'solr_url': 'http://fake-solr:8983/solr', 'timeout': 20}
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    # Simulate a successful response (often empty or has status 0)
    mock_response.json.return_value = {"responseHeader": {"status": 0, "QTime": 50}}
    
    mock_get_admin_url.return_value = mock_admin_url
    mock_requests_get.return_value = mock_response
    
    delete_collection(collection_name=collection, config=mock_config)
    
    mock_get_admin_url.assert_called_once_with(mock_config)
    expected_url = f"{mock_admin_url}/admin/collections?action=DELETE&name={collection}&wt=json"
    mock_requests_get.assert_called_once_with(expected_url, auth=None, timeout=20)
    mock_response.raise_for_status.assert_called_once()
    mock_logger.info.assert_called_with(
        f"Collection '{collection}' deleted successfully (or did not exist)."
    )

@patch('solr_manager.commands.delete.requests.get')
@patch('solr_manager.commands.delete.get_admin_base_url')
@patch('solr_manager.commands.delete.logger')
def test_delete_collection_http_error(mock_logger, mock_get_admin_url, mock_requests_get):
    """Test handling HTTP errors during deletion."""
    collection = "test_delete_fail"
    mock_config = {'solr_url': 'http://fake-solr:8983/solr'}
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_http_response = MagicMock()
    mock_http_response.status_code = 400
    mock_http_response.reason = "Bad Request"
    mock_http_response.text = "Collection not found or other error"
    http_error = requests.exceptions.HTTPError("400 Bad Request", response=mock_http_response)
    
    mock_response_trigger = MagicMock()
    mock_response_trigger.raise_for_status.side_effect = http_error
    
    mock_get_admin_url.return_value = mock_admin_url
    mock_requests_get.return_value = mock_response_trigger
    
    delete_collection(collection_name=collection, config=mock_config)
    
    mock_get_admin_url.assert_called_once_with(mock_config)
    mock_requests_get.assert_called_once()
    mock_response_trigger.raise_for_status.assert_called_once()
    # Check logger error calls
    assert mock_logger.error.call_count == 2
    log_call_args_1 = mock_logger.error.call_args_list[0][0]
    log_call_args_2 = mock_logger.error.call_args_list[1][0]
    assert f"HTTP error occurred while deleting collection '{collection}': 400 Bad Request" in log_call_args_1[0]
    assert "Collection not found or other error" in log_call_args_2[0]

@patch('solr_manager.commands.delete.get_admin_base_url')
@patch('solr_manager.commands.delete.logger')
def test_delete_collection_no_admin_url(mock_logger, mock_get_admin_url):
    """Test delete when admin URL cannot be determined."""
    collection = "test_col"
    mock_config = {}
    mock_get_admin_url.return_value = None
    
    delete_collection(collection_name=collection, config=mock_config)
    
    mock_get_admin_url.assert_called_once_with(mock_config)
    mock_logger.error.assert_called_once_with(
        "Could not determine Solr base URL from configuration."
    )

@patch('solr_manager.commands.delete.logger')
def test_delete_collection_missing_name(mock_logger):
    """Test delete_collection when collection name is missing."""
    delete_collection(None, {})
    
    mock_logger.error.assert_called_with("Collection name is required for 'delete' command.")

@patch('solr_manager.commands.delete.logger')
def test_delete_collection_unexpected_response(mock_logger):
    """Test delete_collection when response format is unexpected."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = '{"unexpected": "format"}'
    mock_response.json.return_value = {"unexpected": "format"}
    
    with patch('requests.get', return_value=mock_response), \
         patch('solr_manager.commands.delete.get_admin_base_url', return_value='http://localhost:8983/solr'):
        delete_collection('test_collection', {})
    
    mock_logger.error.assert_called_with(
        "Unexpected response format during delete. Status: 200, Response: {\"unexpected\": \"format\"}"
    )

@patch('solr_manager.commands.delete.logger')
def test_delete_collection_timeout(mock_logger):
    """Test delete_collection when request times out."""
    with patch('requests.get', side_effect=requests.exceptions.Timeout), \
         patch('solr_manager.commands.delete.get_admin_base_url', return_value='http://localhost:8983/solr'):
        delete_collection('test_collection', {})
    
    mock_logger.error.assert_called_with(
        "Timeout occurred while trying to delete collection 'test_collection' at http://localhost:8983/solr/admin/collections?action=DELETE&name=test_collection&wt=json"
    )

@patch('solr_manager.commands.delete.logger')
def test_delete_collection_connection_error(mock_logger):
    """Test delete_collection when connection error occurs."""
    with patch('requests.get', side_effect=requests.exceptions.ConnectionError("Mock connection error")), \
         patch('solr_manager.commands.delete.get_admin_base_url', return_value='http://localhost:8983/solr'):
        delete_collection('test_collection', {})
    
    mock_logger.error.assert_called_with(
        "Connection error occurred while trying to delete collection 'test_collection': Mock connection error"
    )

@patch('solr_manager.commands.delete.logger')
def test_delete_collection_unexpected_error(mock_logger):
    """Test delete_collection when an unexpected error occurs."""
    with patch('requests.get', side_effect=Exception("Mock unexpected error")), \
         patch('solr_manager.commands.delete.get_admin_base_url', return_value='http://localhost:8983/solr'):
        delete_collection('test_collection', {})
    
    mock_logger.error.assert_called_with(
        "An unexpected error occurred while deleting collection 'test_collection': Mock unexpected error"
    ) 