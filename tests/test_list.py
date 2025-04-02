import pytest
import io # Needed for capturing print output
import contextlib # Needed for capturing print output
from unittest.mock import patch, MagicMock
import requests # Import for exception types

# Ensure the function under test is imported
from solr_manager.commands.list import list_collections
from requests.exceptions import ConnectionError

@patch('solr_manager.commands.list.requests.get')
@patch('solr_manager.commands.list.get_admin_base_url')
@patch('solr_manager.commands.list.get_auth_tuple')
@patch('solr_manager.commands.list.logger')
def test_list_collections_success(mock_logger, mock_get_auth, mock_get_admin_url, mock_requests_get):
    """Test successful listing of collections."""
    mock_config = {'solr_url': 'http://fake-solr:8983/solr', 'timeout': 10}
    mock_admin_url = "http://fake-solr:8983/solr"
    mock_collections = ['collection1', 'collection2', 'collection3']
    
    mock_response = MagicMock()
    mock_response.json.return_value = {'collections': mock_collections}
    mock_requests_get.return_value = mock_response
    mock_get_admin_url.return_value = mock_admin_url
    mock_get_auth.return_value = None
    
    # Capture print output
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        list_collections(config=mock_config)
    
    # Verify output
    output_str = output.getvalue()
    assert "Available collections:" in output_str
    for collection in mock_collections:
        assert f"  - {collection}" in output_str
    
    # Verify correct URL and parameters
    expected_url = f"{mock_admin_url}/admin/collections?action=LIST&wt=json"
    mock_requests_get.assert_called_once_with(
        expected_url,
        auth=None,
        timeout=10
    )

@patch('solr_manager.commands.list.requests.get')
@patch('solr_manager.commands.list.get_admin_base_url')
@patch('solr_manager.commands.list.get_auth_tuple')
@patch('solr_manager.commands.list.logger')
def test_list_collections_empty(mock_logger, mock_get_auth, mock_get_admin_url, mock_requests_get):
    """Test listing when no collections exist."""
    mock_config = {'solr_url': 'http://fake-solr:8983/solr'}
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_response = MagicMock()
    mock_response.json.return_value = {'collections': []}
    mock_requests_get.return_value = mock_response
    mock_get_admin_url.return_value = mock_admin_url
    mock_get_auth.return_value = None
    
    # Capture print output
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        list_collections(config=mock_config)
    
    assert "No collections found." in output.getvalue()

@patch('solr_manager.commands.list.get_admin_base_url')
@patch('solr_manager.commands.list.logger')
def test_list_collections_no_base_url(mock_logger, mock_get_admin_url):
    """Test handling when base URL cannot be determined."""
    mock_config = {}
    mock_get_admin_url.return_value = None
    
    list_collections(config=mock_config)
    
    mock_logger.error.assert_called_once_with(
        "Could not determine Solr base URL from configuration."
    )

@patch('solr_manager.commands.list.requests.get')
@patch('solr_manager.commands.list.get_admin_base_url')
@patch('solr_manager.commands.list.get_auth_tuple')
@patch('solr_manager.commands.list.logger')
def test_list_collections_timeout(mock_logger, mock_get_auth, mock_get_admin_url, mock_requests_get):
    """Test handling of timeout errors."""
    mock_config = {'solr_url': 'http://fake-solr:8983/solr', 'timeout': 5}
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_get_admin_url.return_value = mock_admin_url
    mock_get_auth.return_value = None
    mock_requests_get.side_effect = requests.exceptions.Timeout()
    
    list_collections(config=mock_config)
    
    mock_logger.error.assert_called_once_with(
        f"Timeout occurred while trying to connect to {mock_admin_url}/admin/collections?action=LIST&wt=json"
    )

@patch('solr_manager.commands.list.requests.get')
@patch('solr_manager.commands.list.get_admin_base_url')
@patch('solr_manager.commands.list.get_auth_tuple')
@patch('solr_manager.commands.list.logger')
def test_list_collections_unexpected_format(mock_logger, mock_get_auth, mock_get_admin_url, mock_requests_get):
    """Test handling of unexpected response format."""
    mock_config = {'solr_url': 'http://fake-solr:8983/solr'}
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_response = MagicMock()
    mock_response.text = "Invalid response"
    mock_response.json.return_value = {'unexpected': 'format'}  # No collections key
    mock_requests_get.return_value = mock_response
    mock_get_admin_url.return_value = mock_admin_url
    mock_get_auth.return_value = None
    
    list_collections(config=mock_config)
    
    mock_logger.error.assert_called_once_with(
        "Unexpected response format when listing collections. Response: Invalid response"
    )

@patch('solr_manager.commands.list.requests.get')
@patch('solr_manager.commands.list.get_admin_base_url')
@patch('solr_manager.commands.list.get_auth_tuple')
@patch('solr_manager.commands.list.logger')
def test_list_collections_error_response(mock_logger, mock_get_auth, mock_get_admin_url, mock_requests_get):
    """Test handling of error response from Solr."""
    mock_config = {'solr_url': 'http://fake-solr:8983/solr'}
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_response = MagicMock()
    mock_response.json.return_value = {
        'error': {'msg': 'Something went wrong'}
    }
    mock_requests_get.return_value = mock_response
    mock_get_admin_url.return_value = mock_admin_url
    mock_get_auth.return_value = None
    
    list_collections(config=mock_config)
    
    mock_logger.error.assert_called_once_with(
        "Failed to list collections: Something went wrong"
    )

@patch('solr_manager.commands.list.requests.get')
@patch('solr_manager.commands.list.get_admin_base_url')
@patch('solr_manager.commands.list.get_auth_tuple')
@patch('solr_manager.commands.list.logger')
@patch('traceback.print_exc')
def test_list_collections_unexpected_error(mock_print_exc, mock_logger, mock_get_auth, mock_get_admin_url, mock_requests_get):
    """Test handling of unexpected errors with traceback."""
    mock_config = {'solr_url': 'http://fake-solr:8983/solr'}
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_get_admin_url.return_value = mock_admin_url
    mock_get_auth.return_value = None
    mock_requests_get.side_effect = Exception("Something unexpected happened")
    
    list_collections(config=mock_config)
    
    mock_logger.error.assert_called_once_with(
        "An unexpected error occurred while listing collections: Something unexpected happened"
    )
    mock_print_exc.assert_called_once()

@patch('solr_manager.commands.list.requests.get')
@patch('solr_manager.commands.list.get_admin_base_url')
@patch('solr_manager.commands.list.get_auth_tuple')
@patch('solr_manager.commands.list.logger')
def test_list_collections_with_auth(mock_logger, mock_get_auth, mock_get_admin_url, mock_requests_get):
    """Test list_collections with authentication."""
    mock_config = {
        'solr_url': 'http://fake-solr:8983/solr',
        'username': 'admin',
        'password': 'secret'
    }
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_response = MagicMock()
    mock_response.json.return_value = {'collections': ['collection1']}
    mock_requests_get.return_value = mock_response
    mock_get_admin_url.return_value = mock_admin_url
    mock_get_auth.return_value = ('admin', 'secret')
    
    list_collections(config=mock_config)
    
    # Verify auth tuple was passed
    call_kwargs = mock_requests_get.call_args[1]
    assert call_kwargs['auth'] == ('admin', 'secret')

@patch('solr_manager.commands.list.requests.get')
@patch('solr_manager.commands.list.get_admin_base_url')
@patch('solr_manager.commands.list.logger')
def test_list_collections_http_error(mock_logger, mock_get_admin_url, mock_requests_get):
    """Test handling of HTTP errors."""
    mock_config = {'solr_url': 'http://fake-solr:8983/solr', 'timeout': 10}
    mock_admin_url = "http://fake-solr:8983/solr"
    
    # Simulate an HTTP error with a response attribute
    mock_http_response = MagicMock()
    mock_http_response.status_code = 404
    mock_http_response.reason = "Not Found"
    mock_http_response.text = "Collection not found body"
    http_error = requests.exceptions.HTTPError("404 Client Error", response=mock_http_response)
    
    mock_response_trigger = MagicMock() # Ensure this is defined
    mock_response_trigger.raise_for_status.side_effect = http_error
    
    mock_get_admin_url.return_value = mock_admin_url
    mock_requests_get.return_value = mock_response_trigger # Assign the correct mock
    
    list_collections(config=mock_config)
    
    mock_get_admin_url.assert_called_once_with(mock_config)
    mock_requests_get.assert_called_once()
    mock_response_trigger.raise_for_status.assert_called_once()
    # Check error log - expects two calls now
    assert mock_logger.error.call_count == 2
    log_call_args_1 = mock_logger.error.call_args_list[0][0] # First arg of first call
    log_call_args_2 = mock_logger.error.call_args_list[1][0] # First arg of second call
    assert "HTTP error occurred while listing collections: 404 Not Found" in log_call_args_1[0]
    assert "Response body: Collection not found body" in log_call_args_2[0]

@patch('solr_manager.commands.list.requests.get')
@patch('solr_manager.commands.list.get_admin_base_url')
@patch('solr_manager.commands.list.logger')
def test_list_collections_request_exception(mock_logger, mock_get_admin_url, mock_requests_get):
    """Test handling of general request exceptions."""
    mock_config = {'solr_url': 'http://fake-solr:8983/solr', 'timeout': 5}
    mock_admin_url = "http://fake-solr:8983/solr" # Ensure this is defined
    connection_error = requests.exceptions.ConnectionError("Connection refused")
    mock_requests_get.side_effect = connection_error
    
    mock_get_admin_url.return_value = mock_admin_url
    
    list_collections(config=mock_config)
    
    mock_get_admin_url.assert_called_once_with(mock_config)
    mock_requests_get.assert_called_once()
    mock_logger.error.assert_called_once()
    log_call_args = mock_logger.error.call_args[0]
    # The exception is part of the first argument now
    assert "Connection error occurred while trying to connect to" in log_call_args[0]
    assert "Connection refused" in log_call_args[0]