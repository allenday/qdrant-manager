import pytest
import io 
import contextlib
from unittest.mock import patch, MagicMock
import requests # For exceptions
import json

from solr_manager.commands.info import collection_info

# --- Test collection_info ---

@patch('solr_manager.commands.info.requests.get')
@patch('solr_manager.commands.info.get_admin_base_url')
def test_collection_info_success(mock_get_admin_url, mock_requests_get):
    """Test successful retrieval of collection info."""
    collection = "my_test_collection"
    mock_config = {'solr_url': 'http://fake-solr:8983/solr', 'timeout': 10}
    mock_admin_url = "http://fake-solr:8983/solr"
    mock_api_response = {
        "responseHeader": {"status": 0, "QTime": 5},
        "cluster": {
            "collections": {
                collection: {
                    "configName": "_default",
                    "shards": {"shard1": {}}
                }
            },
            "live_nodes": ["node1:8983_solr"]
        }
    }
    
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = mock_api_response
    
    mock_get_admin_url.return_value = mock_admin_url
    mock_requests_get.return_value = mock_response
    
    stdout_capture = io.StringIO()
    with contextlib.redirect_stdout(stdout_capture):
        collection_info(collection_name=collection, config=mock_config)
    output = stdout_capture.getvalue()

    mock_get_admin_url.assert_called_once_with(mock_config)
    expected_url = f"{mock_admin_url}/admin/collections?action=CLUSTERSTATUS&collection={collection}&wt=json"
    mock_requests_get.assert_called_once_with(expected_url, auth=None, timeout=10)
    mock_response.raise_for_status.assert_called_once()
    
    # Check that the output contains formatted JSON of the collection details
    assert f"Information for collection '{collection}':" in output
    try:
        # Extract the JSON part from the output
        json_output_str = output.split(":\n", 1)[1].strip()
        output_data = json.loads(json_output_str)
        assert output_data == mock_api_response["cluster"]["collections"][collection]
    except (IndexError, json.JSONDecodeError):
        pytest.fail(f"Could not parse JSON from output:\n{output}")

@patch('solr_manager.commands.info.requests.get')
@patch('solr_manager.commands.info.get_admin_base_url')
@patch('solr_manager.commands.info.logger')
def test_collection_info_not_found(mock_logger, mock_get_admin_url, mock_requests_get):
    """Test getting info for a non-existent collection."""
    collection = "non_existent_collection"
    mock_config = {'solr_url': 'http://fake-solr:8983/solr'}
    mock_admin_url = "http://fake-solr:8983/solr"
    mock_api_response = {
        "responseHeader": {"status": 0, "QTime": 3},
        "cluster": {
             "collections": {},
             "live_nodes": ["node1:8983_solr"]
        }
    } 
    
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = mock_api_response
    
    mock_get_admin_url.return_value = mock_admin_url
    mock_requests_get.return_value = mock_response

    # No stdout expected, check logger
    stdout_capture = io.StringIO()
    with contextlib.redirect_stdout(stdout_capture):
        collection_info(collection_name=collection, config=mock_config)
    output = stdout_capture.getvalue()
    assert output == "" # Ensure nothing was printed to stdout

    mock_get_admin_url.assert_called_once_with(mock_config)
    mock_requests_get.assert_called_once()
    mock_response.raise_for_status.assert_called_once()
    
    # Check logger error for not found message
    mock_logger.error.assert_called_once_with(
        f"Collection '{collection}' not found in cluster status."
    )
    # Optionally check that info was also called for available collections
    assert mock_logger.info.call_count > 0

@patch('solr_manager.commands.info.requests.get')
@patch('solr_manager.commands.info.get_admin_base_url')
@patch('solr_manager.commands.info.logger')
def test_collection_info_http_error(mock_logger, mock_get_admin_url, mock_requests_get):
    """Test handling of HTTP errors during info retrieval."""
    collection = "test_col"
    mock_config = {'solr_url': 'http://fake-solr:8983/solr'}
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_http_response = MagicMock()
    mock_http_response.status_code = 500
    mock_http_response.reason = "Server Error"
    mock_http_response.text = "Internal Server Error Detail"
    http_error = requests.exceptions.HTTPError("500 Server Error", response=mock_http_response)
    
    mock_response_trigger = MagicMock()
    mock_response_trigger.raise_for_status.side_effect = http_error
    
    mock_get_admin_url.return_value = mock_admin_url
    mock_requests_get.return_value = mock_response_trigger
    
    collection_info(collection_name=collection, config=mock_config)
    
    mock_get_admin_url.assert_called_once_with(mock_config)
    mock_requests_get.assert_called_once()
    mock_response_trigger.raise_for_status.assert_called_once()
    # Check logger error
    assert mock_logger.error.call_count == 2 # Expect error and response body
    log_call_args_1 = mock_logger.error.call_args_list[0][0]
    log_call_args_2 = mock_logger.error.call_args_list[1][0]
    assert f"HTTP error occurred while getting info for '{collection}': 500 Server Error" in log_call_args_1[0]
    assert "Internal Server Error Detail" in log_call_args_2[0]

@patch('solr_manager.commands.info.get_admin_base_url')
@patch('solr_manager.commands.info.logger')
def test_collection_info_no_admin_url(mock_logger, mock_get_admin_url):
    """Test info command when admin URL cannot be determined."""
    collection = "test_col"
    mock_config = {'zk_hosts': 'invalid'}
    mock_get_admin_url.return_value = None
    
    collection_info(collection_name=collection, config=mock_config)
    
    mock_get_admin_url.assert_called_once_with(mock_config)
    mock_logger.error.assert_called_once_with(
        "Could not determine Solr base URL from configuration."
    ) 