from unittest.mock import patch, MagicMock
import requests # For exceptions
from urllib.parse import urlencode

from solr_manager.commands.create import create_collection

# --- Test create_collection ---

@patch('solr_manager.commands.create._check_collection_exists') # Mock the check helper
@patch('solr_manager.commands.create.requests.get')
@patch('solr_manager.commands.create._get_base_solr_url') 
@patch('solr_manager.commands.create.logger')
def test_create_collection_success(mock_logger, mock_get_base_url, mock_requests_get, mock_check_exists):
    """Test successful creation of a new collection."""
    # Restore full setup
    collection = "new_collection"
    config_set = "_default"
    shards = 2
    replicas = 3
    mock_config = {'solr_url': 'http://fake-solr:8983/solr', 'timeout': 60}
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"responseHeader": {"status": 0, "QTime": 500}}
    # End restore
    
    mock_get_base_url.return_value = mock_admin_url 
    mock_requests_get.return_value = mock_response
    mock_check_exists.return_value = False # Simulate collection does not exist
    
    # Restore call
    create_collection(
        collection_name=collection, 
        config_set_name=config_set, 
        num_shards=shards,
        replication_factor=replicas,
        overwrite=False, 
        config=mock_config
    )
    # End restore
    
    mock_check_exists.assert_called_once_with(collection, mock_config)
    mock_get_base_url.assert_called_once_with(mock_config) # Assert call on correct mock
    # Restore assertions
    # Check the actual URL constructed with urlencode
    expected_query_string = urlencode({
        "action": "CREATE",
        "name": collection,
        "numShards": shards,
        "replicationFactor": replicas,
        "collection.configName": config_set,
        "wt": "json"
    })
    actual_call_url = mock_requests_get.call_args[0][0]
    assert f"{mock_admin_url}/admin/collections?{expected_query_string}" == actual_call_url
    
    # Check other args like auth and timeout from the actual call
    actual_call_kwargs = mock_requests_get.call_args[1]
    assert actual_call_kwargs['auth'] is None
    # Use the timeout value actually used in create.py (timeout*2)
    assert actual_call_kwargs['timeout'] == mock_config['timeout'] * 2 
    
    mock_requests_get.assert_called_once() # General check that it was called once
    mock_response.raise_for_status.assert_called_once()
    mock_logger.info.assert_any_call(f"Collection '{collection}' created successfully.")
    # End restore

@patch('solr_manager.commands.create._check_collection_exists') # Mock the check helper
@patch('solr_manager.commands.create.requests.get')
@patch('solr_manager.commands.create._get_base_solr_url') 
@patch('solr_manager.commands.create.logger')
def test_create_collection_http_error(mock_logger, mock_get_base_url, mock_requests_get, mock_check_exists):
    """Test handling HTTP errors during creation."""
    # Restore full setup
    collection = "fail_create"
    config_set = "bad_config"
    mock_config = {'solr_url': 'http://fake-solr:8983/solr'}
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_http_response = MagicMock()
    mock_http_response.status_code = 400
    mock_http_response.reason = "Bad Request"
    mock_http_response.text = "ConfigSet not found"
    http_error = requests.exceptions.HTTPError("400 Bad Request", response=mock_http_response)
    
    mock_response_trigger = MagicMock()
    mock_response_trigger.raise_for_status.side_effect = http_error
    # End restore
    
    mock_get_base_url.return_value = mock_admin_url 
    mock_requests_get.return_value = mock_response_trigger
    mock_check_exists.return_value = False # Simulate collection does not exist
    
    # Restore call
    create_collection(
        collection_name=collection, 
        config_set_name=config_set,
        num_shards=1, replication_factor=1, overwrite=False,
        config=mock_config
    )
    # End restore
    
    mock_check_exists.assert_called_once_with(collection, mock_config)
    mock_get_base_url.assert_called_once_with(mock_config) # Now called only once
    # Restore assertions
    mock_requests_get.assert_called_once()
    mock_response_trigger.raise_for_status.assert_called_once()
    assert mock_logger.error.call_count == 2
    log_call_args_1 = mock_logger.error.call_args_list[0][0]
    log_call_args_2 = mock_logger.error.call_args_list[1][0]
    assert f"HTTP error occurred while creating collection '{collection}': 400 Bad Request" in log_call_args_1[0]
    assert "ConfigSet not found" in log_call_args_2[0]
    # End restore

@patch('solr_manager.commands.create._get_base_solr_url') 
@patch('solr_manager.commands.create.logger')
def test_create_collection_no_admin_url(mock_logger, mock_get_base_url):
    """Test create when admin URL cannot be determined."""
    # Restore full setup
    collection = "test_col"
    config_set = "_default"
    mock_config = {}
    # End restore
    
    mock_get_base_url.return_value = None # Use the new mock name
    
    # Restore call
    create_collection(
        collection_name=collection, config_set_name=config_set,
        num_shards=1, replication_factor=1, overwrite=False,
        config=mock_config
    )
    # End restore
    
    mock_get_base_url.assert_called_once_with(mock_config) # Assert call on correct mock
    # Restore assertions
    mock_logger.error.assert_called_once_with(
        "Could not determine Solr base URL from configuration."
    )
    # End restore

# --- Test Helper Functions ---

@patch('solr_manager.commands.create.logger')
def test_get_auth_with_credentials(mock_logger):
    """Test _get_auth with valid credentials."""
    from solr_manager.commands.create import _get_auth
    config = {'username': 'user', 'password': 'pass'}
    auth = _get_auth(config)
    assert auth == ('user', 'pass')

def test_get_auth_without_credentials():
    """Test _get_auth without credentials."""
    from solr_manager.commands.create import _get_auth
    config = {'other': 'setting'}
    auth = _get_auth(config)
    assert auth is None

@patch('solr_manager.commands.create.logger')
def test_get_base_solr_url_valid(mock_logger):
    """Test _get_base_solr_url with valid URL."""
    from solr_manager.commands.create import _get_base_solr_url
    config = {'solr_url': 'http://solr:8983/solr/'}
    url = _get_base_solr_url(config)
    assert url == 'http://solr:8983/solr'

@patch('solr_manager.commands.create.logger')
def test_get_base_solr_url_missing(mock_logger):
    """Test _get_base_solr_url with missing URL."""
    from solr_manager.commands.create import _get_base_solr_url
    config = {}
    url = _get_base_solr_url(config)
    assert url is None
    mock_logger.warning.assert_called_once_with("Cannot determine base Solr URL (missing 'solr_url').")

@patch('solr_manager.commands.create.requests.get')
def test_check_collection_exists_success(mock_requests_get):
    """Test _check_collection_exists when collection exists."""
    from solr_manager.commands.create import _check_collection_exists
    mock_response = MagicMock()
    mock_response.json.return_value = {'collections': ['test_collection']}
    mock_requests_get.return_value = mock_response

    config = {'solr_url': 'http://solr:8983/solr'}
    exists = _check_collection_exists('test_collection', config)

    assert exists is True
    mock_requests_get.assert_called_once()
    assert 'action=LIST' in mock_requests_get.call_args[0][0]

@patch('solr_manager.commands.create.requests.get')
def test_check_collection_exists_not_found(mock_requests_get):
    """Test _check_collection_exists when collection doesn't exist."""
    from solr_manager.commands.create import _check_collection_exists
    mock_response = MagicMock()
    mock_response.json.return_value = {'collections': ['other_collection']}
    mock_requests_get.return_value = mock_response

    config = {'solr_url': 'http://solr:8983/solr'}
    exists = _check_collection_exists('test_collection', config)

    assert exists is False

@patch('solr_manager.commands.create.requests.get')
def test_check_collection_exists_error(mock_requests_get):
    """Test _check_collection_exists when request fails."""
    from solr_manager.commands.create import _check_collection_exists
    mock_requests_get.side_effect = requests.exceptions.RequestException("Connection failed")

    config = {'solr_url': 'http://solr:8983/solr'}
    exists = _check_collection_exists('test_collection', config)

    assert exists is False  # Conservative approach

# --- Additional create_collection Tests ---

@patch('solr_manager.commands.create.logger')
def test_create_collection_missing_name(mock_logger):
    """Test create_collection with missing collection name."""
    create_collection(
        collection_name='', 
        config_set_name='config',
        num_shards=1, 
        replication_factor=1,
        overwrite=False,
        config={}
    )
    mock_logger.error.assert_called_once_with("Collection name is required for 'create' command.")

@patch('solr_manager.commands.create.logger')
def test_create_collection_missing_configset(mock_logger):
    """Test create_collection with missing configset name."""
    create_collection(
        collection_name='test', 
        config_set_name='',
        num_shards=1, 
        replication_factor=1,
        overwrite=False,
        config={}
    )
    mock_logger.error.assert_called_once_with("ConfigSet name (--configset) is required for 'create' command.")

@patch('solr_manager.commands.create._check_collection_exists')
@patch('solr_manager.commands.create.delete_collection')
@patch('solr_manager.commands.create.requests.get')
@patch('solr_manager.commands.create._get_base_solr_url')
@patch('solr_manager.commands.create.logger')
@patch('solr_manager.commands.create.time.sleep')
def test_create_collection_overwrite(mock_sleep, mock_logger, mock_get_base_url, mock_requests_get, mock_delete, mock_check_exists):
    """Test create_collection with overwrite=True when collection exists."""
    collection = "existing_collection"
    config_set = "_default"
    mock_config = {'solr_url': 'http://fake-solr:8983/solr'}
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_get_base_url.return_value = mock_admin_url
    mock_check_exists.return_value = True  # Collection exists
    
    mock_response = MagicMock()
    mock_response.json.return_value = {"responseHeader": {"status": 0}}
    mock_requests_get.return_value = mock_response
    
    create_collection(
        collection_name=collection,
        config_set_name=config_set,
        num_shards=1,
        replication_factor=1,
        overwrite=True,
        config=mock_config
    )
    
    mock_delete.assert_called_once_with(collection, mock_config)
    mock_sleep.assert_called_once_with(3)  # Verify delay
    mock_requests_get.assert_called_once()  # Verify create request

@patch('solr_manager.commands.create._check_collection_exists')
@patch('solr_manager.commands.create.requests.get')
@patch('solr_manager.commands.create._get_base_solr_url')
@patch('solr_manager.commands.create.logger')
def test_create_collection_configset_not_found(mock_logger, mock_get_base_url, mock_requests_get, mock_check_exists):
    """Test create_collection when configset is not found."""
    collection = "test_collection"
    config_set = "missing_config"
    mock_config = {'solr_url': 'http://fake-solr:8983/solr'}
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_get_base_url.return_value = mock_admin_url
    mock_check_exists.return_value = False
    
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "responseHeader": {"status": 1},
        "error": {"msg": "Can not find the specified config set: missing_config"}
    }
    mock_requests_get.return_value = mock_response
    
    create_collection(
        collection_name=collection,
        config_set_name=config_set,
        num_shards=1,
        replication_factor=1,
        overwrite=False,
        config=mock_config
    )
    
    mock_logger.error.assert_any_call(
        f"Failed to create collection '{collection}': ConfigSet '{config_set}' not found on the Solr server."
    )

@patch('solr_manager.commands.create._check_collection_exists')
@patch('solr_manager.commands.create.requests.get')
@patch('solr_manager.commands.create._get_base_solr_url')
@patch('solr_manager.commands.create.logger')
def test_create_collection_timeout(mock_logger, mock_get_base_url, mock_requests_get, mock_check_exists):
    """Test create_collection handling timeout."""
    collection = "timeout_collection"
    config_set = "_default"
    mock_config = {'solr_url': 'http://fake-solr:8983/solr'}
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_get_base_url.return_value = mock_admin_url
    mock_check_exists.return_value = False
    mock_requests_get.side_effect = requests.exceptions.Timeout()
    
    create_collection(
        collection_name=collection,
        config_set_name=config_set,
        num_shards=1,
        replication_factor=1,
        overwrite=False,
        config=mock_config
    )
    
    mock_logger.error.assert_called_once_with(
        f"Timeout occurred while trying to create collection '{collection}' at {mock_admin_url}/admin/collections?action=CREATE&name={collection}&numShards=1&replicationFactor=1&collection.configName={config_set}&wt=json"
    )

@patch('solr_manager.commands.create._check_collection_exists')
@patch('solr_manager.commands.create.requests.get')
@patch('solr_manager.commands.create._get_base_solr_url')
@patch('solr_manager.commands.create.logger')
def test_create_collection_with_auth(mock_logger, mock_get_base_url, mock_requests_get, mock_check_exists):
    """Test create_collection with authentication."""
    collection = "auth_collection"
    config_set = "_default"
    mock_config = {
        'solr_url': 'http://fake-solr:8983/solr',
        'username': 'admin',
        'password': 'secret'
    }
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_get_base_url.return_value = mock_admin_url
    mock_check_exists.return_value = False
    
    mock_response = MagicMock()
    mock_response.json.return_value = {"responseHeader": {"status": 0}}
    mock_requests_get.return_value = mock_response
    
    create_collection(
        collection_name=collection,
        config_set_name=config_set,
        num_shards=1,
        replication_factor=1,
        overwrite=False,
        config=mock_config
    )
    
    # Verify auth tuple was passed
    call_kwargs = mock_requests_get.call_args[1]
    assert call_kwargs['auth'] == ('admin', 'secret')

@patch('solr_manager.commands.create._check_collection_exists')
@patch('solr_manager.commands.create.requests.get')
@patch('solr_manager.commands.create._get_base_solr_url')
@patch('solr_manager.commands.create.logger')
def test_create_collection_connection_error(mock_logger, mock_get_base_url, mock_requests_get, mock_check_exists):
    """Test create_collection handling connection error."""
    collection = "test_collection"
    config_set = "_default"
    mock_config = {'solr_url': 'http://fake-solr:8983/solr'}
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_get_base_url.return_value = mock_admin_url
    mock_check_exists.return_value = False
    mock_requests_get.side_effect = requests.exceptions.ConnectionError("Failed to connect")
    
    create_collection(
        collection_name=collection,
        config_set_name=config_set,
        num_shards=1,
        replication_factor=1,
        overwrite=False,
        config=mock_config
    )
    
    mock_logger.error.assert_called_once_with(
        f"Connection error occurred while trying to create collection '{collection}': Failed to connect"
    )

@patch('solr_manager.commands.create._check_collection_exists')
@patch('solr_manager.commands.create.requests.get')
@patch('solr_manager.commands.create._get_base_solr_url')
@patch('solr_manager.commands.create.logger')
def test_create_collection_unexpected_response(mock_logger, mock_get_base_url, mock_requests_get, mock_check_exists):
    """Test create_collection handling unexpected response format."""
    collection = "test_collection"
    config_set = "_default"
    mock_config = {'solr_url': 'http://fake-solr:8983/solr'}
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_get_base_url.return_value = mock_admin_url
    mock_check_exists.return_value = False
    
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "Not JSON"
    mock_response.json.return_value = {"unexpected": "format"}  # No responseHeader
    mock_requests_get.return_value = mock_response
    
    create_collection(
        collection_name=collection,
        config_set_name=config_set,
        num_shards=1,
        replication_factor=1,
        overwrite=False,
        config=mock_config
    )
    
    mock_logger.error.assert_called_once_with(
        f"Unexpected response format during create. Status: 200, Response: Not JSON"
    )

@patch('solr_manager.commands.create._check_collection_exists')
@patch('solr_manager.commands.create.requests.get')
@patch('solr_manager.commands.create._get_base_solr_url')
@patch('solr_manager.commands.create.logger')
@patch('traceback.print_exc')
def test_create_collection_unexpected_error(mock_print_exc, mock_logger, mock_get_base_url, mock_requests_get, mock_check_exists):
    """Test create_collection handling unexpected error."""
    collection = "test_collection"
    config_set = "_default"
    mock_config = {'solr_url': 'http://fake-solr:8983/solr'}
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_get_base_url.return_value = mock_admin_url
    mock_check_exists.return_value = False
    mock_requests_get.side_effect = Exception("Something unexpected happened")
    
    create_collection(
        collection_name=collection,
        config_set_name=config_set,
        num_shards=1,
        replication_factor=1,
        overwrite=False,
        config=mock_config
    )
    
    mock_logger.error.assert_called_once_with(
        f"An unexpected error occurred while creating collection '{collection}': Something unexpected happened"
    )
    mock_print_exc.assert_called_once()

@patch('solr_manager.commands.create._check_collection_exists')
@patch('solr_manager.commands.create.requests.get')
@patch('solr_manager.commands.create._get_base_solr_url')
@patch('solr_manager.commands.create.logger')
def test_create_collection_exists_no_overwrite(mock_logger, mock_get_base_url, mock_requests_get, mock_check_exists):
    """Test create_collection when collection exists and overwrite=False."""
    collection = "existing_collection"
    config_set = "_default"
    mock_config = {'solr_url': 'http://fake-solr:8983/solr'}
    mock_admin_url = "http://fake-solr:8983/solr"
    
    mock_get_base_url.return_value = mock_admin_url
    mock_check_exists.return_value = True  # Collection exists
    
    create_collection(
        collection_name=collection,
        config_set_name=config_set,
        num_shards=1,
        replication_factor=1,
        overwrite=False,
        config=mock_config
    )
    
    mock_logger.warning.assert_called_once_with(
        f"Collection '{collection}' already exists. Use --overwrite to replace it."
    )
    mock_requests_get.assert_not_called()  # Should not attempt to create
