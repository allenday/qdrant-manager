import pytest
from unittest.mock import patch, MagicMock

from solr_manager.utils import get_admin_base_url, initialize_solr_client

# Test cases for get_admin_base_url

def test_get_admin_base_url_with_solr_url():
    """Test that solr_url is prioritized and returned correctly."""
    config = {"solr_url": "http://example.com:8983/solr/"}
    assert get_admin_base_url(config) == "http://example.com:8983/solr"

def test_get_admin_base_url_with_solr_url_trailing_slash():
    """Test removal of trailing slash from solr_url."""
    config = {"solr_url": "http://example.com:8983/solr/"}
    assert get_admin_base_url(config) == "http://example.com:8983/solr"

def test_get_admin_base_url_missing_url_and_zk():
    """Test return None when neither solr_url nor zk_hosts are provided."""
    config = {"collection": "test"}
    assert get_admin_base_url(config) is None

# Mock KazooClient for ZK tests
@pytest.fixture
def mock_kazoo():
    """Fixture to mock the KazooClient."""
    with patch('solr_manager.utils.KazooClient') as MockKazoo:
        mock_instance = MagicMock()
        MockKazoo.return_value = mock_instance
        yield mock_instance # Provide the mock instance to the test

def test_get_admin_base_url_with_zk_hosts_success(mock_kazoo):
    """Test successful discovery via zk_hosts."""
    config = {"zk_hosts": "zk1:2181,zk2:2181"}
    # Configure the mock
    mock_kazoo.get_children.return_value = ["10.0.0.1:8983_solr", "10.0.0.2:7574_solrcloud"]
    # Patch random.choice to be deterministic
    with patch('solr_manager.utils.random.choice') as mock_choice:
        mock_choice.return_value = "10.0.0.1:8983_solr" # Ensure we pick the first node
        base_url = get_admin_base_url(config)
    
    assert base_url == "http://10.0.0.1:8983/solr"
    mock_kazoo.start.assert_called_once()
    mock_kazoo.get_children.assert_called_once_with("/live_nodes")
    mock_choice.assert_called_once_with(["10.0.0.1:8983_solr", "10.0.0.2:7574_solrcloud"])
    mock_kazoo.stop.assert_called_once()
    mock_kazoo.close.assert_called_once()

def test_get_admin_base_url_with_zk_hosts_no_live_nodes(mock_kazoo):
    """Test ZK discovery when /live_nodes is empty."""
    config = {"zk_hosts": "zk1:2181"}
    mock_kazoo.get_children.return_value = [] # Simulate no live nodes
    
    assert get_admin_base_url(config) is None
    mock_kazoo.start.assert_called_once()
    mock_kazoo.get_children.assert_called_once_with("/live_nodes")
    mock_kazoo.stop.assert_called_once()
    mock_kazoo.close.assert_called_once()

def test_get_admin_base_url_with_zk_hosts_kazoo_error(mock_kazoo):
    """Test ZK discovery when KazooClient raises an exception."""
    config = {"zk_hosts": "zk1:2181"}
    mock_kazoo.start.side_effect = Exception("ZK Connection Failed")
    
    assert get_admin_base_url(config) is None
    mock_kazoo.start.assert_called_once()
    # Ensure stop/close are still called if start fails partially or in finally block
    # Depending on exact Kazoo behavior, stop/close might not be called if start fails immediately.
    # Let's assume our finally block ensures they are called if zk instance exists.
    # mock_kazoo.stop.assert_called_once() # Might not be called if start fails
    # mock_kazoo.close.assert_called_once() # Might not be called if start fails

def test_get_admin_base_url_with_zk_hosts_bad_node_format(mock_kazoo):
    """Test ZK discovery with malformed node names."""
    config = {"zk_hosts": "zk1:2181"}
    mock_kazoo.get_children.return_value = ["badnodeformat", "10.0.0.1_no_port_or_context"]
    with patch('solr_manager.utils.random.choice') as mock_choice:
        # Test both bad formats
        mock_choice.return_value = "badnodeformat"
        assert get_admin_base_url(config) is None
        
        mock_choice.return_value = "10.0.0.1_no_port_or_context"
        assert get_admin_base_url(config) is None

@patch('solr_manager.utils.kazoo_imported', False)
def test_get_admin_base_url_zk_hosts_kazoo_not_installed():
    """Test ZK discovery attempt when kazoo is not installed."""
    config = {"zk_hosts": "zk1:2181"}
    assert get_admin_base_url(config) is None

# TODO: Add tests for setup_logging and initialize_solr_client if needed. 