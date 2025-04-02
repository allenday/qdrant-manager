import os
import sys
import logging
from typing import Optional, Dict, Any
import random # Needed for selecting a random live node

try:
    import pysolr
except ImportError:
    print("Error: pysolr is not installed. Please run: pip install pysolr")
    sys.exit(1)

try:
    # Optional dependency
    from kazoo.client import KazooClient
    kazoo_imported = True
except ImportError:
    kazoo_imported = False

# Note: Using relative import assuming utils.py is in the same package as config.py
from .config import load_config

logger = logging.getLogger(__name__)

def load_and_override_config(args) -> Optional[Dict[str, Any]]:
    """
    Load connection configuration from the config file and override
    with any relevant command-line arguments.

    Returns:
        The connection configuration dictionary, or None if loading fails.
    """
    # Load base config from profile or default
    profile = args.profile if hasattr(args, 'profile') else None
    config = load_config(profile)
    
    if config is None:
        # Error message already printed by load_config
        return None

    # Override with command-line arguments if provided
    # Connection details
    if hasattr(args, 'solr_url') and args.solr_url:
        config['solr_url'] = args.solr_url
    if hasattr(args, 'collection') and args.collection:
        config['collection'] = args.collection
    if hasattr(args, 'zk_hosts') and args.zk_hosts:
        config['zk_hosts'] = args.zk_hosts
    if hasattr(args, 'username') and args.username:
        config['username'] = args.username
    # Handle password separately for potential security implications (e.g., prompt if not provided)
    # For now, just override if given.
    if hasattr(args, 'password') and args.password:
        config['password'] = args.password
        
    # Validate required configuration based on connection method
    if config.get('zk_hosts'):
        if not kazoo_imported:
            logger.error("ZooKeeper connection specified (zk_hosts) but 'kazoo' library is not installed.")
            logger.error("Please install it: pip install solr-manager[zookeeper]")
            return None
        # zk_hosts and collection are primary for SolrCloud via ZK
        required_keys = ["zk_hosts", "collection"]
    elif config.get('solr_url'):
        # solr_url and collection are primary for direct connection
        required_keys = ["solr_url", "collection"]
    else:
        logger.error("Missing required configuration: Please provide either 'solr_url' or 'zk_hosts' in your config/arguments.")
        return None

    missing = [key for key in required_keys if not config.get(key)]
    if missing:
        logger.error(f"Missing required configuration keys for the chosen connection method: {', '.join(missing)}")
        logger.error("Please update your configuration or provide command-line arguments.")
        return None
        
    return config

def initialize_solr_client(config: Dict[str, Any], collection_name: str) -> Optional[pysolr.SolrCoreAdmin]:
    """
    Initialize and return a Solr client (either Solr or SolrCloud) based on the configuration.

    Args:
        config: The connection configuration dictionary (excluding collection name).
        collection_name: The resolved collection name to connect to.

    Returns:
        A pysolr.Solr or pysolr.SolrCloud instance, or None if connection fails.
    """
    solr_client = None
    auth = None
    if config.get('username') and config.get('password'):
        auth = (config['username'], config['password'])
        logger.info("Using authentication.")
        
    # Determine connection timeout (use a default if not specified)
    timeout = config.get('timeout', 30) # Default to 30 seconds

    try:
        if config.get('zk_hosts'):
            # Connect using ZooKeeper (SolrCloud)
            zk_hosts = config['zk_hosts']
            logger.info(f"Initializing SolrCloud client via ZooKeeper: {zk_hosts}, collection: {collection_name}")
            if not kazoo_imported:
                 logger.error("Cannot initialize SolrCloud client: 'kazoo' is not installed.")
                 return None
            zk = KazooClient(hosts=zk_hosts, timeout=timeout)
            # Note: pysolr.SolrCloud takes KazooClient instance
            solr_client = pysolr.SolrCloud(zk, collection_name, auth=auth, timeout=timeout)
            # No connection test for now
            logger.info(f"SolrCloud client initialized for collection '{collection_name}'.")

        elif config.get('solr_url'):
            # Connect using direct URL
            solr_url_from_config = config['solr_url']
            # Initialize WITH collection for data operations
            full_solr_url = f"{solr_url_from_config.rstrip('/')}/{collection_name}"
            logger.info(f"Initializing Solr client for collection URL: {full_solr_url}")
            
            solr_client = pysolr.Solr(full_solr_url, auth=auth, timeout=timeout)
            # No connection test for now
            logger.info(f"Solr client initialized for collection '{collection_name}'.")
        else:
            # This case should ideally be caught by load_and_override_config validation
            logger.error("Invalid configuration: No 'solr_url' or 'zk_hosts' provided.")
            return None
            
        return solr_client
        
    except pysolr.SolrError as e:
        # Error during client instantiation itself
        logger.error(f"Failed to initialize Solr client object: {e}")
        return None
    except Exception as e:
        # Catch other potential errors (e.g., network issues, Kazoo errors)
        logger.error(f"An unexpected error occurred during Solr connection: {e}")
        import traceback
        traceback.print_exc()
        return None 

# --- Helper Functions for Admin API calls ---

def get_auth_tuple(config: Dict[str, Any]):
    """Helper to get auth tuple (username, password) from config."""
    if config.get('username') and config.get('password'):
        return (config['username'], config['password'])
    return None

def get_admin_base_url(config: Dict[str, Any]) -> str:
     """Helper to get the base Solr URL for Collections API calls (e.g., http://host:port/solr).
     
     Prioritizes 'solr_url'. If not found, tries to discover via 'zk_hosts'.
     """
     # 1. Prioritize direct URL
     if config.get('solr_url'):
         return config['solr_url'].rstrip('/')
         
     # 2. Try ZooKeeper discovery if zk_hosts is present
     elif config.get('zk_hosts'):
         if not kazoo_imported:
             logger.error("Admin command requires Solr base URL, but 'solr_url' is missing and 'kazoo' is not installed to discover via 'zk_hosts'.")
             logger.error("Please provide 'solr_url' in config/args or install 'kazoo' (pip install solr-manager[zookeeper]).")
             return None

         zk_hosts = config['zk_hosts']
         logger.info(f"'solr_url' not found, attempting node discovery via ZooKeeper: {zk_hosts}")
         zk = None
         try:
             # Connect to ZK
             # Note: KazooClient can be slow, consider reusing connection if performance is critical
             zk_timeout = config.get('timeout', 10) # Use shorter timeout for ZK connection itself
             zk = KazooClient(hosts=zk_hosts, timeout=zk_timeout, read_only=True)
             zk.start()
             
             # Get live nodes
             live_nodes = zk.get_children("/live_nodes")
             if not live_nodes:
                  logger.error(f"Could not find any live Solr nodes in ZooKeeper at path /live_nodes under hosts {zk_hosts}")
                  return None
                  
             # Select a random live node
             random_node = random.choice(live_nodes)
             logger.debug(f"Found live nodes: {live_nodes}. Selected: {random_node}")
             
             # Parse node name (format: host:port_context, e.g., 1.2.3.4:8983_solr)
             parts = random_node.split(':')
             if len(parts) != 2:
                  logger.error(f"Could not parse host:port from live node name: {random_node}")
                  return None
             host = parts[0]
             port_context = parts[1].split('_')
             if len(port_context) != 2:
                  logger.error(f"Could not parse port_context from live node name: {random_node}")
                  return None
             port = port_context[0]
             context = port_context[1] # e.g., 'solr'
             
             # Assume http scheme for now
             # TODO: Add https support detection?
             base_url = f"http://{host}:{port}/{context}"
             logger.info(f"Discovered base Solr URL via ZooKeeper: {base_url}")
             return base_url
             
         except Exception as e:
              logger.error(f"Error discovering Solr nodes via ZooKeeper ({zk_hosts}): {e}")
              return None
         finally:
             # Ensure ZK connection is closed
             if zk:
                 zk.stop()
                 zk.close()
                 
     # 3. If neither is found
     logger.error("Cannot determine base Solr URL for admin task (missing 'solr_url' or 'zk_hosts').")
     return None 