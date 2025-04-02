import logging
import requests
from urllib.parse import urlencode
from typing import Dict, Any
import time

# Assuming delete_collection is refactored in delete.py
from .delete import delete_collection 

logger = logging.getLogger(__name__)

# --- Helper Functions (Copied from list/delete, consider moving to shared utils) ---
def _get_auth(config: Dict[str, Any]):
    if config.get('username') and config.get('password'):
        return (config['username'], config['password'])
    return None

def _get_base_solr_url(config: Dict[str, Any]) -> str:
     if config.get('solr_url'):
         return config['solr_url'].rstrip('/')
     logger.warning("Cannot determine base Solr URL (missing 'solr_url').")
     return None 
# --- End Helper Functions ---

def _check_collection_exists(collection_name: str, config: Dict[str, Any]) -> bool:
    """Check if a Solr collection exists using the LIST action via requests."""
    base_url = _get_base_solr_url(config)
    if not base_url:
        return False # Cannot check if base URL is missing
        
    list_url = f"{base_url}/admin/collections?action=LIST&wt=json"
    auth = _get_auth(config)
    timeout = config.get('timeout', 30)
    
    try:
        response = requests.get(list_url, auth=auth, timeout=timeout)
        response.raise_for_status()
        response_json = response.json()
        return collection_name in response_json.get('collections', [])
    except Exception as e:
        logger.warning(f"Could not check if collection '{collection_name}' exists via API: {e}")
        return False # Be conservative, assume it might exist if check fails

def create_collection(
    collection_name: str, 
    num_shards: int, 
    replication_factor: int, 
    config_set_name: str,
    overwrite: bool,
    config: Dict[str, Any]
):
    """Handles the logic for the 'create' command using requests and Solr Collections API."""
    if not collection_name:
        logger.error("Collection name is required for 'create' command.")
        return
    if not config_set_name:
        logger.error("ConfigSet name (--configset) is required for 'create' command.")
        return

    base_url = _get_base_solr_url(config)
    if not base_url:
        logger.error("Could not determine Solr base URL from configuration.")
        return
        
    auth = _get_auth(config)
    timeout = config.get('timeout', 30)

    collection_exists = _check_collection_exists(collection_name, config)

    if collection_exists:
        if overwrite:
            logger.warning(f"Collection '{collection_name}' already exists. Overwriting...")
            # Call the refactored delete_collection function (which now uses requests)
            delete_collection(collection_name, config)
            # Add a small delay maybe? Solr might need a moment.
            logger.info("Waiting a few seconds after delete before creating...")
            time.sleep(3) 
        else:
            logger.warning(f"Collection '{collection_name}' already exists. Use --overwrite to replace it.")
            return # Exit without creating
            
    logger.info(f"Creating Solr collection '{collection_name}' using configSet '{config_set_name}'")
    logger.info(f"Shards: {num_shards}, Replication Factor: {replication_factor}")

    # Construct the Collections API URL for CREATE action
    create_params = {
        'action': 'CREATE',
        'name': collection_name,
        'numShards': num_shards,
        'replicationFactor': replication_factor,
        'collection.configName': config_set_name,
        'wt': 'json' # Explicitly request JSON response
        # Add other potential params like async, properties, etc. if needed later
    }
    query_string = urlencode(create_params)
    create_url = f"{base_url}/admin/collections?{query_string}"

    try:
        # Send the request
        logger.debug(f"Sending CREATE request for '{collection_name}' to {create_url}")
        response = requests.get(create_url, auth=auth, timeout=timeout*2) # Use longer timeout for create?
        response.raise_for_status()
        
        response_json = response.json()
        
        # Check response status
        if response_json.get('responseHeader', {}).get('status') == 0:
            logger.info(f"Collection '{collection_name}' created successfully.")
        elif response_json.get('error'):
             error_msg = response_json['error'].get('msg', 'Unknown error')
             # Check for specific error about configset
             if config_set_name in error_msg and ('Can not find the specified config set' in error_msg or 'Could not find config set' in error_msg):
                 logger.error(f"Failed to create collection '{collection_name}': ConfigSet '{config_set_name}' not found on the Solr server.")
                 logger.error("Please ensure the configSet exists or use a different name.")
             else:
                 logger.error(f"Failed to create collection '{collection_name}': {error_msg}")
        else:
             logger.error(f"Unexpected response format during create. Status: {response.status_code}, Response: {response.text}")

    except requests.exceptions.Timeout:
        logger.error(f"Timeout occurred while trying to create collection '{collection_name}' at {create_url}")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error occurred while trying to create collection '{collection_name}': {e}")
    except requests.exceptions.HTTPError as e:
         logger.error(f"HTTP error occurred while creating collection '{collection_name}': {e.response.status_code} {e.response.reason}")
         logger.error(f"Response body: {e.response.text}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while creating collection '{collection_name}': {e}")
        import traceback
        traceback.print_exc() 