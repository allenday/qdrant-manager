import logging
import requests
import json
from typing import Dict, Any

# Import helpers from utils
from ..utils import get_auth_tuple, get_admin_base_url

logger = logging.getLogger(__name__)

def collection_info(collection_name: str, config: Dict[str, Any]):
    """Handles the logic for the 'info' command using requests and Solr CLUSTERSTATUS."""
    if not collection_name:
        logger.error("Collection name is required for 'info' command.")
        return
        
    logger.info(f"Getting information for Solr collection '{collection_name}' via CLUSTERSTATUS...")

    base_url = get_admin_base_url(config) # Use imported helper
    if not base_url:
        logger.error("Could not determine Solr base URL from configuration.")
        return
        
    status_url = f"{base_url}/admin/collections?action=CLUSTERSTATUS&collection={collection_name}&wt=json"
    auth = get_auth_tuple(config) # Use imported helper
    timeout = config.get('timeout', 30)

    try:
        logger.debug(f"Sending CLUSTERSTATUS request for '{collection_name}' to {status_url}")
        response = requests.get(status_url, auth=auth, timeout=timeout)
        response.raise_for_status()
        
        response_json = response.json()

        if response_json.get('responseHeader', {}).get('status') == 0:
            cluster_status = response_json.get('cluster', {})
            collections = cluster_status.get('collections', {})
            
            if collection_name in collections:
                coll_info = collections[collection_name]
                # Add live node info for context
                coll_info['live_nodes'] = cluster_status.get('live_nodes', [])
                print(f"Information for collection '{collection_name}':")
                # Pretty print the extracted info
                print(json.dumps(coll_info, indent=2))
            else:
                 logger.error(f"Collection '{collection_name}' not found in cluster status.")
                 available = list(collections.keys())
                 if available:
                     logger.info(f"Available collections found in status: {available}")
                 else:
                     logger.info("No collections found in cluster status response.")
                 
        elif response_json.get('error'):
             error_msg = response_json['error'].get('msg', 'Unknown error')
             logger.error(f"Failed to get info for collection '{collection_name}': {error_msg}")
        else:
             logger.error(f"Unexpected response format during info request. Status: {response.status_code}, Response: {response.text}")

    except requests.exceptions.Timeout:
        logger.error(f"Timeout occurred while trying to get info for '{collection_name}' at {status_url}")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error occurred while trying to get info for '{collection_name}': {e}")
    except requests.exceptions.HTTPError as e:
         logger.error(f"HTTP error occurred while getting info for '{collection_name}': {e.response.status_code} {e.response.reason}")
         logger.error(f"Response body: {e.response.text}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while getting info for '{collection_name}': {e}")
        import traceback
        traceback.print_exc() 