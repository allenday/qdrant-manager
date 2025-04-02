import logging
import requests
from typing import Dict, Any

# Import helpers from utils
from ..utils import get_auth_tuple, get_admin_base_url

logger = logging.getLogger(__name__)

def delete_collection(collection_name: str, config: Dict[str, Any]):
    """Handles the logic for the 'delete' command using requests and Solr Collections API."""
    if not collection_name:
        logger.error("Collection name is required for 'delete' command.")
        return

    logger.info(f"Attempting to delete Solr collection '{collection_name}' via Collections API...")
    
    base_url = get_admin_base_url(config) # Use imported helper
    if not base_url:
        logger.error("Could not determine Solr base URL from configuration.")
        return

    delete_url = f"{base_url}/admin/collections?action=DELETE&name={collection_name}&wt=json"
    auth = get_auth_tuple(config) # Use imported helper
    timeout = config.get('timeout', 30)

    try:
        logger.debug(f"Sending DELETE request for '{collection_name}' to {delete_url}")
        response = requests.get(delete_url, auth=auth, timeout=timeout) # DELETE action uses GET
        response.raise_for_status() 

        response_json = response.json()
        
        if response_json.get('responseHeader', {}).get('status') == 0:
            logger.info(f"Collection '{collection_name}' deleted successfully (or did not exist).")
        elif response_json.get('error'):
             error_msg = response_json['error'].get('msg', 'Unknown error')
             logger.error(f"Failed to delete collection '{collection_name}': {error_msg}")
        else:
             logger.error(f"Unexpected response format during delete. Status: {response.status_code}, Response: {response.text}")

    except requests.exceptions.Timeout:
        logger.error(f"Timeout occurred while trying to delete collection '{collection_name}' at {delete_url}")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error occurred while trying to delete collection '{collection_name}': {e}")
    except requests.exceptions.HTTPError as e:
         logger.error(f"HTTP error occurred while deleting collection '{collection_name}': {e.response.status_code} {e.response.reason}")
         logger.error(f"Response body: {e.response.text}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while deleting collection '{collection_name}': {e}")
        import traceback
        traceback.print_exc() 