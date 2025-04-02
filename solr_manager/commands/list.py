import logging
import requests
from typing import Dict, Any

# Import helpers from utils
from ..utils import get_auth_tuple, get_admin_base_url

logger = logging.getLogger(__name__)

def list_collections(config: Dict[str, Any]):
    """Handles the logic for the 'list' command using requests and Solr Collections API."""
    logger.info("Listing all Solr collections via Collections API...")
    
    base_url = get_admin_base_url(config) # Use imported helper
    if not base_url:
        logger.error("Could not determine Solr base URL from configuration.")
        return
        
    list_url = f"{base_url}/admin/collections?action=LIST&wt=json"
    auth = get_auth_tuple(config) # Use imported helper
    timeout = config.get('timeout', 30)

    try:
        logger.debug(f"Requesting collection list from: {list_url}")
        response = requests.get(list_url, auth=auth, timeout=timeout)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        
        response_json = response.json()

        if 'collections' in response_json:
            collections = response_json['collections']
            if collections:
                print("Available collections:")
                for collection_name in sorted(collections):
                    print(f"  - {collection_name}")
            else:
                print("No collections found.")
        elif response_json.get('error'):
             error_msg = response_json['error'].get('msg', 'Unknown error')
             logger.error(f"Failed to list collections: {error_msg}")
        else:
            logger.error(f"Unexpected response format when listing collections. Response: {response.text}")
            
    except requests.exceptions.Timeout:
        logger.error(f"Timeout occurred while trying to connect to {list_url}")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error occurred while trying to connect to {list_url}: {e}")
    except requests.exceptions.HTTPError as e:
         logger.error(f"HTTP error occurred while listing collections: {e.response.status_code} {e.response.reason}")
         logger.error(f"Response body: {e.response.text}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while listing collections: {e}")
        import traceback
        traceback.print_exc() 