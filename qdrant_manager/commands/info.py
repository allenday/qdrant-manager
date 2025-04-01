import logging
from qdrant_client import QdrantClient
import json

logger = logging.getLogger(__name__)

def collection_info(client: QdrantClient, collection_name: str):
    """Handles the logic for the 'info' command."""
    if not collection_name:
        logger.error("Collection name is required for 'info' command.")
        return
        
    logger.info(f"Getting information for collection '{collection_name}'")
    try:
        info = client.get_collection(collection_name=collection_name)
        # Pretty print the dict representation of the model
        print(json.dumps(info.dict(), indent=2))
    except Exception as e:
        logger.error(f"Failed to get information for collection '{collection_name}': {e}") 