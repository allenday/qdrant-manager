import logging
from qdrant_client import QdrantClient

logger = logging.getLogger(__name__)

def list_collections(client: QdrantClient):
    """Handles the logic for the 'list' command."""
    logger.info("Listing all collections")
    try:
        collections = client.get_collections().collections
        if collections:
            print("Available collections:")
            for collection in collections:
                print(f"  - {collection.name}")
        else:
            print("No collections found.")
    except Exception as e:
        logger.error(f"Failed to list collections: {e}") 