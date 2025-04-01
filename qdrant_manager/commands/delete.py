import logging
from qdrant_client import QdrantClient

logger = logging.getLogger(__name__)

def delete_collection(client: QdrantClient, collection_name: str):
    """Handles the logic for the 'delete' command."""
    if not collection_name:
        logger.error("Collection name is required for 'delete' command.")
        return

    logger.info(f"Deleting collection '{collection_name}'")
    try:
        client.delete_collection(collection_name=collection_name)
        logger.info(f"Collection '{collection_name}' deleted successfully.")
    except Exception as e:
        logger.error(f"Failed to delete collection '{collection_name}': {e}") 