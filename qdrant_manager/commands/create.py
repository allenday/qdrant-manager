import logging
from qdrant_client import QdrantClient
from qdrant_client.http import models
from qdrant_client.http.exceptions import UnexpectedResponse

logger = logging.getLogger(__name__)

def create_collection(client: QdrantClient, collection_name: str, overwrite: bool, config: dict, args):
    """Handles the logic for the 'create' command."""
    if not collection_name:
        logger.error("Collection name is required for 'create' command.")
        return

    # Check if collection exists if overwrite is False
    if not overwrite:
        try:
            client.get_collection(collection_name=collection_name)
            # If the above line doesn't raise an exception, the collection exists
            logger.warning(f"Collection '{collection_name}' already exists. Use --overwrite to replace it.")
            return # Exit without creating
        except UnexpectedResponse as e:
            # Qdrant client raises UnexpectedResponse with status code 404 if not found
            if e.status_code == 404:
                logger.info(f"Collection '{collection_name}' does not exist, proceeding with creation.")
            else:
                # Re-raise other unexpected errors
                logger.error(f"Error checking collection '{collection_name}': {e}")
                return
        except Exception as e:
             logger.error(f"Unexpected error checking collection '{collection_name}': {e}")
             return

    # Map distance string to enum value
    distance_map = {
        "cosine": models.Distance.COSINE,
        "euclid": models.Distance.EUCLID,
        "dot": models.Distance.DOT
    }
    
    # Use command line args if provided, otherwise use config defaults
    vector_size = args.size if args.size is not None else config.get("vector_size", 256)
    distance_str = args.distance if args.distance is not None else config.get("distance", "cosine")
    indexing_threshold = args.indexing_threshold if args.indexing_threshold is not None else config.get("indexing_threshold", 0)
    
    # Get payload indices from config (if any)
    payload_indices = config.get("payload_indices", [])
    
    distance = distance_map.get(distance_str, models.Distance.COSINE)

    logger.info(f"Creating collection '{collection_name}' with size={vector_size}, distance={distance_str}, indexing_threshold={indexing_threshold}")
    if payload_indices:
        logger.info(f"Applying payload indices: {payload_indices}")
    
    try:
        client.recreate_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(size=vector_size, distance=distance),
            hnsw_config=models.HnswConfigDiff(payload_m=16, m=0), # Recommended defaults
            optimizers_config=models.OptimizersConfigDiff(indexing_threshold=indexing_threshold),
            # TODO: Add other config options like quantization, sparse vectors etc. based on args/config
        )
        logger.info(f"Collection '{collection_name}' created successfully.")

        # Apply payload indexing if specified
        for field_path, field_schema in payload_indices:
             try:
                 client.create_payload_index(
                     collection_name=collection_name,
                     field_name=field_path,
                     field_schema=field_schema # Expects models.PayloadSchemaType or string like 'keyword', 'integer', etc.
                 )
                 logger.info(f"Created payload index for field '{field_path}' in collection '{collection_name}'.")
             except Exception as e:
                 logger.error(f"Failed to create payload index for field '{field_path}': {e}")

    except Exception as e:
        logger.error(f"Failed to create collection '{collection_name}': {e}") 