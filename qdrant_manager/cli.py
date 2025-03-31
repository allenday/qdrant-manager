#!/usr/bin/env python3
"""
Qdrant Manager - CLI tool for managing Qdrant vector database collections.

Provides commands to create, delete, list and modify collections, as well as perform
batch operations on documents within collections.
"""
import os
import sys
import json
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from tqdm import tqdm

try:
    from qdrant_client import QdrantClient
    from qdrant_client.http import models
except ImportError:
    print("Error: qdrant-client is not installed. Please run: pip install qdrant-client")
    sys.exit(1)

from qdrant_manager.config import load_config, get_profiles, update_config, create_default_config, get_config_dir

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_configuration(args):
    """Load configuration from config file or command line arguments."""
    # First try to load from config file
    if hasattr(args, 'profile') and args.profile:
        config = load_config(args.profile)
    else:
        config = load_config()
    
    # Override with command-line arguments if provided
    if hasattr(args, 'url') and args.url:
        config['url'] = args.url
    if hasattr(args, 'port') and args.port:
        config['port'] = args.port
    if hasattr(args, 'api_key') and args.api_key:
        config['api_key'] = args.api_key
    if hasattr(args, 'collection') and args.collection:
        config['collection'] = args.collection
        
    # Validate configuration
    required_keys = ["url", "port"]
    missing = [key for key in required_keys if not config.get(key)]
    
    if missing:
        logger.error(f"Missing required configuration: {', '.join(missing)}")
        logger.error("Please update your configuration or provide command-line arguments.")
        sys.exit(1)
    
    return config

def initialize_qdrant_client(env_vars):
    """Initialize Qdrant client."""
    logger.info(f"Connecting to Qdrant at {env_vars['url']}:{env_vars['port']}")
    
    try:
        client = QdrantClient(
            url=env_vars["url"],
            port=env_vars["port"],
            api_key=env_vars["api_key"],
        )
        # Test connection
        client.get_collections()
        logger.info("Successfully connected to Qdrant")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to Qdrant: {e}")
        sys.exit(1)

def create_collection(client, collection_name, overwrite=False, vector_size=256, distance=models.Distance.COSINE, indexing_threshold=0):
    """Create a new Qdrant collection.
    
    Args:
        client: Qdrant client
        collection_name: Name of the collection to create
        overwrite: Whether to overwrite existing collection
        vector_size: Size of the vector dimension for the collection
        distance: Distance function for vector similarity (from models.Distance enum)
        indexing_threshold: Number of vectors to collect before indexing (0 for immediate)
    """
    try:
        # Check if collection exists
        collections = client.get_collections().collections
        collection_names = [c.name for c in collections]
        
        if collection_name in collection_names:
            if overwrite:
                logger.info(f"Collection '{collection_name}' already exists, deleting...")
                client.delete_collection(collection_name=collection_name)
                logger.info(f"Collection '{collection_name}' deleted")
            else:
                logger.info(f"Collection '{collection_name}' already exists, no changes made")
                logger.info("Use --overwrite to recreate the collection")
                return False
        
        logger.info(f"Creating collection '{collection_name}'...")
        
        # Create collection with the specified parameters
        logger.info(f"Creating collection with vector size {vector_size}, distance function: {distance.name}, indexing threshold: {indexing_threshold}")
        client.create_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(
                size=vector_size,
                distance=distance
            ),
            optimizers_config=models.OptimizersConfigDiff(
                indexing_threshold=indexing_threshold
            )
        )
        
        logger.info(f"Collection '{collection_name}' created successfully")
        
        # Create payload indices if specified in the configuration
        if hasattr(args, 'payload_indices') and args.payload_indices:
            logger.info("Creating payload indices from configuration...")
            
            # Map string type names to Qdrant schema types
            schema_type_map = {
                "keyword": models.PayloadSchemaType.KEYWORD,
                "integer": models.PayloadSchemaType.INTEGER,
                "float": models.PayloadSchemaType.FLOAT,
                "geo": models.PayloadSchemaType.GEO,
                "text": models.PayloadSchemaType.TEXT,
                "datetime": models.PayloadSchemaType.DATETIME
            }
            
            for index in args.payload_indices:
                field = index.get("field")
                index_type = index.get("type", "keyword").lower()
                
                if not field:
                    logger.warning(f"Skipping index with missing field name: {index}")
                    continue
                
                # Get the schema type from the map or default to keyword
                schema_type = schema_type_map.get(index_type, models.PayloadSchemaType.KEYWORD)
                
                logger.info(f"Creating {index_type} index on field '{field}'")
                try:
                    client.create_payload_index(
                        collection_name=collection_name,
                        field_name=field,
                        field_schema=schema_type
                    )
                except Exception as e:
                    logger.error(f"Failed to create index for field '{field}': {e}")
        
        logger.info("Collection ready for use")
        
        return True
    except Exception as e:
        logger.error(f"Error creating collection: {e}")
        return False

def delete_collection(client, collection_name):
    """Delete an existing Qdrant collection."""
    try:
        # Check if collection exists
        collections = client.get_collections().collections
        collection_names = [c.name for c in collections]
        
        if collection_name not in collection_names:
            logger.info(f"Collection '{collection_name}' does not exist, nothing to delete")
            return False
        
        logger.info(f"Deleting collection '{collection_name}'...")
        client.delete_collection(collection_name=collection_name)
        logger.info(f"Collection '{collection_name}' deleted successfully")
        return True
    except Exception as e:
        logger.error(f"Error deleting collection: {e}")
        return False

def list_collections(client):
    """List all available Qdrant collections."""
    try:
        collections = client.get_collections().collections
        
        if not collections:
            logger.info("No collections found in Qdrant")
            return []
        
        logger.info(f"Found {len(collections)} collections:")
        for i, collection in enumerate(collections, 1):
            logger.info(f"{i}. {collection.name}")
            
            # Get collection info
            try:
                info = client.get_collection(collection_name=collection.name)
                point_count = info.vectors_count
                logger.info(f"   - Points: {point_count}")
                logger.info(f"   - Created: {info.creation_time}")
            except Exception as e:
                logger.error(f"   - Error getting collection info: {e}")
        
        return [c.name for c in collections]
    except Exception as e:
        logger.error(f"Error listing collections: {e}")
        return []



def resolve_json_path(obj: Dict[str, Any], path: str, create_missing: bool = False) -> Tuple[Optional[Dict[str, Any]], Optional[str], bool]:
    """
    Resolve a JSON path in an object.
    
    Args:
        obj: The object to navigate
        path: Path string (e.g., 'metadata.author' or '/metadata/author')
        create_missing: Whether to create missing intermediate objects
        
    Returns:
        (parent_obj, last_key, success): Tuple with the parent object, the last key, and a success flag
    """
    if not path or path in [".", "/"]:
        return obj, None, True
    
    # Normalize path
    if path.startswith("/"):
        path = path[1:]
    
    parts = path.replace(".", "/").split("/")
    current = obj
    
    # Navigate to the parent object
    for i, part in enumerate(parts[:-1]):
        if part not in current:
            if create_missing:
                current[part] = {}
            else:
                return None, None, False
        current = current[part]
        
        # Make sure we can navigate further
        if not isinstance(current, dict):
            return None, None, False
    
    return current, parts[-1], True

def load_document_ids(id_file: str) -> List[str]:
    """Load document IDs from a file."""
    try:
        with open(id_file, 'r') as file:
            # Strip whitespace from each line
            document_ids = [line.strip() for line in file if line.strip()]
        
        logger.info(f"Loaded {len(document_ids)} document IDs from {id_file}")
        return document_ids
    except Exception as e:
        logger.error(f"Error loading document IDs from {id_file}: {e}")
        sys.exit(1)

def parse_filter(filter_str: str) -> dict:
    """Parse a JSON filter string into a dict."""
    try:
        filter_dict = json.loads(filter_str)
        return filter_dict
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in filter: {e}")
        sys.exit(1)

def get_points_by_ids(client: QdrantClient, collection_name: str, document_ids: List[str]) -> List[Any]:
    """Retrieve points by IDs."""
    logger.info(f"Retrieving {len(document_ids)} points by ID...")
    
    # Process documents in batches
    batch_size = 100
    all_points = []
    
    for i in tqdm(range(0, len(document_ids), batch_size)):
        batch_ids = document_ids[i:i+batch_size]
        
        # Retrieve current payloads
        points = client.retrieve(
            collection_name=collection_name,
            ids=batch_ids,
            with_payload=True,
            with_vectors=False
        )
        
        # Filter out IDs that don't exist
        existing_points = [p for p in points if p is not None]
        if len(existing_points) < len(batch_ids):
            missing = set(batch_ids) - set(p.id for p in existing_points)
            logger.warning(f"Could not find {len(missing)} document(s): {', '.join(list(missing)[:5])}" + 
                        (f"... (and {len(missing) - 5} more)" if len(missing) > 5 else ""))
        
        all_points.extend(existing_points)
    
    logger.info(f"Found {len(all_points)} existing points")
    return all_points

def get_points_by_filter(client: QdrantClient, collection_name: str, filter_dict: dict, limit: int = 10000) -> List[Any]:
    """Retrieve points by a filter."""
    logger.info(f"Searching for points using filter...")
    
    try:
        # Create filter
        scroll_filter = models.Filter(**filter_dict)
        
        # Use scroll with this filter
        points = []
        offset = None
        batch_size = 100  # Fetch in smaller batches to show progress
        
        with tqdm() as pbar:
            while True:
                batch_points, offset = client.scroll(
                    collection_name=collection_name,
                    limit=batch_size,
                    with_payload=True,
                    with_vectors=False,
                    scroll_filter=scroll_filter,
                    offset=offset
                )
                
                if not batch_points:
                    break
                    
                points.extend(batch_points)
                pbar.update(len(batch_points))
                pbar.set_description(f"Retrieved {len(points)} points")
                
                # Stop if we've reached the limit or there are no more points
                if len(points) >= limit or offset is None:
                    break
        
        logger.info(f"Found {len(points)} points matching the filter")
        return points
        
    except Exception as e:
        logger.error(f"Error searching with filter: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

def batch_add_operation(client: QdrantClient, collection_name: str, points: List[Any], 
                     doc_data: Dict[str, Any], selector: Optional[str]) -> None:
    """Add fields to document payloads."""
    logger.info(f"Adding fields to {len(points)} documents...")
    
    # Track modified points to update
    modified_points = []
    
    # Process each point
    for point in tqdm(points):
        if not hasattr(point, 'payload') or point.payload is None:
            point.payload = {}
        
        if selector:
            # Navigate to the specified path
            parent, key, success = resolve_json_path(point.payload, selector, create_missing=True)
            if success and key:
                # Add at a specific path
                parent[key] = doc_data
            elif success:
                # Add at root level (no key needed)
                for k, v in doc_data.items():
                    parent[k] = v
        else:
            # Add at root level
            for key, value in doc_data.items():
                point.payload[key] = value
        
        # Add to the modified list
        modified_points.append({
            "id": point.id,
            "payload": point.payload
        })
    
    # Update points in batches
    if modified_points:
        batch_size = 100
        for i in range(0, len(modified_points), batch_size):
            batch = modified_points[i:i+batch_size]
            client.upsert(
                collection_name=collection_name,
                points=batch
            )
    
    logger.info(f"Successfully updated {len(modified_points)} documents")

def batch_delete_operation(client: QdrantClient, collection_name: str, points: List[Any], 
                        selector: str) -> None:
    """Delete fields from document payloads."""
    if not selector:
        logger.error("Selector is required for delete operations")
        sys.exit(1)
    
    logger.info(f"Deleting fields at '{selector}' from {len(points)} documents...")
    
    # Track modified points to update
    modified_points = []
    
    # Process each point
    for point in tqdm(points):
        if not hasattr(point, 'payload') or point.payload is None:
            continue
        
        # Normalize the selector
        normalized_selector = selector.lstrip('/')
        fields_to_delete = normalized_selector.replace('/', '.').split('.')
        
        # Handle root-level properties
        if len(fields_to_delete) == 1:
            if fields_to_delete[0] in point.payload:
                del point.payload[fields_to_delete[0]]
                modified_points.append({
                    "id": point.id,
                    "payload": point.payload
                })
        else:
            # Handle nested properties
            current = point.payload
            path_valid = True
            
            # Navigate to parent object
            for field in fields_to_delete[:-1]:
                if field in current and isinstance(current[field], dict):
                    current = current[field]
                else:
                    path_valid = False
                    break
            
            # Delete the field if path is valid
            if path_valid and fields_to_delete[-1] in current:
                del current[fields_to_delete[-1]]
                modified_points.append({
                    "id": point.id,
                    "payload": point.payload
                })
    
    # Update points in batches
    if modified_points:
        batch_size = 100
        for i in range(0, len(modified_points), batch_size):
            batch = modified_points[i:i+batch_size]
            client.upsert(
                collection_name=collection_name,
                points=batch
            )
    
    logger.info(f"Successfully updated {len(modified_points)} documents")

def batch_replace_operation(client: QdrantClient, collection_name: str, points: List[Any], 
                         doc_data: Dict[str, Any], selector: str) -> None:
    """Replace fields in document payloads."""
    if not selector:
        logger.error("Selector is required for replace operations")
        sys.exit(1)
    
    logger.info(f"Replacing fields at '{selector}' in {len(points)} documents...")
    
    # Track modified points to update
    modified_points = []
    
    # Process each point
    for point in tqdm(points):
        if not hasattr(point, 'payload') or point.payload is None:
            point.payload = {}
        
        # Handle root replacement
        if selector in [".", "/"]:
            # Replace the entire payload with the doc_data
            point.payload = doc_data.copy()
            modified_points.append({
                "id": point.id,
                "payload": point.payload
            })
            continue
            
        # Normalize the selector
        normalized_selector = selector.lstrip('/')
        
        # Split into path components
        path_parts = normalized_selector.replace('/', '.').split('.')
        
        # Handle nested paths
        if len(path_parts) == 1:
            # Replace a root-level field
            point.payload[path_parts[0]] = doc_data
            modified_points.append({
                "id": point.id,
                "payload": point.payload
            })
        else:
            # Handle nested replacement
            current = point.payload
            path_valid = True
            
            # Create parent objects if needed
            for i, part in enumerate(path_parts[:-1]):
                if part not in current:
                    current[part] = {}
                elif not isinstance(current[part], dict):
                    # If an intermediate node is not a dict, replace it with one
                    current[part] = {}
                
                current = current[part]
            
            # Set the final field
            if path_valid:
                current[path_parts[-1]] = doc_data
                modified_points.append({
                    "id": point.id,
                    "payload": point.payload
                })
    
    # Update points in batches
    if modified_points:
        batch_size = 100
        for i in range(0, len(modified_points), batch_size):
            batch = modified_points[i:i+batch_size]
            client.upsert(
                collection_name=collection_name,
                points=batch
            )
    
    logger.info(f"Successfully updated {len(modified_points)} documents")

def batch_operations(client: QdrantClient, collection_name: str, args: argparse.Namespace) -> None:
    """Perform batch operations on documents."""
    # Get points based on ID file or filter
    points = []
    
    if args.id_file:
        document_ids = load_document_ids(args.id_file)
        points = get_points_by_ids(client, collection_name, document_ids)
    elif args.ids:
        document_ids = [id.strip() for id in args.ids.split(',')]
        points = get_points_by_ids(client, collection_name, document_ids)
    elif args.filter:
        filter_dict = parse_filter(args.filter)
        points = get_points_by_filter(client, collection_name, filter_dict, args.limit)
    else:
        logger.error("Either --id-file, --ids, or --filter must be provided")
        sys.exit(1)
    
    if not points:
        logger.warning("No points found to update")
        return
    
    # Parse the document if provided
    doc_data = None
    if args.doc:
        try:
            doc_data = json.loads(args.doc)
            if not isinstance(doc_data, dict):
                logger.error("The --doc value must be a valid JSON object")
                sys.exit(1)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in --doc: {e}")
            sys.exit(1)
    
    # Perform the operation
    if args.add:
        if not doc_data:
            logger.error("The --doc parameter is required for --add operations")
            sys.exit(1)
        batch_add_operation(client, collection_name, points, doc_data, args.selector)
    elif args.delete:
        if not args.selector:
            logger.error("The --selector parameter is required for --delete operations")
            sys.exit(1)
        batch_delete_operation(client, collection_name, points, args.selector)
    elif args.replace:
        if not doc_data:
            logger.error("The --doc parameter is required for --replace operations")
            sys.exit(1)
        if not args.selector:
            logger.error("The --selector parameter is required for --replace operations")
            sys.exit(1)
        batch_replace_operation(client, collection_name, points, doc_data, args.selector)
    else:
        logger.error("One of --add, --delete, or --replace must be specified")
        sys.exit(1)

def collection_info(client, collection_name):
    """Get detailed information about a specific collection."""
    try:
        # Check if collection exists
        collections = client.get_collections().collections
        collection_names = [c.name for c in collections]
        
        if collection_name not in collection_names:
            logger.info(f"Collection '{collection_name}' does not exist")
            return None
        
        # Get collection info
        info = client.get_collection(collection_name=collection_name)
        
        # Get collection statistics
        # We need to use count explicitly as vectors_count may not be reported correctly
        count_result = client.count(collection_name=collection_name, exact=True)
        points_count = count_result.count
        
        # Get a sample point if available
        points = client.scroll(
            collection_name=collection_name,
            limit=1
        )[0]
        
        logger.info(f"Collection '{collection_name}' information:")
        logger.info(f"Points count: {points_count}")
        
        # Display vector configuration
        vector_config = info.config.params
        
        # Check if collection has named vectors (newer format)
        if hasattr(vector_config, 'vectors') and vector_config.vectors:
            logger.info("Named vector configurations:")
            # Handle different vector config formats
            if isinstance(vector_config.vectors, dict):
                # Dict style format
                for name, params in vector_config.vectors.items():
                    logger.info(f"  {name}: size={params.size}, distance={params.distance}")
            else:
                # Object style format (model instance)
                try:
                    # Try to represent vector_config as a dictionary for display
                    vector_dict = vector_config.vectors.dict() if hasattr(vector_config.vectors, 'dict') else vars(vector_config.vectors)
                    for name, params in vector_dict.items():
                        if isinstance(params, dict) and 'size' in params:
                            logger.info(f"  {name}: size={params['size']}, distance={params.get('distance', 'unknown')}")
                        else:
                            logger.info(f"  {name}: {params}")
                except Exception as e:
                    # Fall back to simple representation
                    logger.info(f"  Vector config: {vector_config.vectors}")
        # Fall back to legacy single vector format
        elif hasattr(vector_config, 'size'):
            logger.info(f"Vector configuration: size={vector_config.size}, distance={vector_config.distance}")
        else:
            logger.info("Vector configuration not available")
        
        # Check if collection has sparse vectors
        if hasattr(vector_config, 'sparse_vectors') and vector_config.sparse_vectors:
            logger.info("Sparse vector configurations:")
            try:
                # Try to get names directly if it's a dict-like object
                if hasattr(vector_config.sparse_vectors, 'keys'):
                    for name in vector_config.sparse_vectors.keys():
                        logger.info(f"  {name}")
                # Otherwise try to convert to a dict or print the object
                else:
                    sparse_dict = vector_config.sparse_vectors.dict() if hasattr(vector_config.sparse_vectors, 'dict') else vars(vector_config.sparse_vectors)
                    for name, config in sparse_dict.items():
                        logger.info(f"  {name}: {config}")
            except Exception as e:
                # Fall back to simple string representation
                logger.info(f"  Sparse vector config: {vector_config.sparse_vectors}")
        
        # Display collection metadata if available
        if hasattr(info, 'metadata') and info.metadata:
            logger.info("Collection metadata:")
            for k, v in info.metadata.items():
                logger.info(f"  {k}: {v}")
        
        # Display sample point if available
        if points:
            logger.info("Sample point payload:")
            payload = points[0].payload
            for k, v in payload.items():
                # Skip tags array to keep output clean
                if k == "tags" and isinstance(v, dict) and len(v) > 5:
                    logger.info(f"  {k}: {len(v)} tags")
                    # Show just a few tags
                    sample_tags = list(v.items())[:5]
                    for tag, score in sample_tags:
                        logger.info(f"    - {tag}: {score}")
                    logger.info(f"    - ... and {len(v) - 5} more tags")
                else:
                    logger.info(f"  {k}: {v}")
        else:
            logger.info("Collection is empty or no points returned")
        
        return info
    except Exception as e:
        logger.error(f"Error getting collection info: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Qdrant Manager - CLI tool for managing Qdrant vector database collections",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    # Command argument
    parser.add_argument(
        "command", 
        choices=["create", "delete", "list", "info", "batch", "config"],
        help="""Command to execute:
  create: Create a new collection
  delete: Delete an existing collection
  list: List all collections
  info: Get detailed information about a collection
  batch: Perform batch operations on documents
  config: View or modify configuration"""
    )
    
    # Connection arguments
    connection_args = parser.add_argument_group('Connection Options')
    connection_args.add_argument(
        "--profile", 
        help="Configuration profile to use (from ~/.config/qdrant-manager/config.yaml)"
    )
    connection_args.add_argument(
        "--url", 
        help="Qdrant server URL"
    )
    connection_args.add_argument(
        "--port", 
        type=int,
        help="Qdrant server port"
    )
    connection_args.add_argument(
        "--api-key", 
        help="Qdrant API key"
    )
    
    # Optional arguments for most commands
    parser.add_argument(
        "--collection", 
        help="Collection name (defaults to value from config)"
    )
    parser.add_argument(
        "--overwrite", 
        action="store_true",
        help="Overwrite collection if it already exists"
    )
    # Collection creation arguments
    collection_create = parser.add_argument_group('Collection Creation Options')
    collection_create.add_argument(
        "--size",
        type=int,
        help="Vector size for created collections (uses config default if not specified)"
    )
    collection_create.add_argument(
        "--distance",
        choices=["cosine", "euclid", "dot"],
        help="Distance function for vector similarity (uses config default if not specified)"
    )
    collection_create.add_argument(
        "--indexing-threshold",
        type=int,
        help="Indexing threshold (number of vectors to collect before indexing, 0 for immediate indexing)"
    )
    
    # Batch command arguments
    # Document selection
    document_selection = parser.add_argument_group('Document Selection (required for batch command)')
    doc_selector = document_selection.add_mutually_exclusive_group()
    doc_selector.add_argument(
        "--id-file", 
        help="Path to a file containing document IDs, one per line"
    )
    doc_selector.add_argument(
        "--ids", 
        help="Comma-separated list of document IDs"
    )
    doc_selector.add_argument(
        "--filter", 
        help="JSON string containing Qdrant filter (e.g., '{\"key\":\"category\",\"match\":{\"value\":\"product\"}}')"
    )
    
    # Operation type
    operation_type = parser.add_argument_group('Operation Type (required for batch command)')
    op_type = operation_type.add_mutually_exclusive_group()
    op_type.add_argument(
        "--add",
        action="store_true",
        help="Add fields to documents (creates paths if needed)"
    )
    op_type.add_argument(
        "--delete",
        action="store_true",
        help="Delete fields from documents"
    )
    op_type.add_argument(
        "--replace",
        action="store_true",
        help="Replace fields in documents"
    )
    
    # Document and selector
    batch_params = parser.add_argument_group('Batch Parameters')
    batch_params.add_argument(
        "--doc",
        help="JSON string containing document data for add/replace operations (e.g., '{\"field1\":\"value1\"}')"
    )
    batch_params.add_argument(
        "--selector",
        help="""JSON path selector for where to add/delete/replace fields (e.g., 'metadata.author')
Example selectors:
  '.' or '/' = root level (same as not providing a selector for --add)
  'metadata' = fields under a 'metadata' property
  'metadata.author' = fields under metadata.author"""
    )
    batch_params.add_argument(
        "--limit",
        type=int,
        default=10000,
        help="Maximum number of points to process when using --filter (default: 10000)"
    )
    
    args = parser.parse_args()
    
    # Handle config command separately (doesn't need client initialization)
    if args.command == "config":
        if len(sys.argv) == 2:
            # Just show available profiles
            profiles = get_profiles()
            print("Available configuration profiles:")
            for profile in profiles:
                print(f"  - {profile}")
            print(f"\nConfiguration file: {get_config_dir() / 'config.yaml'}")
            sys.exit(0)
        else:
            print("Config management will be implemented in a future version.")
            sys.exit(0)
    
    # Load configuration
    config = load_configuration(args)
    
    # Use specified collection or default from config
    collection_name = args.collection or config.get("collection", "")
    
    # Initialize Qdrant client
    client = initialize_qdrant_client(config)
    
    # Execute the requested command
    if args.command == "create":
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
        
        # Add payload indices from config
        args.payload_indices = config.get("payload_indices", [])
        
        distance = distance_map.get(distance_str, models.Distance.COSINE)
        
        create_collection(client, collection_name, args.overwrite, vector_size, distance, indexing_threshold)
    
    elif args.command == "delete":
        delete_collection(client, collection_name)
    
    elif args.command == "list":
        list_collections(client)
    
    elif args.command == "info":
        collection_info(client, collection_name)
        
    elif args.command == "batch":
        batch_operations(client, collection_name, args)

if __name__ == "__main__":
    main()