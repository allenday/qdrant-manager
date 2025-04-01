#!/usr/bin/env python3
"""
Qdrant Manager - CLI tool for managing Qdrant vector database collections.

Provides commands to create, delete, list and modify collections, as well as perform
batch operations on documents within collections.
"""
import os
import sys
import argparse
import logging
from pathlib import Path

try:
    from qdrant_client import QdrantClient
    from qdrant_client.http import models
except ImportError:
    print("Error: qdrant-client is not installed. Please run: pip install qdrant-client")
    sys.exit(1)

from qdrant_manager.config import get_profiles, get_config_dir
from qdrant_manager.utils import load_configuration, initialize_qdrant_client

from qdrant_manager.commands.create import create_collection
from qdrant_manager.commands.delete import delete_collection
from qdrant_manager.commands.list_cmd import list_collections
from qdrant_manager.commands.info import collection_info
from qdrant_manager.commands.batch import batch_operations
from qdrant_manager.commands.get import get_points

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Qdrant Manager - CLI tool for managing Qdrant vector database collections",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    # Command argument
    parser.add_argument(
        "command", 
        choices=["create", "delete", "list", "info", "batch", "config", "get"],
        help="""Command to execute:
  create: Create a new collection
  delete: Delete an existing collection
  list: List all collections
  info: Get detailed information about a collection
  batch: Perform batch operations on documents
  config: View or modify configuration (basic view implemented)
  get: Retrieve points from a collection"""
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
    
    # Collection creation arguments (used by 'create')
    collection_create = parser.add_argument_group("Collection Creation Options (for 'create')")
    collection_create.add_argument(
        "--size",
        type=int,
        help="Vector size for the collection (uses config default if not specified)"
    )
    collection_create.add_argument(
        "--distance",
        choices=["cosine", "euclid", "dot"],
        help="Distance function for vector similarity (uses config default if not specified)"
    )
    collection_create.add_argument(
        "--indexing-threshold",
        type=int,
        help="Indexing threshold (number of vectors before indexing, 0 for immediate indexing)"
    )
    collection_create.add_argument(
        "--overwrite", 
        action="store_true",
        help="Overwrite collection if it already exists during creation"
    )

    # Batch command arguments (used by 'batch')
    batch_group = parser.add_argument_group("Batch Operation Options (for 'batch')")
    # Document selection
    doc_selector = batch_group.add_mutually_exclusive_group(required=False) # Made not strictly required at parser level, checked in command func
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
    op_type = batch_group.add_mutually_exclusive_group(required=False) # Made not strictly required at parser level, checked in command func
    op_type.add_argument(
        "--add",
        action="store_true",
        help="Add/update fields in documents (merges payload)"
    )
    op_type.add_argument(
        "--delete",
        action="store_true",
        help="Delete fields from documents (requires --selector)"
    )
    op_type.add_argument(
        "--replace",
        action="store_true",
        help="Replace payload in documents (requires --selector, currently only works with --ids/--id-file)"
    )
    # Batch parameters
    batch_group.add_argument(
        "--doc",
        help="JSON string containing document payload data for add/replace operations (e.g., '{\"field1\":\"value1\"}')"
    )
    batch_group.add_argument(
        "--selector",
        help="""JSON path selector for where to add/delete/replace fields (e.g., 'metadata.author').
Required for --delete and --replace.
For --add, if omitted, adds to root; if provided, adds under that key."""
    )
    batch_group.add_argument(
        "--limit", # Also used by 'get'
        type=int,
        default=10000,
        help="Maximum number of points to process for --filter in 'batch' or retrieve in 'get' (default: 10000 for batch, 10 for get)"
    )
    
    # Get command arguments (used by 'get')
    get_params = parser.add_argument_group("Get/Retrieve Options (for 'get')")
    # Selection for 'get' uses --ids, --id-file, --filter from batch options
    get_params.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json", # Default format is json
        help="Output format (default: json)"
    )
    get_params.add_argument(
        "--output",
        help="Output file path (prints to stdout if not specified)"
    )
    get_params.add_argument(
        "--with-vectors",
        action="store_true",
        help="Include vector data in output (default: False)"
    )

    args = parser.parse_args()
    
    # Handle config command separately (doesn't need client initialization)
    if args.command == "config":
        if len(sys.argv) == 2 or (len(sys.argv) == 3 and args.profile):
             # Just show available profiles or config path
            profiles = get_profiles()
            print("Available configuration profiles:")
            for profile in profiles:
                print(f"  - {profile}")
            config_path = get_config_dir() / 'config.yaml'
            if args.profile:
                 print(f"\nUsing profile: {args.profile}")
                 # Try loading to show resolved path
                 try:
                    load_config(args.profile) # Load config to potentially create default if missing
                    print(f"Configuration source: {config_path}")
                 except Exception as e:
                    print(f"Could not load profile '{args.profile}': {e}")
            else:
                 print(f"\nDefault configuration file: {config_path}")
            sys.exit(0)
        else:
            # Basic argument validation or help could go here if needed
            print("Config command currently only shows profiles and config path.")
            print("Use 'qdrant-manager config --profile <name>' to see the path for a specific profile.")
            # Future: Add subcommands like 'qdrant-manager config set key value'
            sys.exit(0)
    
    # Load configuration using the utility function
    config = load_configuration(args)
    
    # Determine collection name (use --collection arg first, then config)
    collection_name = args.collection if args.collection else config.get("collection", "")
    
    # Check if collection name is required for the command but missing
    if args.command in ["create", "delete", "info", "batch", "get"] and not collection_name:
         logger.error(f"Collection name is required for command '{args.command}'.")
         logger.error("Please provide --collection argument or set 'collection' in your config/profile.")
         sys.exit(1)
    
    # Initialize Qdrant client using the utility function
    client = initialize_qdrant_client(config)
    
    # Execute the requested command by calling the appropriate handler function
    if args.command == "create":
        # Pass the loaded config and args to the command handler
        create_collection(client, collection_name, args.overwrite, config, args)
    
    elif args.command == "delete":
        delete_collection(client, collection_name)
    
    elif args.command == "list":
        list_collections(client)
    
    elif args.command == "info":
        collection_info(client, collection_name)
        
    elif args.command == "batch":
        # Pass the full args object to batch_operations
        batch_operations(client, collection_name, args)
        
    elif args.command == "get":
        # Adjust default limit for 'get' if not specified
        if not hasattr(args, 'limit') or args.limit == 10000: # Check if default batch limit was used
             args.limit = 10 # Override with get default limit
        # Pass the full args object to get_points
        get_points(client, collection_name, args)

if __name__ == "__main__":
    main()