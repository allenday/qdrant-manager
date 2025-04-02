#!/usr/bin/env python3
"""
Solr Manager - CLI tool for managing SolrCloud collections and documents.

Provides commands to create, delete, list collections, get info, 
and perform batch operations (add/update/delete) on documents.
"""
import sys
import argparse
import logging

# Use relative imports for local modules
from .config import get_profiles, get_config_dir, load_config # Keep load_config for config command
from .utils import load_and_override_config, initialize_solr_client

# Import command handlers (assuming they will be adapted for Solr)
# These imports might fail until the command files are created/updated
try:
    from .commands.create import create_collection
    from .commands.delete import delete_collection
    from .commands.list import list_collections
    from .commands.info import collection_info
    from .commands.batch import batch_operations
    from .commands.get import get_documents
    from .commands.config import show_config_info # Import the new config command handler
except ImportError as e:
    # Allow cli to load for basic --help even if commands are not ready
    print(f"Warning: Could not import command modules: {e}", file=sys.stderr)
    # Define dummy functions to prevent NameError later if commands aren't needed
    def create_collection(*args, **kwargs): print("Create command not implemented.")
    def delete_collection(*args, **kwargs): print("Delete command not implemented.")
    def list_collections(*args, **kwargs): print("List command not implemented.")
    def collection_info(*args, **kwargs): print("Info command not implemented.")
    def batch_operations(*args, **kwargs): print("Batch command not implemented.")
    def get_documents(*args, **kwargs): print("Get command not implemented.")
    def show_config_info(*args, **kwargs): print("Config command not implemented (import failed).") # Dummy for config


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main command-line interface function."""
    parser = argparse.ArgumentParser(
        description="Solr Manager - CLI tool for managing SolrCloud collections and documents",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    # Command argument
    parser.add_argument(
        "command", 
        choices=["create", "delete", "list", "info", "batch", "config", "get"],
        help="""Command to execute:
  create: Create a new Solr collection (requires configSet)
  delete: Delete an existing Solr collection
  list: List all Solr collections
  info: Get detailed information about a Solr collection
  batch: Perform batch operations on Solr documents (add/update, delete)
  config: View available configuration profiles and paths
  get: Retrieve documents from a Solr collection"""
    )
    
    # --- Connection arguments (apply to most commands) ---
    connection_args = parser.add_argument_group('Connection Options')
    connection_args.add_argument(
        "--profile", 
        help="Configuration profile to use (defined in config.yaml)"
    )
    # Allow overriding connection details from config
    connection_args.add_argument(
        "--solr-url", 
        help="Base Solr URL (e.g., http://localhost:8983/solr). Overrides config."
    )
    connection_args.add_argument(
        "--zk-hosts", 
        help="ZooKeeper host string (e.g., zk1:2181,zk2:2181/solr). Overrides config."
    )
    connection_args.add_argument(
        "--username", 
        help="Username for Solr authentication. Overrides config."
    )
    connection_args.add_argument(
        "--password", 
        help="Password for Solr authentication. Overrides config."
    )
    # Collection is almost always needed, except for 'list' and 'config'
    connection_args.add_argument(
        "--collection", 
        help="Target Solr collection name. Overrides config."
    )
    connection_args.add_argument(
        "--timeout",
        type=int,
        help="Connection timeout in seconds (default: 30). Overrides config."
    )

    # --- Collection Creation arguments (used by 'create') ---
    collection_create = parser.add_argument_group("Collection Creation Options (for 'create')")
    collection_create.add_argument(
        "--num-shards",
        type=int,
        default=1,
        help="Number of shards for the new collection (default: 1)"
    )
    collection_create.add_argument(
        "--replication-factor",
        type=int,
        default=1,
        help="Replication factor for the new collection (default: 1)"
    )
    collection_create.add_argument(
        "--configset",
        # required=True, # Make required within the command logic, not parser
        help="Name of the configSet to use for the new collection (e.g., _default)"
    )
    collection_create.add_argument(
        "--overwrite", 
        action="store_true",
        help="Delete existing collection with the same name before creating"
    )

    # --- Batch command arguments (used by 'batch') ---
    batch_group = parser.add_argument_group("Batch Operation Options (for 'batch')")
    # Document selection
    doc_selector = batch_group.add_mutually_exclusive_group(required=False)
    doc_selector.add_argument(
        "--id-file", 
        help="Path to a file containing document IDs, one per line (for --delete-docs)"
    )
    doc_selector.add_argument(
        "--ids", 
        help="Comma-separated list of document IDs (for --delete-docs)"
    )
    doc_selector.add_argument(
        "--query", 
        help="Solr query string to select documents (e.g., 'category:product AND inStock:true')"
    )
    # Operation type
    op_type = batch_group.add_mutually_exclusive_group(required=False)
    op_type.add_argument(
        "--add-update",
        action="store_true",
        help="Add/update documents using the provided --doc data (JSON format)"
    )
    op_type.add_argument(
        "--delete-docs",
        action="store_true",
        help="Delete documents matching --query, --ids, or --id-file"
    )
    # Batch parameters
    batch_group.add_argument(
        "--doc",
        help="JSON string containing a single document or a list of documents for --add-update (e.g., '{\"id\":\"1\", \"field\":\"val\"}' or '[{\"id\":\"1\"}, {\"id\":\"2\"}]')"
    )
    batch_group.add_argument(
        "--commit",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Perform a Solr commit after the batch operation (default: True)"
    )
    batch_group.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of documents to send per batch request (for --add-update with large data)"
    )

    # --- Get command arguments (used by 'get') ---
    get_params = parser.add_argument_group("Get/Retrieve Options (for 'get')")
    # Document selection for 'get'
    get_selector = get_params.add_mutually_exclusive_group(required=False)
    get_selector.add_argument(
        "--id-file-get", # Use different dest to avoid clash if batch also uses it
        dest="id_file",
        help="Path to a file containing document IDs, one per line"
    )
    get_selector.add_argument(
        "--ids-get", # Use different dest
        dest="ids",
        help="Comma-separated list of document IDs"
    )
    get_selector.add_argument(
        "--query-get", # Use different dest
        dest="query",
        help="Solr query string to select documents (default: *:*)"
    )
    # Get parameters
    get_params.add_argument(
        "--fields",
        default="*", # Default to all fields
        help="Comma-separated list of fields to retrieve (default: *)"
    )
    get_params.add_argument(
        "--limit", 
        type=int,
        default=10,
        help="Maximum number of documents to retrieve (default: 10)"
    )
    get_params.add_argument(
        "--sort",
        help="Solr sort specification (e.g., 'score desc, id asc')"
    )
    get_params.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json",
        help="Output format (default: json)"
    )
    get_params.add_argument(
        "--output",
        help="Output file path (prints to stdout if not specified)"
    )

    args = parser.parse_args()
    
    # --- Config Command Handling ---
    if args.command == "config":
        # Call the dedicated config command handler
        show_config_info(args)
        # The show_config_info function now handles exiting

    # --- Main Command Execution --- 

    # Load configuration (profile + overrides) using the utility function
    config = load_and_override_config(args)
    if not config:
        logger.error("Failed to load or validate configuration. Exiting.")
        sys.exit(1)

    # Determine collection name (needed by most commands)
    # Use command-line --collection first, then config
    collection_name = args.collection if hasattr(args, 'collection') and args.collection else config.get("collection")

    # Check if collection name is required but missing
    if args.command not in ["list", "config"] and not collection_name:
         logger.error(f"Collection name is required for command '{args.command}'.")
         logger.error("Please provide --collection argument or set 'collection' in your config/profile.")
         sys.exit(1)

    # --- Execute the requested command ---
    # Pass the initialized Solr client and relevant args/config
    try:
        # Initialize Solr client using the utility function
        # Pass the connection config dict AND the resolved collection name
        solr_client = initialize_solr_client(config, collection_name=collection_name)
        if not solr_client:
            logger.error("Failed to initialize Solr client. Exiting.")
            sys.exit(1)

        if args.command == "create":
            if not args.configset:
                 logger.error("--configset is required for the 'create' command.")
                 sys.exit(1)
            create_collection(
                collection_name=collection_name, 
                num_shards=args.num_shards, 
                replication_factor=args.replication_factor,
                config_set_name=args.configset,
                overwrite=args.overwrite,
                config=config
            )
        
        elif args.command == "delete":
            delete_collection(
                collection_name=collection_name, 
                config=config
            )
        
        elif args.command == "list":
            # List command doesn't need collection_name, but needs config for base URL
            list_collections(
                config=config
            )
        
        elif args.command == "info":
            collection_info(
                collection_name=collection_name, 
                config=config
            )
            
        elif args.command == "batch":
            # Pass client, collection, args, and config (client needed for pysolr add/delete)
            batch_operations(
                client=solr_client, 
                collection_name=collection_name, 
                args=args,
                config=config # Keep config in case needed later?
            )
            
        elif args.command == "get":
            # Pass the client, collection, and relevant args
            get_documents(
                client=solr_client, 
                collection_name=collection_name, 
                args=args
            )
            
    except Exception as e:
        logger.error(f"An error occurred during command execution ('{args.command}'): {e}")
        # Optionally add more detailed logging or traceback here
        # import traceback
        # traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()