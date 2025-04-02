import logging
import json
import pysolr # Use pysolr again
import math
from tqdm import tqdm
from typing import Dict, Any

# Import helpers from utils
from ..utils import get_auth_tuple, get_admin_base_url

logger = logging.getLogger(__name__)

def _parse_ids(args):
    """Parse IDs from --ids or --id-file arguments."""
    if args.id_file:
        try:
            with open(args.id_file, 'r') as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            logger.error(f"ID file not found: {args.id_file}")
            return None # Indicate error
        except Exception as e:
            logger.error(f"Error reading ID file {args.id_file}: {e}")
            return None
    elif args.ids:
        return [id.strip() for id in args.ids.split(',') if id.strip()]
    return [] # Return empty list if neither is provided

def _parse_docs_from_arg(doc_arg: str):
    """Parse the --doc argument, expecting JSON for a single doc or list of docs."""
    if not doc_arg:
        return None
    try:
        docs = json.loads(doc_arg)
        if isinstance(docs, dict):
            return [docs]
        elif isinstance(docs, list):
            # Validate that each item in the list is a dictionary
            if all(isinstance(doc, dict) for doc in docs):
                return docs
            else:
                logger.error("Invalid JSON structure in --doc argument. All items in the list must be JSON objects.")
                return None
        else:
            logger.error("Invalid JSON structure in --doc argument. Expected a JSON object or list of objects.")
            return None
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in --doc argument: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing --doc argument: {e}")
        return None

def batch_operations(client: pysolr.Solr, collection_name: str, args, config: Dict[str, Any]):
    """Handles the logic for the 'batch' command using pysolr client add/delete methods."""
    # config is no longer needed here, client is pre-initialized for the collection
    logger.info(f"Executing 'batch' command for collection: {collection_name}")

    # --- Argument Parsing --- 
    # Wrap helper calls in try-except
    try:
        doc_ids = _parse_ids(args)
        if doc_ids is None: # Helper function indicated an error during parsing
             logger.error("Failed to parse document IDs.")
             return # Exit early
    except Exception as e:
        logger.error(f"Error during ID parsing: {e}")
        return # Exit early
        
    try:
        solr_query = args.query if hasattr(args, 'query') and args.query else None
        docs_to_add = _parse_docs_from_arg(args.doc) if hasattr(args, 'doc') and args.doc else None
        if args.add_update and docs_to_add is None and hasattr(args, 'doc') and args.doc:
             # If add_update is true and doc arg was provided but parsing failed (returned None)
             logger.error("Failed to parse documents from --doc argument.")
             return # Exit early
    except Exception as e:
        logger.error(f"Error during document/query parsing: {e}")
        return # Exit early

    commit = args.commit if hasattr(args, 'commit') else True
    batch_size = args.batch_size if hasattr(args, 'batch_size') else 500
    
    operation_performed = False
    
    # --- Execute Add/Update Operation --- 
    if args.add_update:
        operation_performed = True
        if not docs_to_add:
            logger.error("--add-update operation requires valid JSON data in the --doc argument.")
            return
            
        logger.info(f"Starting batch add/update via pysolr.add for {len(docs_to_add)} document(s). Batch size: {batch_size}, Commit: {commit}")
        
        num_batches = math.ceil(len(docs_to_add) / batch_size)
        
        try:
            for i in tqdm(range(num_batches), desc="Adding/Updating Docs", unit="batch"):
                batch_docs = docs_to_add[i * batch_size : (i + 1) * batch_size]
                # Use the standard add method - let pysolr handle format/endpoint
                client.add(batch_docs, commit=False) 
            
            if commit:
                logger.info("Performing final commit...")
                client.commit() 
                logger.info("Commit successful.")
            else:
                 logger.info("Skipping final commit as per --no-commit flag.")
                 
            logger.info(f"Batch add/update operation completed successfully for {len(docs_to_add)} documents.")

        except pysolr.SolrError as e:
             logger.error(f"Solr error during add/update operation: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during add/update: {e}")
            import traceback
            traceback.print_exc()
            
    # --- Execute Delete Operation --- 
    elif args.delete_docs:
        operation_performed = True
        delete_type = None # Keep track of how we are deleting

        try:
            if doc_ids:
                delete_type = "IDs"
                logger.info(f"Starting batch delete operation for {len(doc_ids)} document ID(s) via pysolr.delete(id=...). Commit: {commit}")
                # Use standard delete by ID
                client.delete(id=doc_ids, commit=commit)
            elif solr_query:
                delete_type = "query"
                logger.info(f"Starting batch delete by query via pysolr.delete(q=...). Query: '{solr_query}'. Commit: {commit}")
                # Use standard delete by query
                client.delete(q=solr_query, commit=commit)
            else:
                logger.error("--delete-docs operation requires --ids, --id-file, or --query to specify documents.")
                return

            logger.info(f"Batch delete operation ({delete_type}) completed successfully via pysolr.")
            if not commit:
                 logger.info("Commit was skipped as per --no-commit flag.")
                 
        except pysolr.SolrError as e:
             logger.error(f"Solr error during delete operation ({delete_type}): {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during delete ({delete_type}): {e}")
            import traceback
            traceback.print_exc()
            
    # Should be caught by argparser mutual exclusion, but check anyway
    if not operation_performed:
        logger.error("No batch operation specified (use --add-update or --delete-docs).") 