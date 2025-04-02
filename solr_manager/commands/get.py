import logging
import json
import csv
import sys
import pysolr

logger = logging.getLogger(__name__)

def _parse_ids(args):
    """Parse IDs from --ids or --id-file arguments."""
    if hasattr(args, 'id_file') and args.id_file:
        try:
            with open(args.id_file, 'r') as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            logger.error(f"ID file not found: {args.id_file}")
            return None # Indicate error
        except Exception as e:
            logger.error(f"Error reading ID file {args.id_file}: {e}")
            return None
    elif hasattr(args, 'ids') and args.ids:
        return [id.strip() for id in args.ids.split(',') if id.strip()]
    return [] # Return empty list if neither is provided

# Removed _parse_filter_for_get as Solr uses query strings

def get_documents(client: pysolr.Solr, collection_name: str, args):
    """Handles the logic for the 'get' command using pysolr client."""
    # collection_name is not directly used by pysolr.Solr client methods
    # as it's part of the client's URL, but good to log.
    logger.info(f"Executing 'get' command for collection handled by client: {collection_name}")

    # --- Argument Parsing & Query Building ---
    # Wrap ID parsing in try-except
    try:
        doc_ids = _parse_ids(args)
        if doc_ids is None and args.id_file: # Check if helper signaled error specifically for id_file
            logger.error(f"Failed to parse IDs from file: {args.id_file}")
            return
    except Exception as e:
        logger.error(f"Error during ID parsing: {e}")
        return

    # Build the query
    solr_query = "*:*" # Default query
    query_source = "default (*:*)" # For logging
    if doc_ids:
        # Escape IDs containing special characters if necessary (basic escaping)
        escaped_ids = [str(id).replace(':', '\\:').replace(' ', '\\ ') for id in doc_ids]
        solr_query = f"id:({' OR '.join(escaped_ids)})" # Assuming id field is 'id'
        query_source = f"{len(doc_ids)} IDs"
    elif args.query: # Use explicit query if provided
        solr_query = args.query
        query_source = f"query '{args.query}'"

    # --- Search Parameters --- 
    search_params = {
        'fl': args.fields,
        'rows': args.limit
    }
    if args.sort:
        search_params['sort'] = args.sort

    # --- Execute Search --- 
    logger.info(f"Searching collection '{collection_name}' using {query_source}...")
    logger.debug(f"Executing Solr query: q={solr_query}, params={search_params}")
    
    try:
        results = client.search(q=solr_query, **search_params)
        logger.info(f"Found {results.hits} total documents matching criteria.")
        
        if not results.docs:
             logger.info("No documents found matching the criteria.")
             # Write empty result if outputting
             docs_to_write = []
        else:
            docs_to_write = results.docs
            logger.info(f"Retrieved {len(docs_to_write)} documents (limited by --limit={args.limit}).")
            
    except pysolr.SolrError as e:
        logger.error(f"Solr search failed: {e}")
        return
    except Exception as e:
        logger.error(f"An unexpected error occurred during Solr search: {e}")
        return

    # --- Format and Output Results --- 
    output_file = args.output
    output_format = args.format.lower()

    # Determine output handle (stdout or file)
    output_handle = None
    try:
        # Use utf-8 encoding for files
        output_handle = open(output_file, 'w', newline='', encoding='utf-8') if output_file else sys.stdout
        is_file_output = bool(output_file)

        if output_format == 'json':
            # Use json.dump for potentially large results
            json.dump(docs_to_write, output_handle, indent=4)
            if not is_file_output: # Add newline if printing to stdout
                 print()

        elif output_format == 'csv':
            if not docs_to_write:
                logger.warning("No documents to write to CSV.")
                if is_file_output:
                     output_handle.write("\n") # Write empty line if file
                # else: print nothing to stdout
            else:
                # Dynamically get field names from the first document
                fieldnames = list(docs_to_write[0].keys())
                writer = csv.DictWriter(output_handle, fieldnames=fieldnames)
                
                writer.writeheader()
                writer.writerows(docs_to_write)

        if is_file_output:
            logger.info(f"Successfully wrote {len(docs_to_write)} documents to {output_file}")

    except IOError as e:
        logger.error(f"Error writing to output file {output_file}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during output formatting/writing: {e}")
    finally:
        # Ensure file handle is closed if it was opened
        if output_handle and output_handle is not sys.stdout:
            try:
                output_handle.close()
            except Exception as e:
                logger.error(f"Error closing output file {output_file}: {e}") 