import logging
import json
from qdrant_client import QdrantClient
from qdrant_client.http import models
from qdrant_client.http.models import PointStruct, UpdateResult, Filter, FieldCondition, MatchValue

logger = logging.getLogger(__name__)

def _parse_ids(args):
    if args.id_file:
        try:
            with open(args.id_file, 'r') as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            logger.error(f"ID file not found: {args.id_file}")
            return None
    elif args.ids:
        return [id.strip() for id in args.ids.split(',') if id.strip()]
    return [] # Return empty list if neither is provided

def _parse_filter(args):
    if args.filter:
        try:
            filter_dict = json.loads(args.filter)
            # Basic validation - check if it has 'key' and 'match'
            if 'key' in filter_dict and 'match' in filter_dict:
                 # Qdrant client expects Filter model, construct it
                 must_conditions = []
                 # Assuming simple match for now, extend later if needed
                 if 'value' in filter_dict['match']:
                     must_conditions.append(
                         FieldCondition(
                             key=filter_dict['key'],
                             match=MatchValue(value=filter_dict['match']['value'])
                         )
                     )
                 # Add more condition types (range, geo, etc.) here based on filter_dict structure
                 
                 if must_conditions:
                    return Filter(must=must_conditions)
                 else:
                     logger.error("Could not parse filter structure.")
                     return None
            else:
                logger.warning("Invalid filter structure. Must contain 'key' and 'match'. Proceeding without filter.")
                return None
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in filter: {args.filter}")
            return None
    return None

def _parse_doc(args):
    if args.doc:
        try:
            return json.loads(args.doc)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in doc: {args.doc}")
            return None
    return None

def batch_operations(client: QdrantClient, collection_name: str, args):
    """Handles the logic for the 'batch' command."""
    if not collection_name:
        logger.error("Collection name is required for 'batch' command.")
        return

    point_ids = _parse_ids(args)
    qdrant_filter = _parse_filter(args)
    doc_payload = _parse_doc(args)
    selector = args.selector # JSON path selector string

    # Determine points to operate on
    points_selector = None
    if point_ids:
        points_selector = models.PointIdsList(points=point_ids)
        logger.info(f"Operating on {len(point_ids)} specified point IDs.")
    elif qdrant_filter:
        points_selector = qdrant_filter # Use the parsed filter directly
        logger.info(f"Operating on points matching filter: {args.filter}")
        logger.warning(f"Filter operations limited to first {args.limit} matching points.")
        # Note: Qdrant delete/set_payload_blocking work with filters directly
        # For add/replace via upsert, we might need to scroll first if we don't want to overwrite vectors.
    else:
        logger.error("Batch command requires --ids, --id-file, or --filter.")
        return

    operation = None
    if args.add:
        operation = "add"
        if not doc_payload:
            logger.error("--add operation requires --doc argument.")
            return
        logger.info(f"Adding/Updating payload: {args.doc} at path: {selector if selector else 'root'}")
    elif args.delete:
        operation = "delete"
        if not selector:
            logger.error("--delete operation requires --selector argument specifying fields to delete.")
            return
        logger.info(f"Deleting fields selected by: {selector}")
    elif args.replace:
        operation = "replace"
        if not doc_payload:
            logger.error("--replace operation requires --doc argument.")
            return
        if not selector:
             logger.error("--replace operation requires --selector argument specifying where to replace.")
             return
        logger.info(f"Replacing payload at {selector} with: {args.doc}")
    else:
        logger.error("Batch command requires an operation type: --add, --delete, or --replace.")
        return

    try:
        result: UpdateResult = None
        if operation == "add":
            # Use set_payload_blocking for adding/updating fields without affecting vectors
            # Qdrant merges the payload by default.
            # If selector is provided, the doc_payload is placed *under* that key.
            payload_to_set = {selector: doc_payload} if selector else doc_payload
            result = client.set_payload_blocking(
                collection_name=collection_name,
                payload=payload_to_set,
                points=points_selector, # Can be list of IDs or Filter
                key=selector, # Use key for path if selector provided (might be redundant? Test)
                wait=True
            )

        elif operation == "delete":
            # Use delete_payload_blocking
            result = client.delete_payload_blocking(
                collection_name=collection_name,
                keys=[selector], # List of JSON paths to delete
                points=points_selector, # Can be list of IDs or Filter
                wait=True
            )

        elif operation == "replace":
             # Use set_payload_blocking with overwrite=True semantics (via selector?)
             # The `key` parameter in set_payload might achieve this, need testing.
             # Or perhaps overwrite_payload is better?
             
             # Let's try overwrite_payload first as it seems more direct for replacement.
             # Note: Overwrite requires PointIdsList, not Filter.
             if isinstance(points_selector, Filter):
                 logger.error("Overwrite/Replace operation currently only supports --ids or --id-file, not --filter.")
                 logger.error("To replace based on a filter, consider fetching IDs first, then using --ids.")
                 # TODO: Implement scrolling + overwrite if filter is needed
                 return
             
             # Construct the payload structure required by overwrite_payload
             payload_to_overwrite = {selector: doc_payload} if selector else doc_payload

             result = client.overwrite_payload_blocking(
                 collection_name=collection_name,
                 payload=payload_to_overwrite, # The new payload
                 points=points_selector, # Must be PointIdsList
                 wait=True
             )

        logger.info(f"Batch operation completed. Status: {result.status}. Points affected (approx): {result.count if hasattr(result, 'count') else 'N/A'}")

    except Exception as e:
        logger.error(f"Batch operation failed: {e}")
        import traceback
        traceback.print_exc() # Print stack trace for detailed debugging 