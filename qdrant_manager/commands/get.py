import logging
import json
import csv
import sys
import time
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, Filter, FieldCondition, MatchValue

logger = logging.getLogger(__name__)

def _parse_ids_for_get(args):
    # Reusing the ID parsing logic from batch, could be moved to utils if needed more widely
    if args.id_file:
        try:
            with open(args.id_file, 'r') as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            logger.error(f"ID file not found: {args.id_file}")
            return None
    elif args.ids:
        return [id.strip() for id in args.ids.split(',') if id.strip()]
    return None # Return None if neither is provided, indicates fetch all/use filter

def _parse_filter_for_get(args):
    """Parse filter string from command arguments."""
    if args.filter:
        try:
            filter_dict = json.loads(args.filter)
            
            # Handle empty filter case - return None to match everything
            if not filter_dict:
                logger.info("Empty filter provided, will match all documents")
                return None
                
            # Continue with normal filter parsing
            if 'key' in filter_dict and 'match' in filter_dict:
                must_conditions = []
                # Check different match value formats
                if 'value' in filter_dict['match']:
                    must_conditions.append(
                        FieldCondition(
                            key=filter_dict['key'],
                            match=MatchValue(value=filter_dict['match']['value'])
                        )
                    )
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

def get_points(client: QdrantClient, collection_name: str, args):
    """Handles the logic for the 'get' command."""
    if not collection_name:
        logger.error("Collection name is required for 'get' command.")
        return

    point_ids = _parse_ids_for_get(args)
    qdrant_filter = _parse_filter_for_get(args)
    limit = args.limit if hasattr(args, 'limit') and args.limit else 10 # Default limit for get
    with_payload = True # Always fetch payload for get
    with_vectors = args.with_vectors

    logger.info(f"Retrieving points from collection '{collection_name}'")
    if point_ids:
        logger.info(f"Retrieving specific IDs: {point_ids}")
    elif qdrant_filter:
        logger.info(f"Retrieving points matching filter: {args.filter} (limit: {limit})")
    else:
        logger.info(f"Retrieving all points (limit: {limit})")

    try:
        if point_ids:
            # Use retrieve for specific IDs
            points_data = client.retrieve(
                collection_name=collection_name,
                ids=point_ids,
                with_payload=with_payload,
                with_vectors=with_vectors
            )
        else:
            # Use scroll with pagination for filters or getting all points
            points_data = []
            # Use a small batch size to avoid timeouts
            batch_size = 1000  # Smaller batch size to prevent server rejection
            offset = None
            remaining = limit
            page_num = 1
            max_retries = 3
            
            while remaining > 0:
                current_batch_size = min(batch_size, remaining)
                logger.info(f"Fetching page {page_num} (up to {current_batch_size} points)...")
                
                # Implement retry logic
                retries = 0
                while retries < max_retries:
                    try:
                        batch_points, offset = client.scroll(
                            collection_name=collection_name,
                            scroll_filter=qdrant_filter,
                            limit=current_batch_size,
                            with_payload=with_payload,
                            with_vectors=with_vectors,
                            offset=offset  # Use the offset from previous call
                        )
                        break  # Success, break out of retry loop
                    except Exception as e:
                        retries += 1
                        if retries >= max_retries:
                            raise  # Re-raise if max retries reached
                        logger.warning(f"Batch retrieval error (attempt {retries}/{max_retries}): {e}")
                        time.sleep(1)  # Short delay before retry
                
                # Process the retrieved batch
                if not batch_points:
                    logger.info(f"No more points to retrieve after {len(points_data)} total points")
                    break
                    
                points_data.extend(batch_points)
                logger.info(f"Retrieved {len(batch_points)} points in this batch, {len(points_data)} total so far")
                
                page_num += 1
                remaining -= len(batch_points)
                
                # Break if we've retrieved all available points
                if not offset or len(batch_points) < current_batch_size:
                    logger.info(f"Retrieved all available points ({len(points_data)} total)")
                    break
            
            if offset and remaining <= 0:
                logger.warning(f"Reached limit of {limit} points. More points may exist.")

        if not points_data:
            logger.info("No points found matching the criteria.")
            return

        # Format and output
        output_format = args.format or "json"
        output_file = args.output

        output_handle = open(output_file, 'w', newline='') if output_file else sys.stdout

        try:
            if output_format == "json":
                # Convert PointStruct objects to dictionaries for JSON serialization
                points_list = [point.dict() for point in points_data]
                json.dump(points_list, output_handle, indent=2)
            
            elif output_format == "csv":
                if not points_data:
                    return # Nothing to write
                
                # Dynamically determine headers from the first point's payload + id (+ vector)
                headers = ['id']
                if points_data[0].payload:
                    headers.extend(points_data[0].payload.keys())
                if with_vectors and points_data[0].vector is not None:
                    # Handle named vectors or single vector
                    if isinstance(points_data[0].vector, dict):
                        headers.extend(points_data[0].vector.keys()) # Add vector names as headers
                    else:
                        headers.append('vector') # Single unnamed vector
                
                writer = csv.DictWriter(output_handle, fieldnames=headers, extrasaction='ignore')
                writer.writeheader()
                
                for point in points_data:
                    row = {'id': point.id}
                    if point.payload:
                        row.update(point.payload)
                    if with_vectors and point.vector is not None:
                         if isinstance(point.vector, dict):
                             row.update(point.vector) # Add named vectors
                         else:
                             row['vector'] = json.dumps(point.vector) # Serialize vector list/tuple
                    writer.writerow(row)
            
            if output_file:
                logger.info(f"Output written to {output_file}")
            else:
                print() # Add a newline after stdout output for clarity

        finally:
            if output_file:
                output_handle.close()

    except Exception as e:
        logger.error(f"Failed to retrieve points: {e}")
        import traceback
        traceback.print_exc() 