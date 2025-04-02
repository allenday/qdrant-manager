import os
import sys
import logging
try:
    from qdrant_client import QdrantClient
    from qdrant_client.http import models
except ImportError:
    print("Error: qdrant-client is not installed. Please run: pip install qdrant-client")
    sys.exit(1)

from qdrant_manager.config import load_config

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
        # Check if this is a cloud endpoint (contains hostname)
        is_cloud = "http" in env_vars["url"] or "." in env_vars["url"]
        
        if is_cloud:
            # For cloud endpoints, don't specify port separately
            client = QdrantClient(
                url=env_vars["url"],
                api_key=env_vars.get("api_key"),
                timeout=30,  # Reasonable timeout
                prefer_grpc=False,  # Use HTTP protocol
            )
            logger.info(f"Using cloud configuration for {env_vars['url']}")
        else:
            # For local/custom endpoints
            client = QdrantClient(
                url=env_vars["url"],
                port=env_vars["port"],
                api_key=env_vars.get("api_key"),
                timeout=30,
                prefer_grpc=False,
            )
        
        # Test connection
        client.get_collections()
        logger.info("Successfully connected to Qdrant")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to Qdrant: {e}")
        sys.exit(1) 