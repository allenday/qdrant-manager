# Qdrant Manager

A general-purpose command-line tool for managing Qdrant vector database collections and documents. Simplifies common Qdrant management tasks through a CLI interface.

## Features

- Create, delete, and list collections
- Get detailed information about collections
- Batch operations on documents:
  - Add fields to documents
  - Delete fields from documents
  - Replace fields in documents
- Support for JSON path selectors for precise document modifications
- Multiple configuration profiles support

## Installation

```bash
# From PyPI (once published)
pip install qdrant-manager

# From source
git clone https://github.com/allenday/qdrant-manager.git
cd qdrant-manager
pip install -e .
```

## Configuration

When first run, qdrant-manager will create a configuration file at:
- Linux/macOS: `~/.config/qdrant-manager/config.yaml`
- Windows: `%APPDATA%\qdrant-manager\config.yaml`

You can edit this file to add your Qdrant connection details and schema configuration:

```yaml
default:
  connection:
    url: localhost
    port: 6333
    api_key: ""
    collection: my-collection
  
  vectors:
    size: 256
    distance: cosine
    indexing_threshold: 0
  
  # Optional payload indices for optimized searching
  payload_indices:
    - field: category
      type: keyword
    - field: created_at
      type: datetime
    - field: price
      type: float

production:
  connection:
    url: your-production-instance.region.cloud.qdrant.io
    port: 6333
    api_key: your-production-api-key
    collection: production-collection
  
  vectors:
    size: 1536  # For OpenAI embeddings
    distance: cosine
    indexing_threshold: 1000
  
  payload_indices:
    - field: product_id
      type: keyword
    - field: timestamp
      type: datetime
```

Each profile can define its own:
- Connection settings 
- Vector configuration (size, distance metric, indexing behavior)
- Payload indices for optimized search performance

The YAML format makes it easy to maintain a clean, organized configuration across multiple environments.

You can switch between profiles using the `--profile` flag:

```bash
qdrant-manager --profile production list
```

You can also override any setting with command-line arguments.

## Usage

```
qdrant-manager <command> [options]
```

### Available Commands:

- `create`: Create a new collection
- `delete`: Delete an existing collection
- `list`: List all collections
- `info`: Get detailed information about a collection
- `batch`: Perform batch operations on documents
- `config`: View available configuration profiles

### Connection Options:

```
--profile PROFILE  Configuration profile to use
--url URL          Qdrant server URL
--port PORT        Qdrant server port
--api-key API_KEY  Qdrant API key
--collection NAME  Collection name
```

### Examples:

```bash
# List all collections
qdrant-manager list

# Create a new collection with custom settings
qdrant-manager create --collection my-collection --size 1536 --distance euclid

# Get info about a collection
qdrant-manager info --collection my-collection

# Add a field to documents matching a filter
qdrant-manager batch --filter '{"key":"category","match":{"value":"product"}}' \
  --add --doc '{"processed": true}'

# Delete a field from specific documents
qdrant-manager batch --ids "doc1,doc2,doc3" --delete --selector "metadata.temp_data"

# Replace fields in documents from an ID file
qdrant-manager batch --id-file my_ids.txt --replace --selector "metadata.source" \
  --doc '{"provider": "new-provider", "date": "2025-03-31"}'

# Switch between profiles
qdrant-manager --profile production list
```

## Changelog

### v0.1.3
- Fixed bug in collection creation with payload indices

### v0.1.2
- Added comprehensive test coverage
- Improved error handling

### v0.1.1
- Initial release with basic functionality
- Added configuration profiles support

## License

Apache-2.0