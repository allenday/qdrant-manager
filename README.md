# solr-manager

A general-purpose command-line tool for managing SolrCloud collections and documents. Simplifies common Solr management tasks through a CLI interface.

## Features

- Create, delete, and list Solr collections
- Get detailed information about Solr collections (e.g., schema, status)
- Retrieve documents from collections with flexible query options (Solr query syntax)
- Batch operations on documents:
  - Add/Update fields in documents
  - Delete fields from documents
  - Delete documents by query or ID
- Support for standard Solr query syntax and filters
- Multiple configuration profiles support

## Installation

```bash
# From PyPI (once published)
# pipx install solr-manager 

# From source
git clone https://github.com/allenday/qdrant-manager.git # Repository URL might change
cd qdrant-manager # Directory name might change
pipx install -e .
```

## Configuration

The CLI tool uses a configuration file (`config.yaml`) located in a platform-specific application support directory (e.g., `~/.config/solr-manager/` on Linux, `~/Library/Application Support/solr-manager/` on macOS). You can generate a default config using `solr-manager config generate`.

**Key Configuration Options:**

*   `connection`:
    *   `solr_url`: The base URL of **a** Solr node (e.g., `http://localhost:8983/solr`). This is used for both admin tasks (like listing collections via the Collections API) and collection-specific tasks (like adding documents).
    *   `zk_hosts`: **Alternatively**, for SolrCloud, you can provide a comma-separated list of ZooKeeper hosts (e.g., `zk1:2181,zk2:2181/solr`).
        *   If `solr_url` is **not** provided, the tool will attempt to discover a live Solr node via ZooKeeper to determine the base URL needed for admin commands (`list`, `create`, `delete`, `info`).
        *   The `batch` command will **always** use `zk_hosts` if available to initialize its `SolrClient` for interacting with a specific collection, regardless of whether `solr_url` is also present.
    *   `collection`: The default target Solr collection name for commands like `batch` or `get` if not specified via `--collection`.
    *   `username` / `password`: Optional Basic Authentication credentials.
*   `defaults`:
    *   `timeout`: Default request timeout in seconds.
    *   `batch_size`: Default number of documents per batch for `batch` commands.
    *   `commit_within`: Default `commitWithin` value for `batch --add-update`.

*   `logging`:
    *   `level`: Logging level (e.g., `DEBUG`, `INFO`, `WARNING`, `ERROR`).
    *   `format`: Log message format.

**Profiles:**

You can define multiple connection profiles (e.g., `default`, `production`, `staging`) and select one using the `--profile` option.

## Usage

```
solr-manager <command> [options]
```

### Available Commands (Planned/Initial):

- `create`: Create a new Solr collection (may require pre-defined config set)
- `delete`: Delete an existing Solr collection
- `list`: List all Solr collections
- `info`: Get detailed information about a collection (e.g., schema, status)
- `batch`: Perform batch operations on documents (add, update, delete)
- `get`: Retrieve documents from a collection using Solr queries
- `config`: View available configuration profiles

### Connection Options:

```
--profile PROFILE  Configuration profile to use
--solr-url URL     Solr base URL (e.g., http://host:port/solr)
# --port PORT        (Port is usually part of solr-url)
# --api-key API_KEY  (Authentication might use username/password or other methods)
--collection NAME  Solr collection name
# --zk-hosts HOSTS   ZooKeeper hosts string (alternative connection method)
# --username USER    Solr username (if basic auth is used)
# --password PASS    Solr password (if basic auth is used)
```

### Examples:

```bash
# List all Solr collections
solr-manager list

# Create a new collection (details TBD, might need config set name)
# solr-manager create --collection my-new-collection --configset _default 

# Get info about a collection
solr-manager info --collection my_solr_collection

# Retrieve documents matching a query
solr-manager get --query 'category:product AND inStock:true' --fields 'id,name,price'

# Retrieve documents by ID and save as CSV
solr-manager get --query 'id:(doc1 OR doc2 OR doc3)' --format csv --output results.csv

# Add/update a field in documents matching a query
solr-manager batch --query 'category:product' --update '{"processed_b": true}'

# Delete documents matching a query
solr-manager batch --delete-query 'status:obsolete'

# Delete documents by ID from an ID file
solr-manager batch --id-file my_ids.txt --delete-ids

# Switch between profiles
solr-manager --profile production list
```

## Changelog 
# (Keep existing changelog for now, maybe add a note about the Solr pivot)

### v0.2.0 (In Progress - Solr Refactor)
- Refactoring core logic to use SolrCloud instead of Qdrant.
- Updating configuration structure for Solr.
- Renaming tool to `solr-manager`.

### v0.1.6 
# ... existing changelog entries ...

## License

Apache-2.0