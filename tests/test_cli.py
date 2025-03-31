"""
Reorganized tests for the CLI module.

This file now serves as an entry point for importing all the CLI module tests,
which have been refactored into separate files for better organization.
"""
import pytest

# Import tests from the reorganized modules
from tests.cli.test_utils import (
    test_resolve_json_path,
    test_load_configuration,
    test_parse_filter,
    test_load_document_ids
)
from tests.cli.test_connection import test_initialize_qdrant_client
from tests.cli.test_collections import (
    test_create_collection_new,
    test_create_collection_existing,
    test_create_collection_overwrite,
    test_delete_collection,
    test_list_collections,
    test_collection_info
)
from tests.cli.test_batch_operations import (
    test_batch_add_operation,
    test_batch_delete_operation,
    test_batch_replace_operation,
    test_get_points_by_ids,
    test_get_points_by_filter,
    test_batch_operations
)
from tests.cli.test_main import (
    test_main_config,
    test_main_list,
    test_main_create,
    test_main_delete,
    test_main_info,
    test_main_batch
)

# Add a simple test to check that importable modules are working
def test_cli_module_exists():
    """Test that the CLI module can be imported."""
    from qdrant_manager import cli
    assert cli is not None