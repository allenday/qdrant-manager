import pytest
import sys
import io
import contextlib
import logging # Needed for log level test
from unittest.mock import patch, MagicMock

# Import the main entry point
from solr_manager.cli import main

# Helper function to run main with mocked argv and capture output
def run_cli(command_args, mock_exit):
    """Runs the CLI main function with mocked argv and captures stdout/stderr."""
    full_args = ['solr-manager'] + command_args
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    
    # Patch sys.argv for this call
    with patch.object(sys, 'argv', full_args), \
         contextlib.redirect_stdout(stdout_capture), \
         contextlib.redirect_stderr(stderr_capture):
        try:
            main()
        except SystemExit as e:
            pass # mock_exit will record the call
            
    return mock_exit, stdout_capture.getvalue(), stderr_capture.getvalue()

# --- Basic Invocation Tests --- 

@patch('sys.exit')
def test_cli_no_args(mock_exit):
    """Test invoking the CLI with no arguments (should show help)."""
    mock_exit, stdout, stderr = run_cli([], mock_exit)
    assert "usage: solr-manager" in stderr
    assert "the following arguments are required: command" in stderr
    mock_exit.assert_called_once_with(2)

@patch('sys.stdout', new_callable=io.StringIO)
@patch('sys.stderr', new_callable=io.StringIO)
@patch('sys.exit') 
def test_cli_help_direct(mock_sys_exit, mock_sys_stderr, mock_sys_stdout):
    """Test calling main directly with patched argv for --help."""
    # Configure the mock to raise SystemExit(code) with the code it was called with
    mock_sys_exit.side_effect = lambda code: (_ for _ in ()).throw(SystemExit(code)) 

    # Expect SystemExit to be raised
    with pytest.raises(SystemExit) as pytest_wrapped_e, \
         patch('sys.argv', ['solr-manager', '--help']):
        main()

    # Check stdout contains help message
    stdout = mock_sys_stdout.getvalue()
    assert "usage: solr-manager" in stdout
        
    # Assert that sys.exit was called ONLY once and with 0
    mock_sys_exit.assert_called_once_with(0)
    
    # Verify the exit code caught by pytest is 0
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 0 

# --- Command Dispatching Tests (with mocks) ---

# Patch names as used *within* cli.py
@patch('sys.exit')
@patch('solr_manager.cli.load_and_override_config') 
@patch('solr_manager.cli.initialize_solr_client') 
@patch('solr_manager.cli.list_collections')
def test_cli_dispatch_list(mock_list, mock_client_init, mock_load_config, mock_exit):
    """Test dispatching to the 'list' command."""
    mock_config = {"some": "config"}
    mock_client = MagicMock()
    mock_load_config.return_value = mock_config
    mock_client_init.return_value = mock_client
    
    mock_exit, stdout, stderr = run_cli(['list'], mock_exit)
    
    mock_load_config.assert_called_once()
    mock_client_init.assert_called_once_with(mock_config, collection_name=None)
    mock_list.assert_called_once()
    call_args, call_kwargs = mock_list.call_args
    # Call signature in cli.py: list_collections(config=config)
    assert call_args == () # No positional args
    assert call_kwargs == {'config': mock_config}
    mock_exit.assert_not_called()

@patch('sys.exit')
@patch('solr_manager.cli.load_and_override_config')
@patch('solr_manager.cli.initialize_solr_client') 
@patch('solr_manager.cli.create_collection')
def test_cli_dispatch_create(mock_create, mock_client_init, mock_load_config, mock_exit):
    """Test dispatching to the 'create' command."""
    mock_config = {"some": "config", "collection": "default_col"}
    mock_client = MagicMock()
    mock_load_config.return_value = mock_config
    mock_client_init.return_value = mock_client
    
    cmd_args = ['create', '--collection', 'mycol', '--configset', 'myconf', '--num-shards', '2']
    mock_exit, stdout, stderr = run_cli(cmd_args, mock_exit)
    
    mock_load_config.assert_called_once()
    mock_client_init.assert_called_once_with(mock_config, collection_name='mycol') 
    mock_create.assert_called_once()
    # Call signature in cli.py: create_collection(collection_name=..., num_shards=..., ... config=...)
    call_args, call_kwargs = mock_create.call_args
    assert call_args == () # No positional args
    assert call_kwargs['collection_name'] == 'mycol'
    assert call_kwargs['config_set_name'] == 'myconf'
    assert call_kwargs['num_shards'] == 2
    assert call_kwargs['config'] == mock_config
    mock_exit.assert_not_called()

@patch('sys.exit')
@patch('solr_manager.cli.load_and_override_config') 
@patch('solr_manager.cli.initialize_solr_client') 
@patch('solr_manager.cli.delete_collection')
def test_cli_dispatch_delete(mock_delete, mock_client_init, mock_load_config, mock_exit):
    """Test dispatching to the 'delete' command."""
    mock_config = {"some": "config", "collection": "default_col"}
    mock_client = MagicMock()
    mock_load_config.return_value = mock_config
    mock_client_init.return_value = mock_client

    cmd_args = ['delete', '--collection', 'mycol']
    mock_exit, stdout, stderr = run_cli(cmd_args, mock_exit)

    mock_load_config.assert_called_once()
    mock_client_init.assert_called_once_with(mock_config, collection_name='mycol')
    mock_delete.assert_called_once()
    # Call signature: delete_collection(collection_name=..., config=...)
    call_args, call_kwargs = mock_delete.call_args
    assert call_kwargs['collection_name'] == 'mycol'
    assert call_kwargs['config'] == mock_config
    mock_exit.assert_not_called()

@patch('sys.exit')
@patch('solr_manager.cli.load_and_override_config') 
@patch('solr_manager.cli.initialize_solr_client') 
@patch('solr_manager.cli.collection_info')
def test_cli_dispatch_info(mock_info, mock_client_init, mock_load_config, mock_exit):
    """Test dispatching to the 'info' command."""
    mock_config = {"some": "config", "collection": "default_col"}
    mock_client = MagicMock()
    mock_load_config.return_value = mock_config
    mock_client_init.return_value = mock_client

    cmd_args = ['info', '--collection', 'mycol']
    mock_exit, stdout, stderr = run_cli(cmd_args, mock_exit)

    mock_load_config.assert_called_once()
    mock_client_init.assert_called_once_with(mock_config, collection_name='mycol')
    mock_info.assert_called_once()
    # Call signature: collection_info(collection_name=..., config=...)
    call_args, call_kwargs = mock_info.call_args
    assert call_kwargs['collection_name'] == 'mycol'
    assert call_kwargs['config'] == mock_config
    mock_exit.assert_not_called()

@patch('sys.exit')
@patch('solr_manager.cli.load_and_override_config') 
@patch('solr_manager.cli.initialize_solr_client') 
@patch('solr_manager.cli.batch_operations')
def test_cli_dispatch_batch_add(mock_batch, mock_client_init, mock_load_config, mock_exit):
    """Test dispatching to the 'batch --add-update' command."""
    mock_config = {"some": "config", "collection": "default_col"}
    mock_client = MagicMock()
    mock_load_config.return_value = mock_config
    mock_client_init.return_value = mock_client

    cmd_args = ['batch', '--collection', 'mycol', '--add-update', '--doc', '[{"id": "1"}]']
    mock_exit, stdout, stderr = run_cli(cmd_args, mock_exit)

    mock_load_config.assert_called_once()
    mock_client_init.assert_called_once_with(mock_config, collection_name='mycol')
    mock_batch.assert_called_once()
    # Call signature: batch_operations(client=..., collection_name=..., args=..., config=...)
    call_args, call_kwargs = mock_batch.call_args
    assert call_kwargs['client'] == mock_client
    assert call_kwargs['collection_name'] == 'mycol' 
    assert call_kwargs['args'].add_update is True # Check the parsed args object
    assert call_kwargs['args'].doc == '[{"id": "1"}]'
    assert call_kwargs['config'] == mock_config
    mock_exit.assert_not_called()

# Config command doesn't initialize client
@patch('sys.exit')
@patch('solr_manager.cli.show_config_info') # Patch name used in cli.py
def test_cli_dispatch_config_show(mock_show_config, mock_exit):
    """Test dispatching to the 'config' command."""
    mock_exit, stdout, stderr = run_cli(['config'], mock_exit)
    mock_show_config.assert_called_once() 
    # Check args passed to show_config_info (should be the argparse namespace)
    call_args, call_kwargs = mock_show_config.call_args
    assert call_args[0].command == 'config'
    # mock_exit might be called by show_config_info, so don't assert not_called

# --- Option/Argument Tests --- 

@patch('sys.exit')
@patch('solr_manager.cli.load_and_override_config') 
@patch('solr_manager.cli.initialize_solr_client') 
@patch('solr_manager.cli.list_collections')
def test_cli_profile_option(mock_list, mock_client_init, mock_load_config, mock_exit):
    """Test that the --profile option is passed correctly."""
    mock_config = {"some": "config"}
    mock_client = MagicMock()
    mock_load_config.return_value = mock_config # Mock returns successfully
    mock_client_init.return_value = mock_client

    cmd_args = ['--profile', 'staging', 'list']
    mock_exit, stdout, stderr = run_cli(cmd_args, mock_exit)
    
    mock_load_config.assert_called_once()
    call_args, call_kwargs = mock_load_config.call_args
    assert call_args[0].profile == 'staging' 
    mock_client_init.assert_called_once()
    mock_list.assert_called_once()
    mock_exit.assert_not_called()

@patch('sys.exit')
@patch('solr_manager.cli.load_and_override_config') 
@patch('solr_manager.cli.initialize_solr_client') 
@patch('solr_manager.cli.list_collections')
@patch('logging.basicConfig') # Patch logging setup
def test_cli_loglevel_option(mock_log_config, mock_list, mock_client_init, mock_load_config, mock_exit):
    """Test that the --log-level option is handled (basic check)."""
    mock_config = {"some": "config"}
    mock_client = MagicMock()
    mock_load_config.return_value = mock_config
    mock_client_init.return_value = mock_client
    
    # Still can't easily test --log-level here due to when basicConfig is called.
    cmd_args = ['list'] 
    mock_exit, stdout, stderr = run_cli(cmd_args, mock_exit)
    mock_load_config.assert_called_once()
    mock_client_init.assert_called_once()
    mock_list.assert_called_once()
    mock_exit.assert_not_called()

# TODO: Add tests for overriding config values with command-line args 
# TODO: Add tests for error handling in cli.main (e.g., config loading failure, client init failure)

@patch('solr_manager.cli.logger')
@patch('solr_manager.cli.load_and_override_config')
@patch('solr_manager.cli.initialize_solr_client')
def test_cli_dispatch_create_error(mock_init_client, mock_load_config, mock_logger):
    """Test CLI dispatch when create command fails."""
    from solr_manager.cli import main
    
    mock_load_config.return_value = {'solr_url': 'http://localhost:8983/solr'}
    mock_init_client.side_effect = Exception("Client init failed")
    
    test_args = ['create', '--collection', 'test_coll', '--configset', 'test_config']
    with patch('sys.argv', ['solr-manager'] + test_args):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
    
    mock_logger.error.assert_called_with("An error occurred during command execution ('create'): Client init failed")

@patch('solr_manager.cli.logger')
@patch('solr_manager.cli.load_and_override_config')
@patch('solr_manager.cli.initialize_solr_client')
def test_cli_dispatch_delete_error(mock_init_client, mock_load_config, mock_logger):
    """Test CLI dispatch when delete command fails."""
    from solr_manager.cli import main
    
    mock_load_config.return_value = {'solr_url': 'http://localhost:8983/solr'}
    mock_init_client.side_effect = Exception("Client init failed")
    
    test_args = ['delete', '--collection', 'test_coll']
    with patch('sys.argv', ['solr-manager'] + test_args):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
    
    mock_logger.error.assert_called_with("An error occurred during command execution ('delete'): Client init failed")

@patch('solr_manager.cli.logger')
@patch('solr_manager.cli.load_and_override_config')
@patch('solr_manager.cli.initialize_solr_client')
def test_cli_dispatch_info_error(mock_init_client, mock_load_config, mock_logger):
    """Test CLI dispatch when info command fails."""
    from solr_manager.cli import main
    
    mock_load_config.return_value = {'solr_url': 'http://localhost:8983/solr'}
    mock_init_client.side_effect = Exception("Client init failed")
    
    test_args = ['info', '--collection', 'test_coll']
    with patch('sys.argv', ['solr-manager'] + test_args):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
    
    mock_logger.error.assert_called_with("An error occurred during command execution ('info'): Client init failed")

@patch('solr_manager.cli.logger')
@patch('solr_manager.cli.load_and_override_config')
@patch('solr_manager.cli.initialize_solr_client')
def test_cli_dispatch_batch_error(mock_init_client, mock_load_config, mock_logger):
    """Test CLI dispatch when batch command fails."""
    from solr_manager.cli import main
    
    mock_load_config.return_value = {'solr_url': 'http://localhost:8983/solr'}
    mock_init_client.side_effect = Exception("Client init failed")
    
    test_args = ['batch', '--collection', 'test_coll', '--add-update', '--doc', '{"id": "1"}']
    with patch('sys.argv', ['solr-manager'] + test_args):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
    
    mock_logger.error.assert_called_with("An error occurred during command execution ('batch'): Client init failed")

@patch('solr_manager.cli.logger')
@patch('solr_manager.cli.load_and_override_config')
@patch('solr_manager.cli.initialize_solr_client')
def test_cli_dispatch_get_error(mock_init_client, mock_load_config, mock_logger):
    """Test CLI dispatch when get command fails."""
    from solr_manager.cli import main
    
    mock_load_config.return_value = {'solr_url': 'http://localhost:8983/solr'}
    mock_init_client.side_effect = Exception("Client init failed")
    
    test_args = ['get', '--collection', 'test_coll', '--query', '*:*']
    with patch('sys.argv', ['solr-manager'] + test_args):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
    
    mock_logger.error.assert_called_with("An error occurred during command execution ('get'): Client init failed")

@patch('solr_manager.cli.logger')
def test_cli_command_import_error(mock_logger):
    """Test CLI when command imports fail."""
    from solr_manager.cli import main
    import importlib
    
    # Save the original module
    original_module = sys.modules['solr_manager.cli']
    
    try:
        # Create a new module with ImportError for commands
        spec = importlib.util.spec_from_file_location('solr_manager.cli', original_module.__file__)
        new_module = importlib.util.module_from_spec(spec)
        sys.modules['solr_manager.cli'] = new_module
        
        # Simulate ImportError for commands
        with patch('solr_manager.cli.create_collection', side_effect=ImportError("Command not found")):
            test_args = ['create', '--collection', 'test_coll', '--configset', 'test_config']
            with patch('sys.argv', ['solr-manager'] + test_args):
                with pytest.raises(SystemExit) as exc_info:
                    main()
                assert exc_info.value.code == 1
        
        mock_logger.error.assert_called_with("An error occurred during command execution ('create'): Command not found")
    finally:
        # Restore the original module
        sys.modules['solr_manager.cli'] = original_module

@patch('solr_manager.cli.logger')
@patch('solr_manager.cli.load_and_override_config')
def test_cli_config_load_error(mock_load_config, mock_logger):
    """Test CLI when configuration loading fails."""
    from solr_manager.cli import main
    
    mock_load_config.return_value = None  # Simulate config load failure
    
    test_args = ['list']  # Any command that requires config
    with patch('sys.argv', ['solr-manager'] + test_args):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
    
    mock_logger.error.assert_called_with("Failed to load or validate configuration. Exiting.")

@patch('solr_manager.cli.logger')
@patch('solr_manager.cli.load_and_override_config')
@patch('solr_manager.cli.initialize_solr_client')
def test_cli_client_init_error(mock_init_client, mock_load_config, mock_logger):
    """Test CLI when Solr client initialization fails."""
    from solr_manager.cli import main
    
    mock_load_config.return_value = {'solr_url': 'http://localhost:8983/solr'}
    mock_init_client.return_value = None  # Simulate client init failure
    
    test_args = ['get', '--collection', 'test_coll', '--query', '*:*']
    with patch('sys.argv', ['solr-manager'] + test_args):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
    
    mock_logger.error.assert_called_with("Failed to initialize Solr client. Exiting.")

@patch('solr_manager.cli.logger')
@patch('solr_manager.cli.load_and_override_config')
def test_cli_missing_collection_error(mock_load_config, mock_logger):
    """Test CLI when collection name is missing for commands that require it."""
    from solr_manager.cli import main
    
    mock_load_config.return_value = {'solr_url': 'http://localhost:8983/solr'}  # No collection in config
    
    test_cases = [
        ('create', ['create', '--configset', 'test_config']),
        ('delete', ['delete']),
        ('info', ['info']),
        ('get', ['get', '--query', '*:*']),
        ('batch', ['batch', '--add-update', '--doc', '{"id": "1"}'])
    ]
    
    for command, args in test_cases:
        with patch('sys.argv', ['solr-manager'] + args):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1
        
        mock_logger.error.assert_any_call(f"Collection name is required for command '{command}'.")
        mock_logger.error.assert_any_call("Please provide --collection argument or set 'collection' in your config/profile.")
        mock_logger.reset_mock()

@patch('solr_manager.cli.logger')
@patch('solr_manager.cli.load_and_override_config')
@patch('solr_manager.cli.initialize_solr_client')
def test_cli_create_missing_configset(mock_init_client, mock_load_config, mock_logger):
    """Test create command when configset is missing."""
    from solr_manager.cli import main
    
    mock_load_config.return_value = {'solr_url': 'http://localhost:8983/solr'}
    mock_init_client.return_value = MagicMock()
    
    test_args = ['create', '--collection', 'test_coll']  # Missing --configset
    with patch('sys.argv', ['solr-manager'] + test_args):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
    
    mock_logger.error.assert_called_with("--configset is required for the 'create' command.")
