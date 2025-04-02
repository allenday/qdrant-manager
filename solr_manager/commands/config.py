import sys
import logging
import yaml

# Use relative imports assuming this file is in the commands directory
from ..config import get_profiles, get_config_dir, load_config

logger = logging.getLogger(__name__)

def show_config_info(args):
    """Handles the logic for the 'config' command."""
    config_dir = get_config_dir()
    print(f"Configuration Directory: {config_dir}")
    config_path = config_dir / 'config.yaml'

    profiles = get_profiles() # No args needed
    
    # Handle case where profiles could not be loaded
    if profiles is None:
        print(f"Error: Could not load profiles from configuration file '{config_path}'", file=sys.stderr)
        sys.exit(1)
    else:
        print("Available Profiles:")
        for profile in profiles:
            print(f"  - {profile}")

        # Determine profile to show
        profile_name = args.profile if args.profile else 'default'
        print(f"\nCurrent profile ({profile_name}):")

        if profile_name not in profiles:
            print(f"Error: Profile '{profile_name}' not found in {config_path}", file=sys.stderr)
            sys.exit(1)
        else:
            # Use yaml.dump for clean formatting
            print(yaml.dump({profile_name: profiles[profile_name]}, default_flow_style=False))

    # Print config file status (moved outside profile logic)
    print(f"\nConfiguration file:    {config_path}")
    if config_path.exists():
            print("Status: Configuration file exists.")
            # Optionally show content or validation status here in the future
    else:
            print("Status: Configuration file does NOT exist.")
            print("        Run any other command (e.g., 'solr-manager list') to create a default config from the sample.")

    # Try loading the specified profile to validate it (if provided)
    if args.profile:
            print(f"\nChecking profile: '{args.profile}'")
            try:
                # Use the basic load_config just to check profile existence/syntax
                cfg = load_config(args.profile)
                if cfg:
                    print(f"Profile '{args.profile}' loaded successfully from {config_path}")
                # else: load_config should print errors
            except SystemExit: # Raised by load_config if profile not found etc.
                pass # Error message already printed by load_config
            except Exception as e:
                print(f"Could not load profile '{args.profile}': {e}", file=sys.stderr)
                # Don't exit here, just report the loading error
                
    # Removed the final sys.exit(0) 