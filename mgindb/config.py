import os
import ujson
import uuid
from .app_state import AppState
from .constants import CONFIG_FILE, BACKUP_DIR, DATA_DIR

def ensure_directories():
    """
    Ensure all necessary directories exist.

    This function checks if the directories specified in DATA_DIR and BACKUP_DIR
    exist. If they do not, it creates them with appropriate permissions.
    DATA_DIR is created with permissions 755 (owner can read, write, and execute;
    others can read and execute). BACKUP_DIR is created with permissions 777
    (everyone can read, write, and execute).
    """
    os.makedirs(DATA_DIR, mode=0o755, exist_ok=True)
    os.makedirs(BACKUP_DIR, mode=0o777, exist_ok=True)

def get_default_config():
    """
    Get the default configuration settings.

    This function returns a dictionary containing the default configuration settings
    for the application. These settings include various parameters such as instance UUID,
    host, port, username, password, and other options.
    """
    return {
        "INSTANCE_UUID": str(uuid.uuid4()),  # Unique identifier for the instance
        "HOST": "127.0.0.1",  # Default host IP
        "PORT": "6446",  # Default port
        "USERNAME": "",  # Default username (empty)
        "PASSWORD": "",  # Default password (empty)
        "AUTO_UPDATE": "1",  # Enable auto-update by default
        "SAVE_ON_FILE_INTERVAL": "15",  # Interval for saving to file
        "BACKUP_ON_SHUTDOWN": "0",  # Disable backup on shutdown by default
        "SCHEDULER": "1",  # Enable scheduler by default
        "QUERY_CACHING": "1",  # Enable query caching by default
        "QUERY_CACHING_TTL": "300",  # Query caching time to live
        "REPLICATION": "0",  # Disable replication by default
        "REPLICATION_TYPE": "MASTER",  # Default replication type
        "REPLICATION_MASTER": "",  # Default replication master
        "REPLICATION_SLAVES": [],  # Default replication slaves
        "SHARDING_TYPE": "MASTER",  # Default sharding type
        "SHARDING": "0",  # Disable sharding by default
        "SHARDING_BATCH_SIZE": "500",  # Default sharding batch size
        "SHARDS": [],  # Default shards
        "BLOCKCHAIN": "0",  # Blockchain
        "BLOCKCHAIN_TYPE": "",  # Blockchain Type (MASTER OR SLAVE)
        "BLOCKCHAIN_NAME": "",  # Blockchain name
        "BLOCKCHAIN_DESCRIPTION": "",  # Blockchain description
        "BLOCKCHAIN_LOGO": "",  # Blockchain logo
        "BLOCKCHAIN_SYMBOL": "",  # Blockchain symbol
        "BLOCKCHAIN_DECIMAL": "8",  # Blockchain decimal
        "BLOCKCHAIN_SUPPLY": "",  # Blockchain supply
        "BLOCKCHAIN_MAX_SUPPLY": "",  # Blockchain max supply
        "BLOCKCHAIN_CAN_MINT": False,  # Blockchain can mint
        "BLOCKCHAIN_CAN_BURN": False,  # Blockchain can burn
        "BLOCKCHAIN_SETUP_FEE": "",  # Blockchain setup fee
        "BLOCKCHAIN_CONTRACT_FEE": "5000",  # Blockchain contract fee
        "BLOCKCHAIN_VALIDATOR_REWARD": "",  # Blockchain validator reward
        "BLOCKCHAIN_SYNC_CHUNKS": "100",  # Blockchain sync chunks
        "BLOCKCHAIN_BLOCK_MAX_SIZE": "100",  # Blockchain block max size
        "BLOCKCHAIN_BLOCK_AUTO_CREATION_INTERVAL": "60", # Blockchain automatic block creation interval
        "BLOCKCHAIN_CONF": {}  # Blockchain conf
    }

def load_config():
    """
    Load the configuration file and update the application state.

    This function ensures necessary directories exist by calling ensure_directories().
    It then checks if the configuration file (CONFIG_FILE) exists. If the file does not exist,
    it creates a new one with default settings.

    The function reads the configuration file, updates the AppState's config_store
    with the loaded data, and sets the authentication data in AppState based on the loaded
    configuration. It also ensures that the 'REPLICATION_AUTHORIZED_SLAVES' and 'SHARDS'
    settings in the configuration are lists. If the configuration file is not a valid JSON,
    it returns an empty dictionary.

    Additionally, it compares the loaded configuration with the default configuration,
    adding any new keys from the default configuration and removing any old keys that are
    not present in the default configuration.

    Returns:
        dict: The loaded and updated configuration data.
    """
    ensure_directories()
    default_config = get_default_config()

    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, mode='w', encoding='utf-8') as file:
            # Create a default configuration file with various settings
            ujson.dump(default_config, file, indent=4)
    
    with open(CONFIG_FILE, mode='r') as file:
        try:
            # Load the configuration from the file
            loaded_data = ujson.load(file)
        except ujson.JSONDecodeError:
            # Return an empty dictionary if the configuration file is invalid
            return {}

    # Update configuration with new keys or remove old keys
    updated_config = {**default_config, **loaded_data}  # Merge dictionaries
    # Remove keys not in default config
    updated_config = {key: updated_config[key] for key in default_config.keys()}

    with open(CONFIG_FILE, mode='w', encoding='utf-8') as file:
        ujson.dump(updated_config, file, indent=4)

    # Update the application's configuration store with the loaded data
    AppState().config_store.update(updated_config)
    # Set authentication data in the application state
    AppState().auth_data = {
        "username": AppState().config_store.get('USERNAME'),
        "password": AppState().config_store.get('PASSWORD')
    }
    # Validate 'REPLICATION_AUTHORIZED_SLAVES' and 'SHARDS' settings
    if not isinstance(AppState().config_store.get('REPLICATION_AUTHORIZED_SLAVES', []), list):
        AppState().config_store['REPLICATION_AUTHORIZED_SLAVES'] = []
    if not isinstance(AppState().config_store.get('SHARDS', []), list):
        AppState().config_store['SHARDS'] = []

    return updated_config

def save_config():
    """
    Save the current configuration to the configuration file.

    This function writes the current state of the AppState's config_store to
    the configuration file (CONFIG_FILE) in JSON format. It uses an indentation
    level of 4 for readability.
    """
    with open(CONFIG_FILE, mode='w', encoding='utf-8') as file:
        ujson.dump(AppState().config_store, file, indent=4)
