import os
import json
import uuid
from .app_state import AppState
from .constants import CONFIG_FILE, BACKUP_DIR, DATA_DIR

def ensure_directories():
    """Ensure all necessary directories exist."""
    os.makedirs(DATA_DIR, mode=0o755, exist_ok=True)
    os.makedirs(BACKUP_DIR, mode=0o777, exist_ok=True)

def load_config():
    ensure_directories()

    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, mode='w', encoding='utf-8') as file:
            json.dump({
                "INSTANCE_UUID": str(uuid.uuid4()),
                "HOST": "127.0.0.1",
                "PORT": "6446",
                "USERNAME": "",
                "PASSWORD": "",
                "AUTO_UPDATE": "1",
                "SAVE_ON_FILE_INTERVAL": "15",
                "BACKUP_ON_SHUTDOWN": "0",
                "SCHEDULER": "1",
                "REPLICATION": "0",
                "REPLICATION_TYPE": "MASTER",
                "REPLICATION_MASTER": "",
                "REPLICATION_SLAVES": [],
                "SHARDING_TYPE": "MASTER",
                "SHARDING": "0",
                "SHARDING_BATCH_SIZE": "10",
                "SHARDS": []
            }, file, indent=4)
    
    with open(CONFIG_FILE, mode='r') as file:
        try:
            loaded_data = json.load(file)
            AppState().config_store.update(loaded_data)
            AppState().auth_data = {"username": AppState().config_store.get('USERNAME'), "password": AppState().config_store.get('PASSWORD')}
            slaves = AppState().config_store.get('REPLICATION_AUTHORIZED_SLAVES', [])
            shards = AppState().config_store.get('SHARDS', [])
            if not isinstance(slaves, list):
                AppState().config_store['REPLICATION_AUTHORIZED_SLAVES'] = []
            if not isinstance(shards, list):
                AppState().config_store['SHARDS'] = []
            return loaded_data
        except json.JSONDecodeError:
            return {}

def save_config():
    with open(CONFIG_FILE, mode='w', encoding='utf-8') as file:
        json.dump(AppState().config_store, file, indent=4)
