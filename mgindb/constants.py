import os  # Module for interacting with the operating system

# Base directory of the current file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Directory for data storage
DATA_DIR = os.path.join(BASE_DIR, 'data')

# Directory for backup storage
BACKUP_DIR = os.path.join(BASE_DIR, 'backup')

# Directory for upload storage
UPLOAD_DIR = os.path.join(BASE_DIR, 'upload')

# Path to the configuration file
CONFIG_FILE = os.path.join(BASE_DIR, 'conf.json')

# Path to the blockchain file
BLOCKCHAIN_FILE = os.path.join(DATA_DIR, 'blockchain.json')
BLOCKCHAIN_DB = f'{DATA_DIR}/blockchain.db'

# Path to the pending transactions file
PENDING_TRANSACTIONS_FILE = os.path.join(DATA_DIR, 'pending_transactions.json')

# Path to the wallets file
WALLETS_FILE = os.path.join(DATA_DIR, 'wallets.json')

# Path to the main data file
DATA_FILE = os.path.join(DATA_DIR, 'data.json')

# Path to the indices file
INDICES_FILE = os.path.join(DATA_DIR, 'indices.json')

# Path to the scheduler file
SCHEDULER_FILE = os.path.join(DATA_DIR, 'scheduler.json')
