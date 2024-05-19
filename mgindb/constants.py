import os  # Module for interacting with the operating system

# Base directory of the current file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Directory for data storage
DATA_DIR = os.path.join(BASE_DIR, 'data')

# Directory for backup storage
BACKUP_DIR = os.path.join(BASE_DIR, 'backup')

# Path to the configuration file
CONFIG_FILE = os.path.join(BASE_DIR, 'conf.json')

# Path to the main data file
DATA_FILE = os.path.join(DATA_DIR, 'data.json')

# Path to the indices file
INDICES_FILE = os.path.join(DATA_DIR, 'indices.json')

# Path to the scheduler file
SCHEDULER_FILE = os.path.join(DATA_DIR, 'scheduler.json')
