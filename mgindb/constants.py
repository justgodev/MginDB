import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(BASE_DIR, 'data')
BACKUP_DIR = os.path.join(BASE_DIR, 'backup')

CONFIG_FILE = os.path.join(BASE_DIR, 'conf.json')
DATA_FILE = os.path.join(DATA_DIR, 'data.json')
INDICES_FILE = os.path.join(DATA_DIR, 'indices.json')
SCHEDULER_FILE = os.path.join(DATA_DIR, 'scheduler.json')