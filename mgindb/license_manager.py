import json
import os
import time
from .app_state import AppState
from .constants import DATA_FILE

class LicenseManager:
    def __init__(self):
        self.app_state = AppState()
    
    def has_license(self):
        return self.app_state.license