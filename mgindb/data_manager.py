import json
import os
import time
from .app_state import AppState
from .constants import DATA_FILE

class DataManager:
    def __init__(self):
        self.app_state = AppState()
        self.data_file = DATA_FILE

    def get_all_local_data(self):
        return self.app_state.data_store.copy()

    def load_data(self):
        if not os.path.exists(self.data_file):
            with open(self.data_file, mode='w', encoding='utf-8') as file:
                json.dump({}, file)
        try:
            with open(self.data_file, mode='r', encoding='utf-8') as file:
                loaded_data = json.load(file)
            self.app_state.data_store.update(loaded_data)
        except json.JSONDecodeError as e:
            # Log the error
            print(f"Failed to load data: {e}")
            return {}

    def save_data(self):
        try:
            if self.app_state.data_has_changed:
                with open(self.data_file, mode='w', encoding='utf-8') as file:
                    json.dump(self.app_state.data_store, file, indent=4)
                    self.app_state.data_has_changed = False
        except IOError as e:
            # Log the error
            print(f"Failed to save data: {e}")

    async def cleanup_expired_keys(self):
        current_time = time.time()
        keys_to_remove = [key for key, expire_at in self.app_state.expires_store.items() if expire_at < current_time]

        for key in keys_to_remove:
            key_parts = key.split(':')
            if self.nested_delete(self.app_state.data_store, key_parts):
                del self.app_state.expires_store[key]
                # Check and possibly clean up parent keys
                while len(key_parts) > 1:
                    key_parts.pop()  # Go up one level in the key hierarchy
                    parent_key = ':'.join(key_parts)
                    parent_data = self.get_nested(self.app_state.data_store, key_parts)
                    if parent_data is not None and not parent_data:  # Check if parent is empty
                        self.nested_delete(self.app_state.data_store, key_parts)  # Remove empty parent
                    else:
                        break  # Parent has other children or data, stop cleanup
            else:
                print(f"Failed to delete expired key: {key}")

    def nested_delete(self, data_store, key_parts):
        ref = data_store
        for part in key_parts[:-1]:
            if part in ref:
                ref = ref[part]
            else:
                return False
        if key_parts[-1] in ref:
            del ref[key_parts[-1]]
            return True
        return False

    def get_nested(self, data_store, key_parts):
        ref = data_store
        for part in key_parts:
            ref = ref.get(part, {})
            if not isinstance(ref, dict):
                return None  # Return None if any part of the path is not a dictionary
        return ref

    def process_nested_data(self, data):
        """Recursively process data of any type, converting strings to numerical values when possible."""
        if isinstance(data, dict):
            return {key: self.process_nested_data(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.process_nested_data(item) for item in data]
        elif isinstance(data, str):
            try:
                return int(data)
            except ValueError:
                try:
                    return float(data)
                except ValueError:
                    return data
        else:
            return data

    def prepare_data(self, data):
        """
        Recursively prepares data for transmission by ensuring all data types are
        compatible with JSON serialization, especially handling deeply nested structures.
        """
        if isinstance(data, dict):
            return {key: self.prepare_data(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.prepare_data(item) for item in data]
        elif isinstance(data, set):
            return [self.prepare_data(item) for item in data]  # Convert sets to list
        elif isinstance(data, (int, float, str)):
            return data
        else:
            raise TypeError(f"Unsupported data type: {type(data)}")

    # Helper function to convert sets to lists
    def convert_sets_to_lists(self, data):
        if isinstance(data, set):
            return list(data)
        elif isinstance(data, dict):
            return {k: self.convert_sets_to_lists(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.convert_sets_to_lists(item) for item in data]
        return data
