import json  # Module for JSON operations
import os  # Module for interacting with the operating system
import time  # Module for time-related functions
from .app_state import AppState  # Import application state management
from .constants import DATA_FILE  # Import constant for data file path

class DataManager:
    def __init__(self):
        """Initialize DataManager with application state and data file path."""
        self.app_state = AppState()
        self.data_file = DATA_FILE

    def get_all_local_data(self):
        """Return a copy of all local data stored in the application state."""
        return self.app_state.data_store.copy()

    def load_data(self):
        """
        Load data from the data file into the application state.

        If the data file does not exist, create an empty one. Attempt to load
        the data from the file and update the application state. Handle JSON
        decode errors and return an empty dictionary if an error occurs.
        """
        if not os.path.exists(self.data_file):
            with open(self.data_file, mode='w', encoding='utf-8') as file:
                json.dump({}, file)
        try:
            with open(self.data_file, mode='r', encoding='utf-8') as file:
                loaded_data = json.load(file)
            self.app_state.data_store.update(loaded_data)
        except json.JSONDecodeError as e:
            print(f"Failed to load data: {e}")
            return {}

    def save_data(self):
        """
        Save the current data in the application state to the data file.

        If there have been changes to the data, write the updated data store
        to the data file and reset the data change flag. Handle I/O errors.
        """
        try:
            if self.app_state.data_has_changed:
                with open(self.data_file, mode='w', encoding='utf-8') as file:
                    json.dump(self.app_state.data_store, file, indent=4)
                    self.app_state.data_has_changed = False
        except IOError as e:
            print(f"Failed to save data: {e}")

    async def cleanup_expired_keys(self):
        """
        Asynchronously clean up expired keys from the data store.

        Remove keys that have expired based on their expiration times. Clean up
        empty parent keys after removing expired keys.
        """
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
        """
        Delete a nested key from the data store.

        Args:
            data_store (dict): The data store.
            key_parts (list): The parts of the key to delete.

        Returns:
            bool: True if the key was successfully deleted, False otherwise.
        """
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
        """
        Get the value of a nested key from the data store.

        Args:
            data_store (dict): The data store.
            key_parts (list): The parts of the key to get.

        Returns:
            The value of the nested key, or None if the key does not exist.
        """
        ref = data_store
        for part in key_parts:
            ref = ref.get(part, {})
            if not isinstance(ref, dict):
                return None  # Return None if any part of the path is not a dictionary
        return ref

    def process_nested_data(self, data):
        """
        Recursively process data of any type, converting strings to numerical values when possible.

        Args:
            data: The data to process.

        Returns:
            The processed data with strings converted to integers or floats where possible.
        """
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
        Recursively prepare data for transmission by ensuring all data types are compatible with JSON serialization.

        Args:
            data: The data to prepare.

        Returns:
            The prepared data with all sets converted to lists and unsupported data types handled.
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

    def convert_sets_to_lists(self, data):
        """
        Helper function to convert sets to lists recursively.

        Args:
            data: The data to convert.

        Returns:
            The data with all sets converted to lists.
        """
        if isinstance(data, set):
            return list(data)
        elif isinstance(data, dict):
            return {k: self.convert_sets_to_lists(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.convert_sets_to_lists(item) for item in data]
        return data