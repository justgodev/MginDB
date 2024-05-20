import json  # Module for JSON operations
import time  # Module for time-related functions
from .app_state import AppState  # Import application state management

class CacheManager:
    def __init__(self):
        """Initialize CacheManager with application state."""
        self.app_state = AppState()

    def add_to_cache(self, command, query_key, query_result, ttl):
        current_time = time.time()
        expiration_time = current_time + ttl
        self.app_state.data_store_cache[command] = {
            "result": query_result,
            "last_accessed": current_time,
            "expiration": expiration_time
        }
        self.app_state.data_store_cache_keys_expiration[command] = expiration_time
        if query_key not in self.app_state.data_store_key_command_mapping:
            self.app_state.data_store_key_command_mapping[query_key] = []
        self.app_state.data_store_key_command_mapping[query_key].append(command)

        # Track individual keys involved in the query
        if isinstance(query_result, list):
            for entry in query_result:
                if 'key' in entry:
                    individual_key = f"{query_key}:{entry['key']}"
                    if individual_key not in self.app_state.data_store_key_command_mapping:
                        self.app_state.data_store_key_command_mapping[individual_key] = []
                    self.app_state.data_store_key_command_mapping[individual_key].append(command)
    
    def remove_from_cache(self, query_key):
        if query_key in self.app_state.data_store_key_command_mapping:
            related_commands = self.app_state.data_store_key_command_mapping.pop(query_key)
            for command in related_commands:
                if command in self.app_state.data_store_cache:
                    del self.app_state.data_store_cache[command]
                if command in self.app_state.data_store_cache_keys_expiration:
                    del self.app_state.data_store_cache_keys_expiration[command]

        # Also remove broader queries that depend on this key
        for key, commands in self.app_state.data_store_key_command_mapping.items():
            if query_key in key:
                for command in commands:
                    if command in self.app_state.data_store_cache:
                        del self.app_state.data_store_cache[command]
                    if command in self.app_state.data_store_cache_keys_expiration:
                        del self.app_state.data_store_cache_keys_expiration[command]
                self.app_state.data_store_key_command_mapping[key] = [
                    cmd for cmd in commands if cmd not in self.app_state.data_store_cache
                ]
                if not self.app_state.data_store_key_command_mapping[key]:
                    del self.app_state.data_store_key_command_mapping[key]

    async def cleanup_expired_entries(self):
        current_time = time.time()
        expired_keys = [key for key, exp in self.app_state.data_store_cache_keys_expiration.items() if exp <= current_time]
        for key in expired_keys:
            del self.app_state.data_store_cache[key]
            del self.app_state.data_store_cache_keys_expiration[key]
            # Remove command from self.app_state.data_store_key_command_mapping
            for query_key in self.app_state.data_store_key_command_mapping:
                if key in self.app_state.data_store_key_command_mapping[query_key]:
                    self.app_state.data_store_key_command_mapping[query_key].remove(key)
                    if not self.app_state.data_store_key_command_mapping[query_key]:  # Clean up empty lists
                        del self.app_state.data_store_key_command_mapping[query_key]

    def get_cache(self, command):
        if command in self.app_state.data_store_cache:
            # Return cached result if not expired
            if self.app_state.data_store_cache_keys_expiration[command] > time.time():
                self.app_state.data_store_cache[command]["last_accessed"] = time.time()
                print(f"Command {command} found in cache")
                return self.app_state.data_store_cache[command]["result"]
        print(f"Command {command} not found in cache")
        return None

    def flush_cache(self, *args, **kwargs):
        """Flush the entire cache."""
        self.app_state.data_store_cache.clear()
        self.app_state.data_store_cache_keys_expiration.clear()
        self.app_state.data_store_key_command_mapping.clear()
        return "OK"