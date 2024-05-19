import json
import os
from .app_state import AppState  # Importing AppState class from app_state module
from .constants import INDICES_FILE  # Importing INDICES_FILE constant from constants module
from .replication_manager import ReplicationManager
replication_manager = ReplicationManager()

class IndicesManager:
    def __init__(self):
        self.app_state = AppState()
        self.indice_file = INDICES_FILE

    # Function to get all local indices
    def get_all_local_indices(self):
        return self.app_state.indices.copy()  # Returns a copy of the indices stored in the AppState object

    # Function to load indices from a file
    def load_indices(self):
        # If the indices file doesn't exist, create it and write an empty dictionary to it
        if not os.path.exists(self.indice_file):
            with open(self.indice_file, mode='w', encoding='utf-8') as file:
                json.dump({}, file)

        # Open the indices file for reading
        with open(self.indice_file, mode='r', encoding='utf-8') as file:
            try:
                # Try to load JSON data from the file
                loaded_indices = json.load(file)
                # Update the indices in the AppState object with the loaded indices
                self.app_state.indices.update(self.deserialize_indices(loaded_indices))
            except json.JSONDecodeError:
                # If there's a JSON decoding error, update indices with an empty dictionary
                self.app_state.indices.update({})

    # Function to save indices to a file
    def save_indices(self):
        try:
            if self.app_state.indices_has_changed:
                # Perform cleanup of expired keys and get cleaned indices
                # Serialize the indices data
                serializable_indices = self.serialize_indices(self.app_state.indices)
                # Open the indices file for writing and dump the serialized data to it
                with open(self.indice_file, mode='w', encoding='utf-8') as file:
                    json.dump(serializable_indices, file, indent=4)
                    self.app_state.indices_has_changed = False
        except IOError as e:
            # Log the error
            print(f"Failed to save data: {e}")

    # Function to serialize indices data
    def serialize_indices(self, data):
        if isinstance(data, set):  # If data is a set
            return list(data)  # Convert it to a list
        elif isinstance(data, dict):  # If data is a dictionary
            # Serialize each key-value pair recursively
            return {key: self.serialize_indices(value) for key, value in data.items()}
        elif isinstance(data, list):  # If data is a list
            # Serialize each item in the list recursively
            return [self.serialize_indices(item) for item in data]
        return data  # Return data as is if it's not a set, dictionary, or list

    # Function to deserialize indices data
    def deserialize_indices(self, data):
        if isinstance(data, list) and all(isinstance(x, (int, float)) for x in data):
            # If data is a list of integers or floats, convert it to a set
            return set(data)
        elif isinstance(data, dict):  # If data is a dictionary
            # Deserialize each key-value pair recursively
            return {key: self.deserialize_indices(value) for key, value in data.items()}
        return data  # Return data as is if it's not a list or dictionary

    # Function to handle index-related commands
    async def indice_command(self, args):
        parts = args.split(maxsplit=1)  # Split the arguments into parts, maximum split is 1
        sub_command = parts[0]  # Get the sub-command
        if len(parts) > 1:
            sub_args = parts[1]  # Get the arguments for the sub-command
        else:
            if sub_command == 'LIST':
                return self.indices_list()  # If sub-command is LIST, call indices_list function
            else:
                return "ERROR: Missing arguments for INDEX command"  # Return error message for missing arguments

        # Check the sub-command and execute corresponding function
        if sub_command == 'LIST':
            return "ERROR: LIST sub-command does not require additional arguments"
        elif sub_command == 'GET':
            return self.indices_get(sub_args)
        elif sub_command == 'CREATE':
            return await self.indices_create(sub_args)
        elif sub_command == 'DEL':
            return await self.indices_del(sub_args)
        elif sub_command == 'FLUSH':
            return await self.indices_flush(sub_args)
        else:
            return "ERROR: Invalid INDEX operation"  # Return error message for invalid sub-command

    # Function to list all indices, only returning keys
    def indices_list(self):
        indices = self.app_state.indices

        # Recursive function to list indices without exposing content
        def recursive_structure(current_indices):
            result = {}
            for key, value in current_indices.items():
                if isinstance(value, dict) and 'type' in value and 'values' in value:
                    # Include only type and list of keys (excluding their values)
                    result[key] = {"type": value['type'], "keys": []}
                elif isinstance(value, dict):  # Further nested dictionaries
                    result[key] = recursive_structure(value)  # Recursively explore nested dictionaries

            return result

        # Create the index structure using the recursive function
        index_structure = recursive_structure(indices)
        if index_structure:
            return json.dumps(index_structure, indent=4)  # Return the index structure in JSON format with indentation
        else:
            return json.dumps({"message": "No indices defined."})  # Return a message if no indices are defined

    # Function to get details about specific indices
    def indices_get(self, args):
        indices = self.app_state.indices
        if args.strip().upper() == "ALL":
            return json.dumps(self.indices_get_all(), indent=4)  # Return details of all indices in JSON format

        if not args:
            return json.dumps({"error": "No index path provided"})  # Return an error if no index path is provided

        # Split the index path by colon to handle nested keys
        keys = args.split(':')
        data = indices

        # Navigate through the indices to reach the desired key
        for key in keys:
            if key in data:
                data = data[key]
            else:
                return json.dumps({"error": f"Index '{args}' not found"})  # Return an error if the index is not found

        # Check the type of the final data node
        if 'type' in data and data['type'] == 'set':
            return json.dumps(list(data['values'].keys()), indent=4)  # Return list of set members
        elif 'type' in data and data['type'] == 'string':
            return json.dumps(data['values'], indent=4)  # Return string values
        else:
            return json.dumps({"error": "Unsupported index type"})  # Return an error for unsupported types

        # Check and return details about the index
        if 'type' in data and 'values' in data:
            if data['type'] == 'set':
                # For 'set' type, include previews of set values
                set_data = {k: data['values'][k][:10] for k in list(data['values'].keys())[:10]}
                return json.dumps({"type": data['type'], "data": set_data}, indent=4)
            elif data['type'] == 'string':
                # For 'string' type, list the keys as before
                string_data = list(data['values'].keys())[:10]
                return json.dumps({"type": data['type'], "data": string_data}, indent=4)
        else:
            # If not a direct index, return a summary of sub-indices
            if isinstance(data, dict):
                return json.dumps({key: list(value.get('values', {}).keys())[:10] for key, value in data.items() if isinstance(value, dict)}, indent=4)
            else:
                return json.dumps({"error": f"Index data for '{args}' is not properly configured"})

    # Function to get details of all indices
    def indices_get_all(self):
        indices = self.app_state.indices
        result = self.recursive_list(indices)
        return result if result else {"message": "No indices defined."}

    # Recursive function to list indices
    def recursive_list(self, current_indices, prefix=""):
        result = {}
        for key, value in current_indices.items():
            if isinstance(value, dict) and 'type' in value and 'values' in value:
                if value['type'] == 'set':
                    set_data = {k: value['values'][k][:10] for k in list(value['values'].keys())[:10]}
                    result[prefix + key] = {"type": value['type'], "data": set_data}
                elif value['type'] == 'string':
                    string_data = list(value['values'].keys())[:10]
                    result[prefix + key] = {"type": value['type'], "data": string_data}
            elif isinstance(value, dict):
                result.update(self.recursive_list(value, prefix=prefix + key + ":"))
        return result

    def make_hashable(self, value):
        # If value is a list, convert it to a string by joining with a unique separator
        if isinstance(value, list):
            return ','.join(map(str, sorted(self.make_hashable(v) for v in value)))
        # If value is a dictionary, convert it to a tuple of sorted key-value pairs
        elif isinstance(value, dict):
            return tuple(sorted((k, self.make_hashable(v)) for k, v in value.items()))
        return value  # Return the value as is if it's already hashable

    def get_nested_index_info(self, index_dict, index_parts):
        current_dict = index_dict
        for part in index_parts:
            current_dict = current_dict.get(part, None)
            if current_dict is None:
                return None
        return current_dict

    async def update_index_on_add(self, parts, last_key, value, entity_key):
        from .scheduler import SchedulerManager
        scheduler_manager = SchedulerManager()

        index_parts = self.construct_index_parts(parts, AppState().indices)
        index_info = self.get_nested_index_info(AppState().indices, index_parts)

        if not index_info or 'type' not in index_info:
            return

        index_type = index_info['type']
        values_dict = index_info.setdefault('values', {})

        if index_type == 'set':
            # Ensure value is treated as iterable if it's a list, otherwise wrap it in a list for uniform processing
            processed_values = value if isinstance(value, list) else [value]
            for processed_value in processed_values:
                item = str(processed_value)  # Convert each item to string
                set_for_item = values_dict.setdefault(item, set())
                if isinstance(set_for_item, list):  # Convert to set if it's accidentally a list
                    set_for_item = set(set_for_item)
                set_for_item.add(entity_key)
                values_dict[item] = set_for_item  # Ensure the updated set is saved back to the dictionary
        elif index_type == 'string':
            values_dict[str(value)] = entity_key

        if not scheduler_manager.is_scheduler_active():
            self.save_indices()
        else:
            self.app_state.indices_has_changed = True

    async def update_index_on_remove(self, parts, last_key, old_value, entity_key):
        from .scheduler import SchedulerManager
        scheduler_manager = SchedulerManager()

        index_parts = self.construct_index_parts(parts, AppState().indices)
        index_info = self.get_nested_index_info(AppState().indices, index_parts)

        if not index_info or 'type' not in index_info:
            return

        index_type = index_info['type']
        values_dict = index_info['values']

        if index_type == 'set':
            if isinstance(old_value, list):
                for item in old_value:
                    item = str(item)
                    if item in values_dict and entity_key in values_dict[item]:
                        values_dict[item].remove(entity_key)
                        if not values_dict[item]:
                            del values_dict[item]
            else:
                old_value = str(old_value)
                if old_value in values_dict and entity_key in values_dict[old_value]:
                    values_dict[old_value].remove(entity_key)
                    if not values_dict[old_value]:
                        del values_dict[old_value]
        elif index_type == 'string':
            for key, val in list(values_dict.items()):
                if val == entity_key:
                    del values_dict[key]

        if not scheduler_manager.is_scheduler_active():
            self.save_indices()
        else:
            self.app_state.indices_has_changed = True

    def construct_index_parts(self, parts, indices_structure):
        index_parts = []
        temp_dict = indices_structure

        for part in parts:
            if part in temp_dict:
                index_parts.append(part)
                temp_dict = temp_dict.get(part, {})
            else:
                if isinstance(temp_dict, dict):
                    # If we're still navigating the dictionary, continue without adding
                    continue
                else:
                    # If no further dictionary to navigate, break out
                    break

        return index_parts

    # Asynchronous function to remove a specific field from an index
    async def remove_field_from_index(self, keys, field, value_to_remove):
        from .scheduler import SchedulerManager
        scheduler_manager = SchedulerManager()

        if not keys:
            return "Error: No keys provided for index removal."

        is_nested = len(keys) > 2

        if is_nested:
            index_parts = keys + [field]
        else:
            index_parts = self.construct_index_parts(keys[:-1], AppState().indices) + [field]

        index_info = self.get_nested_index_info(AppState().indices, index_parts)

        if index_info:
            index_type = index_info.get('type', 'set')
            values_dict = index_info.get('values', {})

            if index_type == 'set':
                if value_to_remove in values_dict and ':'.join(keys) in values_dict[value_to_remove]:
                    values_dict[value_to_remove].discard(':'.join(keys))
                    if not values_dict[value_to_remove]:
                        del values_dict[value_to_remove]
                message = "Index entry for 'set' removed successfully."
            elif index_type == 'string':
                if value_to_remove in values_dict:
                    del values_dict[value_to_remove]
                message = "Index entry for 'string' removed successfully."
            else:
                message = "Field not indexed or no matching entry found."

            self.cleanup_empty_dicts(AppState().indices, index_parts)
            if not scheduler_manager.is_scheduler_active():
                self.save_indices()
            else:
                AppState().indices_has_changed = True
            return message
        else:
            return "Field not indexed or index part not found."

    async def remove_entity_from_index(self, keys, entity_data):
        from .scheduler import SchedulerManager
        scheduler_manager = SchedulerManager()

        if not keys:
            return "Error: No keys provided for entity index removal."

        main_key = keys[0]
        entity_key = f"{main_key}:{':'.join(keys[1:])}" if len(keys) > 1 else main_key
        indices = AppState().indices.get(main_key, {})
        message = "Entity index entries removed successfully."

        for field, field_info in indices.items():
            index_type = field_info.get('type', 'set')
            values_dict = field_info.get('values', {})

            if index_type == 'set':
                for value, entities in list(values_dict.items()):
                    # Check if entities is a set or list and remove the entity_key accordingly
                    if isinstance(entities, set):
                        entities.discard(entity_key)
                    elif isinstance(entities, list):
                        if entity_key in entities:
                            entities.remove(entity_key)

                    # Clean up if the collection is empty
                    if not entities:
                        del values_dict[value]

                self.cleanup_empty_dicts(indices, [main_key, field])
            elif index_type == 'string':
                for value, stored_entity in list(values_dict.items()):
                    if stored_entity == entity_key:
                        del values_dict[value]
                self.cleanup_empty_dicts(indices, [main_key, field])

        if not scheduler_manager.is_scheduler_active():
            self.save_indices()
        else:
            AppState().indices_has_changed = True

        return message

    # Function to create a new index
    async def indices_create(self, args):
        from .scheduler import SchedulerManager
        scheduler_manager = SchedulerManager()

        parts = args.split()
        if len(parts) < 2:
            return "ERROR: Missing index name or type"

        indice_path, index_type = parts[0], parts[1]
        path_parts = indice_path.split(':')

        if index_type not in ['string', 'set']:
            return f"ERROR: Invalid index type '{index_type}' specified. Choose 'string' or 'set'."

        indices = self.app_state.indices
        current_level = indices
        for part in path_parts[:-1]:
            if part not in current_level:
                current_level[part] = {}
            current_level = current_level[part]

        if path_parts[-1] in current_level:
            return "ERROR: Index already exists"

        new_index = current_level[path_parts[-1]] = {
            'type': index_type,
            'values': {}
        }

        main_key = path_parts[0]
        nested_keys = path_parts[1:]  # Capture all parts of the path for nested access

        # Iterate over data store to populate the new index with existing data
        for key, item in self.app_state.data_store.get(main_key, {}).items():
            attribute_value = self.get_nested_value(item, nested_keys)
            if attribute_value is not None:
                if index_type == 'set':
                    if not isinstance(attribute_value, list):
                        attribute_value = [attribute_value]
                    for single_value in attribute_value:
                        single_value_str = str(single_value)
                        if single_value_str not in new_index['values']:
                            new_index['values'][single_value_str] = []
                        new_index['values'][single_value_str].append(f"{main_key}:{key}")
                elif index_type == 'string':
                    attribute_value_str = str(attribute_value)
                    new_index['values'][attribute_value_str] = f"{main_key}:{key}"

        if not scheduler_manager.is_scheduler_active():
            self.save_indices()
        else:
            self.app_state.indices_has_changed = True
        
        # Replication
        if await replication_manager.has_replication_is_replication_master():
            await replication_manager.send_command_to_slaves(f"INDICES CREATE {args}")

        return "OK"

    # Asynchronous function to update indices based on instruction
    async def indices_update(self, base_key, value, instruction):
        from .scheduler import SchedulerManager
        scheduler_manager = SchedulerManager()
        
        try:
            # Split the instruction into parts and trim extraneous spaces
            parts = [part.strip() for part in instruction.split(',')]
            field_and_keys = parts[0].split(':')
            field = field_and_keys[0].strip()
            subkeys = [key.strip() for key in field_and_keys[1:]]
            identifier = parts[1].strip()

            # Fetch the nested value for the unique identifier
            data_object = self.get_nested_value(self.app_state.data_store, base_key.split(':')[:-1])
            identifier_value = data_object.get(identifier)
            if identifier_value is None:
                print(f"Error: Identifier '{identifier}' not found in data for {base_key}")
                return "ERROR: Identifier not found"

            # Build the index path
            indices = self.app_state.indices
            current_level = indices.setdefault(field, {})
            for subkey in subkeys[:-1]:
                actual_key = str(value) if subkey == 'value' else data_object.get(subkey.strip('%'), '')
                current_level = current_level.setdefault(actual_key, {})
            
            final_key = subkeys[-1] if subkeys else 'value'
            actual_final_key = str(value) if final_key == 'value' else data_object.get(final_key.strip('%'), '')
            
            if actual_final_key not in current_level:
                current_level[actual_final_key] = set()
            current_level[actual_final_key].add(identifier_value)

            # Save the updated indices if the scheduler is not active
            if not scheduler_manager.is_scheduler_active():
                self.save_indices()
            else:
                self.app_state.indices_has_changed = True

            return "OK"
        except Exception as e:
            print(f"ERROR in update_indices: {e}")
            return f"ERROR: Failed to update indices with message {str(e)}"

    # Function to delete a value from an index
    async def indices_del(self, args):
        from .scheduler import SchedulerManager
        scheduler_manager = SchedulerManager()

        indices = self.app_state.indices
        parts = args.split()  # Split the arguments into parts
        if len(parts) < 2:
            return "ERROR: Missing arguments for INDEX DEL command"

        keys = parts[0].split(':')  # Split the keys
        value_to_delete = parts[1]  # Get the value to delete

        current_dict = indices
        path = []
        for key in keys[:-1]:
            if key in current_dict:
                path.append((current_dict, key))
                current_dict = current_dict[key]
            else:
                return f"ERROR: Key {'/'.join(keys[:-1])} not found"

        final_key = keys[-1]
        # Check if value exists in the index and delete it if found
        if final_key in current_dict and value_to_delete in current_dict[final_key]["values"]:
            del current_dict[final_key]["values"][value_to_delete]  # Delete the value from the dictionary
            # Check and clean up empty dictionary
            if not current_dict[final_key]["values"]:
                del current_dict[final_key]
                while path:
                    parent_dict, key = path.pop()
                    if not parent_dict[key]:
                        del parent_dict[key]
                    else:
                        break
        else:
            return f"ERROR: Value {value_to_delete} not found under index {':'.join(keys)}"

        if not scheduler_manager.is_scheduler_active():
            self.save_indices()  # Save the updated indices to file
        else:
            # Track changes
            self.app_state.indices_has_changed = True

            # Replication
            if await replication_manager.has_replication_is_replication_master():
                await replication_manager.send_command_to_slaves(f"INDICES CREATE {args}")

        return "OK"  # Return success message

    # Function to flush an index or a sub-key from an index
    async def indices_flush(self, args):
        from .scheduler import SchedulerManager  # Importing data management functions from scheduler_manager module
        scheduler_manager = SchedulerManager()

        indices = self.app_state.indices
        parts = args.split(':')  # Split the arguments by colon
        if args in indices:
            # If the argument is the name of an index, delete the entire index
            del indices[args]
            if not scheduler_manager.is_scheduler_active():
                self.save_indices() # Save the updated indices to file
            else:
                # Track changes
                self.app_state.indices_has_changed = True
            return "OK"  # Return success message
        elif len(parts) > 1:
            # If there are sub-keys specified, delete the specified sub-key
            indice_name = parts[0]
            sub_keys = parts[1:]
            if indice_name in indices:
                current = indices[indice_name]
                for key in sub_keys:
                    if key in current:
                        del current[key]
                        # If the current dictionary becomes empty, delete the index
                        if not current:
                            del indices[indice_name]
                        if not scheduler_manager.is_scheduler_active():
                            self.save_indices() # Save the updated indices to file
                        else:
                            # Track changes
                            self.app_state.indices_has_changed = True

                        # Replication
                        if await replication_manager.has_replication_is_replication_master():
                            await replication_manager.send_command_to_slaves(f"INDICES FLUSH {args}")

                        return "OK"  # Return success message
                    else:
                        return f"ERROR: Sub-key '{key}' not found in '{indice_name}'"
                return "OK"  # Return success message
            else:
                return f"ERROR: Index '{indice_name}' not found"
        else:
            return f"ERROR: Index or Sub-key '{args}' not found"

    # Function to recursively clean up empty dictionaries in the index structure
    def cleanup_empty_dicts(self, data, path, value_to_remove=None):
        """
        Recursively deletes empty dictionaries or specific values from a nested data structure.

        Args:
            data (dict): The root dictionary from which to start cleanup.
            path (list): A list of keys that form the path to the deepest dictionary to check.
            value_to_remove (str): The specific value to delete from the dictionary at the path, if applicable.
        """
        # Navigate to the deepest dictionary first
        current_level = data
        for part in path[:-1]:  # Stop before the last part because we need to check it separately
            if part in current_level:
                current_level = current_level[part]
            else:
                # If any part of the path does not exist, exit the function
                return

        # Remove the specific value if provided and applicable
        deepest_dict = current_level.get(path[-1], {})
        if value_to_remove and value_to_remove in deepest_dict:
            del deepest_dict[value_to_remove]

        # Now check and clean up from the deepest part back to the root
        for i in range(len(path) - 1, -1, -1):
            parent_level = data
            for part in path[:i]:
                parent_level = parent_level.get(part)

            current_dict = parent_level.get(path[i], {})
            if isinstance(current_dict, dict) and not current_dict:
                del parent_level[path[i]]  # Delete empty dictionary
            elif current_dict:
                break  # Stop cleaning up as we found a non-empty dictionary

    def get_nested_value(self, data, keys):
        for key in keys:
            if isinstance(data, dict) and key in data:
                data = data[key]
            elif isinstance(data, list):
                try:
                    index = int(key)
                    if 0 <= index < len(data):
                        data = data[index]
                    else:
                        return None
                except ValueError:
                    return None
            else:
                return None  # Return None if any key is not found in the path
        return data