import ujson  # Module for JSON operations
import os  # Module for interacting with the operating system
from .app_state import AppState  # Importing AppState class from app_state module
from .constants import INDICES_FILE  # Importing INDICES_FILE constant from constants module
from .replication_manager import ReplicationManager
replication_manager = ReplicationManager()

class IndicesManager:
    def __init__(self):
        """Initialize IndicesManager with application state and indices file path."""
        self.app_state = AppState()
        self.indice_file = INDICES_FILE

    def get_all_local_indices(self):
        """Return a copy of all local indices stored in the application state."""
        return self.app_state.indices.copy()

    def load_indices(self):
        """
        Load indices from the indices file into the application state.

        If the indices file does not exist, create it and write an empty dictionary to it.
        Attempt to load the indices from the file and update the application state.
        Handle JSON decode errors by updating indices with an empty dictionary.
        """
        if not os.path.exists(self.indice_file):
            with open(self.indice_file, mode='w', encoding='utf-8') as file:
                ujson.dump({}, file)

        with open(self.indice_file, mode='r', encoding='utf-8') as file:
            try:
                loaded_indices = ujson.load(file)
                self.app_state.indices.update(self.deserialize_indices(loaded_indices))
            except ujson.JSONDecodeError:
                self.app_state.indices.update({})

    def save_indices(self):
        """
        Save the current indices in the application state to the indices file.

        If there have been changes to the indices, write the updated indices to the file
        and reset the indices change flag. Handle I/O errors.
        """
        try:
            if self.app_state.indices_has_changed:
                serializable_indices = self.serialize_indices(self.app_state.indices)
                with open(self.indice_file, mode='w', encoding='utf-8') as file:
                    ujson.dump(serializable_indices, file, indent=4)
                    self.app_state.indices_has_changed = False
        except IOError as e:
            print(f"Failed to save indices: {e}")

    def serialize_indices(self, data):
        """
        Serialize indices data to ensure it is JSON-compatible.

        Args:
            data: The indices data to serialize.

        Returns:
            The serialized data.
        """
        if isinstance(data, set):
            return list(data)
        elif isinstance(data, dict):
            return {key: self.serialize_indices(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.serialize_indices(item) for item in data]
        return data

    def deserialize_indices(self, data):
        """
        Deserialize indices data to restore its original format.

        Args:
            data: The indices data to deserialize.

        Returns:
            The deserialized data.
        """
        if isinstance(data, list) and all(isinstance(x, (int, float)) for x in data):
            return set(data)
        elif isinstance(data, dict):
            return {key: self.deserialize_indices(value) for key, value in data.items()}
        return data

    async def indice_command(self, args):
        """
        Handle index-related commands.

        Args:
            args: The arguments for the index command.

        Returns:
            The result of the index command.
        """
        parts = args.split(maxsplit=1)
        sub_command = parts[0]
        if len(parts) > 1:
            sub_args = parts[1]
        else:
            if sub_command == 'LIST':
                return self.indices_list()
            else:
                return "ERROR: Missing arguments for INDEX command"

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
            return "ERROR: Invalid INDEX operation"

    def indices_list(self):
        """
        List all indices, returning only keys.

        Returns:
            The index structure in JSON format.
        """
        indices = self.app_state.indices

        def recursive_structure(current_indices):
            result = {}
            for key, value in current_indices.items():
                if isinstance(value, dict) and 'type' in value and 'values' in value:
                    result[key] = {"type": value['type'], "keys": []}
                elif isinstance(value, dict):
                    result[key] = recursive_structure(value)
            return result

        index_structure = recursive_structure(indices)
        if index_structure:
            return ujson.dumps(index_structure, indent=4)
        else:
            return ujson.dumps({"message": "No indices defined."})

    def indices_get(self, args):
        """
        Get details about specific indices.

        Args:
            args: The index path.

        Returns:
            The details of the specified indices in JSON format.
        """
        indices = self.app_state.indices
        if args.strip().upper() == "ALL":
            return ujson.dumps(self.indices_get_all(), indent=4)

        if not args:
            return ujson.dumps({"error": "No index path provided"})

        keys = args.split(':')
        data = indices

        for key in keys:
            if key in data:
                data = data[key]
            else:
                return ujson.dumps({"error": f"Index '{args}' not found"})

        if 'type' in data and data['type'] == 'set':
            return ujson.dumps(list(data['values'].keys()), indent=4)
        elif 'type' in data and data['type'] == 'string':
            return ujson.dumps(data['values'], indent=4)
        else:
            return ujson.dumps({"error": "Unsupported index type"})

    def indices_get_all(self):
        """
        Get details of all indices.

        Returns:
            The details of all indices.
        """
        indices = self.app_state.indices
        result = self.recursive_list(indices)
        return result if result else {"message": "No indices defined."}

    def recursive_list(self, current_indices, prefix=""):
        """
        Recursive function to list indices.

        Args:
            current_indices: The current level of indices.
            prefix: The prefix for the current level of indices.

        Returns:
            The structured list of indices.
        """
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
        """
        Make a value hashable for use in sets and dictionaries.

        Args:
            value: The value to make hashable.

        Returns:
            The hashable value.
        """
        if isinstance(value, list):
            return ','.join(map(str, sorted(self.make_hashable(v) for v in value)))
        elif isinstance(value, dict):
            return tuple(sorted((k, self.make_hashable(v)) for k, v in value.items()))
        return value

    def get_nested_index_info(self, index_dict, index_parts):
        """
        Get nested index information.

        Args:
            index_dict: The index dictionary.
            index_parts: The parts of the index path.

        Returns:
            The nested index information.
        """
        current_dict = index_dict
        for part in index_parts:
            current_dict = current_dict.get(part, None)
            if current_dict is None:
                return None
        return current_dict

    async def update_index_on_add(self, parts, last_key, value, entity_key):
        """
        Update an index when a value is added.

        Args:
            parts: The parts of the key.
            last_key: The last key.
            value: The value to add.
            entity_key: The entity key.
        """
        from .scheduler import SchedulerManager
        scheduler_manager = SchedulerManager()

        index_parts = self.construct_index_parts(parts, AppState().indices)
        index_info = self.get_nested_index_info(AppState().indices, index_parts)

        if not index_info or 'type' not in index_info:
            return

        index_type = index_info['type']
        values_dict = index_info.setdefault('values', {})

        if index_type == 'set':
            processed_values = value if isinstance(value, list) else [value]
            for processed_value in processed_values:
                item = str(processed_value)
                set_for_item = values_dict.setdefault(item, set())
                if isinstance(set_for_item, list):
                    set_for_item = set(set_for_item)
                set_for_item.add(entity_key)
                values_dict[item] = set_for_item
        elif index_type == 'string':
            values_dict[str(value)] = entity_key

        if not scheduler_manager.is_scheduler_active():
            self.save_indices()
        else:
            self.app_state.indices_has_changed = True

    async def update_index_on_remove(self, parts, last_key, old_value, entity_key):
        """
        Update an index when a value is removed.

        Args:
            parts: The parts of the key.
            last_key: The last key.
            old_value: The value to remove.
            entity_key: The entity key.
        """
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
        """
        Construct index parts from the key parts.

        Args:
            parts: The parts of the key.
            indices_structure: The indices structure.

        Returns:
            The constructed index parts.
        """
        index_parts = []
        temp_dict = indices_structure

        for part in parts:
            if part in temp_dict:
                index_parts.append(part)
                temp_dict = temp_dict.get(part, {})
            else:
                if isinstance(temp_dict, dict):
                    continue
                else:
                    break

        return index_parts

    async def remove_field_from_index(self, keys, field, value_to_remove):
        """
        Remove a specific field from an index.

        Args:
            keys: The keys of the index.
            field: The field to remove.
            value_to_remove: The value to remove.

        Returns:
            The result of the removal.
        """
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
        """
        Remove an entity from an index.

        Args:
            keys: The keys of the entity.
            entity_data: The data of the entity.

        Returns:
            The result of the removal.
        """
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
                    if isinstance(entities, set):
                        entities.discard(entity_key)
                    elif isinstance(entities, list):
                        if entity_key in entities:
                            entities.remove(entity_key)

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

    async def indices_create(self, args):
        """
        Create a new index.

        Args:
            args: The arguments for creating the index.

        Returns:
            The result of the creation.
        """
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
        nested_keys = path_parts[1:]

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

        if await replication_manager.has_replication_is_replication_master():
            await replication_manager.send_command_to_slaves(f"INDICES CREATE {args}")

        return "OK"

    async def indices_update(self, base_key, value, instruction):
        """
        Update indices based on instruction.

        Args:
            base_key: The base key.
            value: The value to update.
            instruction: The update instruction.

        Returns:
            The result of the update.
        """
        from .scheduler import SchedulerManager
        scheduler_manager = SchedulerManager()

        try:
            parts = [part.strip() for part in instruction.split(',')]
            field_and_keys = parts[0].split(':')
            field = field_and_keys[0].strip()
            subkeys = [key.strip() for key in field_and_keys[1:]]
            identifier = parts[1].strip()

            data_object = self.get_nested_value(self.app_state.data_store, base_key.split(':')[:-1])
            identifier_value = data_object.get(identifier)
            if identifier_value is None:
                print(f"Error: Identifier '{identifier}' not found in data for {base_key}")
                return "ERROR: Identifier not found"

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

            if not scheduler_manager.is_scheduler_active():
                self.save_indices()
            else:
                self.app_state.indices_has_changed = True

            return "OK"
        except Exception as e:
            print(f"ERROR in indices_update: {e}")
            return f"ERROR: Failed to update indices with message {str(e)}"

    async def indices_del(self, args):
        """
        Delete a value from an index.

        Args:
            args: The arguments for the deletion.

        Returns:
            The result of the deletion.
        """
        from .scheduler import SchedulerManager
        scheduler_manager = SchedulerManager()

        indices = self.app_state.indices
        parts = args.split()
        if len(parts) < 2:
            return "ERROR: Missing arguments for INDEX DEL command"

        keys = parts[0].split(':')
        value_to_delete = parts[1]

        current_dict = indices
        path = []
        for key in keys[:-1]:
            if key in current_dict:
                path.append((current_dict, key))
                current_dict = current_dict[key]
            else:
                return f"ERROR: Key {'/'.join(keys[:-1])} not found"

        final_key = keys[-1]
        if final_key in current_dict and value_to_delete in current_dict[final_key]["values"]:
            del current_dict[final_key]["values"][value_to_delete]
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
            self.save_indices()
        else:
            self.app_state.indices_has_changed = True

            if await replication_manager.has_replication_is_replication_master():
                await replication_manager.send_command_to_slaves(f"INDICES DEL {args}")

        return "OK"

    async def indices_flush(self, args):
        """
        Flush an index or a sub-key from an index.

        Args:
            args: The arguments for the flush.

        Returns:
            The result of the flush.
        """
        from .scheduler import SchedulerManager
        scheduler_manager = SchedulerManager()

        indices = self.app_state.indices
        parts = args.split(':')
        if args in indices:
            del indices[args]
            if not scheduler_manager.is_scheduler_active():
                self.save_indices()
            else:
                self.app_state.indices_has_changed = True
            return "OK"
        elif len(parts) > 1:
            indice_name = parts[0]
            sub_keys = parts[1:]
            if indice_name in indices:
                current = indices[indice_name]
                for key in sub_keys:
                    if key in current:
                        del current[key]
                        if not current:
                            del indices[indice_name]
                        if not scheduler_manager.is_scheduler_active():
                            self.save_indices()
                        else:
                            self.app_state.indices_has_changed = True

                        if await replication_manager.has_replication_is_replication_master():
                            await replication_manager.send_command_to_slaves(f"INDICES FLUSH {args}")

                        return "OK"
                    else:
                        return f"ERROR: Sub-key '{key}' not found in '{indice_name}'"
                return "OK"
            else:
                return f"ERROR: Index '{indice_name}' not found"
        else:
            return f"ERROR: Index or Sub-key '{args}' not found"

    def cleanup_empty_dicts(self, data, path, value_to_remove=None):
        """
        Recursively clean up empty dictionaries in the index structure.

        Args:
            data (dict): The root dictionary from which to start cleanup.
            path (list): A list of keys that form the path to the deepest dictionary to check.
            value_to_remove (str): The specific value to delete from the dictionary at the path, if applicable.
        """
        current_level = data
        for part in path[:-1]:
            if part in current_level:
                current_level = current_level[part]
            else:
                return

        deepest_dict = current_level.get(path[-1], {})
        if value_to_remove and value_to_remove in deepest_dict:
            del deepest_dict[value_to_remove]

        for i in range(len(path) - 1, -1, -1):
            parent_level = data
            for part in path[:i]:
                parent_level = parent_level.get(part)

            current_dict = parent_level.get(path[i], {})
            if isinstance(current_dict, dict) and not current_dict:
                del parent_level[path[i]]
            elif current_dict:
                break

    def get_nested_value(self, data, keys):
        """
        Get the value of a nested key from the data store.

        Args:
            data (dict): The data store.
            keys (list): The parts of the key.

        Returns:
            The value of the nested key, or None if the key does not exist.
        """
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
                return None
        return data