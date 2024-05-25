import hashlib
import ujson
from .app_state import AppState
from .connection_handler import asyncio, websockets
from .data_manager import DataManager
from .indices_manager import IndicesManager

class ShardingManager:
    def __init__(self):
        """Initialize the ShardingManager with necessary managers and state."""
        self.app_state = AppState()
        self.data_manager = DataManager()
        self.indices_manager = IndicesManager()

    async def has_sharding(self):
        """Check if sharding is enabled in the configuration."""
        return self.app_state.config_store.get('SHARDING') == '1'

    async def is_sharding_master(self):
        """Check if the current node is a sharding master."""
        return self.app_state.config_store.get('SHARDING_TYPE') == 'MASTER'

    async def has_sharding_is_sharding_master(self):
        """Check if sharding is enabled and the current node is a sharding master."""
        sharding_type = self.app_state.config_store.get('SHARDING_TYPE')
        sharding = self.app_state.config_store.get('SHARDING')
        return sharding_type == 'MASTER' and sharding == '1'

    async def check_sharding(self, command, command_line, key):
        """
        Check the sharding status and route the command accordingly.
        
        Parameters:
            command (str): The command to execute.
            command_line (str): The full command line string.
            key (str): The key involved in the command.
        
        Returns:
            str: "LOCAL" if the command is to be executed locally, or the result from the shard.
        """
        try:
            sharding = self.app_state.config_store.get('SHARDING')
            sharding_type = self.app_state.config_store.get('SHARDING_TYPE')

            if sharding == "0":
                return "LOCAL"

            host = self.app_state.config_store.get('HOST')
            port = self.app_state.config_store.get('PORT')
            shard = self.get_shard(key)
            shard_uri = f'{shard}:{port}'

            if sharding_type == 'MASTER' and shard != host:
                result = await self.send_to_shard(f'{command} {command_line}', shard_uri)
                return result if result else "ERROR"
            else:
                return "LOCAL"
        except Exception as e:
            return f"ERROR: {str(e)}"

    def get_shard(self, key):
        """
        Determine the shard for a given key.
        
        Parameters:
            key (str): The key to determine the shard for.
        
        Returns:
            str: The shard responsible for the given key.
        """
        try:
            shards = self.app_state.config_store.get('SHARDS')
            total_shards = len(shards)
            hash_object = hashlib.sha256(key.encode())
            hash_digest = hash_object.hexdigest()
            hash_int = int(hash_digest, 16)
            shard_number = hash_int % total_shards
            return shards[shard_number]
        except Exception as e:
            print(f"Error in get_shard: {e}")
            raise

    async def send_to_shard(self, command, shard_uri):
        """
        Send a command to a specific shard.
        
        Parameters:
            command (str): The command to send.
            shard_uri (str): The URI of the shard.
        
        Returns:
            str: The response from the shard or an error message.
        """
        uri = f"ws://{shard_uri}"
        try:
            async with websockets.connect(uri) as websocket:
                await websocket.send(ujson.dumps(self.app_state.auth_data))
                auth_response = await websocket.recv()
                if 'Welcome!' in auth_response:
                    await websocket.send(command)
                    response = await websocket.recv()
                    return response
                else:
                    return "Authentication failed."
        except Exception as e:
            print(f"Shard error: {e}")
            return False

    async def broadcast_query(self, command, shard_uris, expected_types=None):
        """
        Broadcast a query to multiple shards and aggregate the results.
        
        Parameters:
            command (str): The query command to broadcast.
            shard_uris (list): The list of shard URIs to send the query to.
            expected_types (dict): The expected data types for the response data.
        
        Returns:
            list: The aggregated results from all shards.
        """
        async def query_shard(uri):
            try:
                async with websockets.connect(f'ws://{uri}') as websocket:
                    await websocket.send(ujson.dumps(self.app_state.auth_data))
                    auth_response = await websocket.recv()
                    if 'Welcome!' in auth_response:
                        await websocket.send(command)
                        response = await websocket.recv()
                        data = ujson.loads(response)
                        if expected_types:
                            data = self.correct_data_types(data, expected_types)
                        return data
                    else:
                        return "Authentication failed."
            except Exception as e:
                return None

        tasks = [query_shard(uri) for uri in shard_uris]
        results = await asyncio.gather(*tasks)
        aggregated_results = []
        for result in results:
            if isinstance(result, list):
                aggregated_results.extend(result)
            elif isinstance(result, dict):
                aggregated_results.append(result)
        return aggregated_results

    @staticmethod
    def correct_data_types(data, expected_types):
        """
        Correct the data types of the entries based on expected types.
        
        Parameters:
            data (list): The list of data entries to correct.
            expected_types (dict): The expected data types for each key.
        
        Returns:
            list: The corrected data entries.
        """
        for entry in data:
            for key, expected_type in expected_types.items():
                if key in entry and not isinstance(entry[key], expected_type):
                    try:
                        entry[key] = ujson.loads(entry[key]) if isinstance(entry[key], str) else expected_type(entry[key])
                    except (ValueError, TypeError):
                        print(f"Warning: Failed to convert {key} to {expected_type}")
        return data

    async def broadcast_query_with_response_tracking(self, command, shard_uris):
        """
        Broadcast a query to multiple shards and track responsive shards.
        
        Parameters:
            command (str): The query command to broadcast.
            shard_uris (list): The list of shard URIs to send the query to.
        
        Returns:
            tuple: The list of results and the list of responsive shards.
        """
        results = []
        responsive_shards = []
        for uri in shard_uris:
            try:
                async with websockets.connect(f'ws://{uri}') as websocket:
                    await websocket.send(ujson.dumps(self.app_state.auth_data))
                    auth_response = await websocket.recv()
                    if 'Welcome!' in auth_response:
                        await websocket.send(command)
                        response = await websocket.recv()
                        response_data = ujson.loads(response)
                        data = response_data.get('local_data', {})
                        indices = response_data.get('local_indices', {})
                        results.append((data, indices))
                        responsive_shards.append(uri.split(':')[0])
                    else:
                        results.append(({}, {}))
            except Exception as e:
                results.append(({}, {}))
        return results, responsive_shards

    async def reshard(self):
        """
        Reshard the data and indices across all shards.
        
        Returns:
            str: The result of the resharding process.
        """
        try:
            host = self.app_state.config_store.get('HOST')
            port = self.app_state.config_store.get('PORT')
            shards = self.app_state.config_store.get('SHARDS')
            shard_uris = [f"{shard}:{port}" for shard in shards if shard != host]
            all_data = []
            all_indices = []

            if shard_uris:
                remote_data, responsive_shards = await self.broadcast_query_with_response_tracking('RESHARD', shard_uris)
                for data, indices in remote_data:
                    all_data.append(data)
                    all_indices.append(indices)

            local_data, local_indices = self.data_manager.get_all_local_data(), self.indices_manager.get_all_local_indices()
            all_data.append(local_data)
            all_indices.append(local_indices)

            merged_data = self.merge_data(all_data)
            merged_indices = self.merge_data(all_indices)

            self.app_state.data_store.clear()
            self.app_state.indices.clear()

            self.app_state.data_has_changed = True
            self.app_state.indices_has_changed = True
            self.data_manager.save_data()
            self.indices_manager.save_indices()

            if not shard_uris or (len(shards) > 1 and len(responsive_shards) == len(shard_uris)):
                await self.redistribute_data(merged_data)
                await self.redistribute_indices(merged_indices, shards)
            else:
                raise Exception("Not all shards responded, cannot complete resharding.")

            return "Resharding completed successfully."
        except Exception as error:
            rollback_result = await self.rollback_all_shards(responsive_shards)
            return f"Resharding failed {error}, rolled back. {rollback_result}"

    async def redistribute_indices(self, indices, shards):
        """
        Redistribute indices to the appropriate shards.
        
        Parameters:
            indices (dict): The indices to redistribute.
            shards (list): The list of all shards.
        
        Returns:
            str: The result of the redistribution process.
        """
        from .command_processing import CommandProcessor
        command_processor = CommandProcessor()

        async def create_index_command(path, index_info):
            index_type = index_info['type']
            command = f"INDICES CREATE {path} {index_type}"
            for shard_uri in shard_uris:
                if shard_uri == f"{host}:{port}":
                    await command_processor.process_command(command)
                else:
                    await self.send_to_shard(command, shard_uri)

        async def process_indices(indices, current_path=""):
            for key, value in indices.items():
                full_path = f"{current_path}:{key}" if current_path else key
                if 'type' in value:
                    await create_index_command(full_path, value)
                else:
                    await process_indices(value, full_path)

        try:
            host = self.app_state.config_store.get('HOST')
            port = self.app_state.config_store.get('PORT')
            sharding = self.app_state.config_store.get('SHARDING')
            shard_uris = [f"{shard}:{port}" for shard in shards]

            if sharding == '0':
                self.app_state.indices = indices
                self.app_state.indices_has_changed = True
                self.indices_manager.save_indices()
            else:
                await process_indices(indices)
        except Exception as error:
            print(f"Redistribution of indices failed: {error}")
            return "Redistribution of indices failed"

    async def redistribute_data(self, data):
        """
        Redistribute data to the appropriate shards.
        
        Parameters:
            data (dict): The data to redistribute.
        
        Returns:
            str: The result of the redistribution process.
        """
        try:
            host = self.app_state.config_store.get('HOST')
            port = self.app_state.config_store.get('PORT')
            sharding = self.app_state.config_store.get('SHARDING')
            batch_size = int(self.app_state.config_store.get('SHARDING_BATCH_SIZE'))

            if sharding == '0':
                self.app_state.data_store = data
                self.app_state.data_has_changed = True
                self.data_manager.save_data()
            else:
                shard_commands = {}
                for key, value in data.items():
                    shard = None
                    if isinstance(value, dict):
                        for sub_key, sub_value in value.items():
                            combined_key = f"{key}:{sub_key}" if sub_key else key
                            shard = self.get_shard(combined_key)
                            self.process_data_item(key, sub_key, sub_value, shard, shard_commands)
                    else:
                        shard = self.get_shard(key)
                        self.process_data_item(key, None, value, shard, shard_commands)

                    if shard_commands.get(shard) and len(shard_commands[shard]) >= batch_size:
                        await self.process_batch(shard, shard_commands[shard], host, port)
                        shard_commands[shard] = []

                for shard, commands in shard_commands.items():
                    if commands:
                        await self.process_batch(shard, commands, host, port)
        except Exception as error:
            print(f"Redistribution failed: {error}")
            return "Redistribution failed"

    def process_data_item(self, key, sub_key, value, shard, shard_commands):
        """
        Process a data item for redistribution.
        
        Parameters:
            key (str): The key of the data item.
            sub_key (str): The subkey of the data item.
            value (Any): The value of the data item.
            shard (str): The shard responsible for the data item.
            shard_commands (dict): The dictionary to store shard commands.
        """
        combined_key = f"{key}:{sub_key}" if sub_key else key
        value_str = ujson.dumps(value) if isinstance(value, (dict, list, set)) else str(value)
        command = f"{combined_key} {value_str}"
        if shard not in shard_commands:
            shard_commands[shard] = [command]
        else:
            shard_commands[shard].append(command)

    async def process_batch(self, shard, commands, host, port):
        """
        Process a batch of commands and send to the appropriate shard.
        
        Parameters:
            shard (str): The shard to send the commands to.
            commands (list): The list of commands to send.
            host (str): The host address.
            port (str): The port number.
        """
        from .command_processing import CommandProcessor
        command_processor = CommandProcessor()
        batch_command = "SET " + '|'.join(commands)
        if shard == host:
            await command_processor.process_command(batch_command)
        else:
            shard_uri = f"{shard}:{port}"
            await self.send_to_shard(batch_command, shard_uri)

    async def rollback_all_shards(self, shards):
        """
        Rollback all shards to the previous state.
        
        Parameters:
            shards (list): The list of shard URIs.
        
        Returns:
            str: The result of the rollback process.
        """
        from .backup_manager import BackupManager
        backup_manager = BackupManager()
        host = self.app_state.config_store.get('HOST')
        port = self.app_state.config_store.get('PORT')
        shard_uris = [f"{shard}:{port}" for shard in shards if shard != host]
        return await backup_manager.backup_rollback()

    def merge_data(self, all_data):
        """
        Merge data from multiple sources into a single dataset.
        
        Parameters:
            all_data (list): The list of datasets to merge.
        
        Returns:
            dict: The merged dataset.
        """
        def deep_merge_dict(target, source):
            for key, value in source.items():
                if key in target:
                    if isinstance(target[key], dict) and isinstance(value, dict):
                        deep_merge_dict(target[key], value)
                    elif isinstance(target[key], list) and isinstance(value, list):
                        target[key].extend(value)
                    elif isinstance(target[key], set) and isinstance(value, set):
                        target[key].update(value)
                    else:
                        target[key] = value
                else:
                    target[key] = value

        merged = {}
        for data in all_data:
            deep_merge_dict(merged, self.convert_sets_to_lists(data))
        return merged

    def convert_sets_to_lists(self, data):
        """
        Convert sets to lists in the given data.
        
        Parameters:
            data (Any): The data to convert.
        
        Returns:
            Any: The converted data.
        """
        if isinstance(data, set):
            return list(data)
        elif isinstance(data, dict):
            return {k: self.convert_sets_to_lists(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.convert_sets_to_lists(item) for item in data]
        return data

    def prepare_value(self, value):
        """
        Prepare a value for storage or transmission.
        
        Parameters:
            value (Any): The value to prepare.
        
        Returns:
            str: The prepared value as a string.
        """
        if not isinstance(value, str):
            try:
                value = ujson.dumps(value)
            except TypeError as e:
                print(f"Error converting value to JSON: {e}")
                return None
        return value

    def prepare_data_for_transmission(self, data):
        """
        Prepare data for transmission over the network.
        
        Parameters:
            data (Any): The data to prepare.
        
        Returns:
            Any: The prepared data.
        """
        if isinstance(data, dict):
            return {key: self.prepare_data_for_transmission(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.prepare_data_for_transmission(item) for item in data]
        elif isinstance(data, set):
            return [self.prepare_data_for_transmission(item) for item in data]
        elif isinstance(data, (int, float, str)):
            return data
        elif isinstance(data, str):
            try:
                ujson.loads(data)
                return data
            except ujson.JSONDecodeError:
                return ujson.dumps(data)
        else:
            raise TypeError(f"Unsupported data type: {type(data)}")