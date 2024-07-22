# Import necessary modules and classes
import ujson  # Module for JSON operations
import re  # Module for regular expressions
import time  # Module for time
from .app_state import AppState  # Application state management
from .connection_handler import asyncio, signal_stop  # Asynchronous programming and signal handling
from .config import save_config  # Function to save configuration
from .update_manager import UpdateManager  # Update management
from .scheduler import SchedulerManager  # Scheduler management
from .sharding_manager import ShardingManager  # Sharding management
from .blockchain_manager import BlockchainManager  # Blockchain management
from .data_manager import DataManager  # Data management
from .indices_manager import IndicesManager  # Indices management
from .backup_manager import BackupManager  # Backup management
from .sub_pub_manager import SubPubManager  # Publish/subscribe management
from .command_utils import CommandUtilsManager  # Utility functions for command handling
from .replication_manager import ReplicationManager  # Replication management
from .cache_manager import CacheManager  # Cache management
from .upload_manager import UploadManager  # Upload management
from .two_factor_manager import TwoFactorManager  # Two Factor management

class CommandProcessor:
    def __init__(self, thread_executor, process_executor):
        self.app_state = AppState()
        self.thread_executor = thread_executor
        self.process_executor = process_executor
        # Initialize other managers and handlers
        self.updater = UpdateManager()
        self.scheduler_manager = SchedulerManager()
        self.sharding_manager = ShardingManager()
        self.blockchain_manager = BlockchainManager()
        self.data_manager = DataManager()
        self.indices_manager = IndicesManager()
        self.backup_manager = BackupManager()
        self.sub_pub_manager = SubPubManager()
        self.command_utils_manager = CommandUtilsManager()
        self.replication_manager = ReplicationManager()
        self.config_handler = ConfigCommandHandler(self)
        self.data_handler = DataCommandHandler(self)
        self.query_handler = QueryCommandHandler(self)
        self.shard_handler = ShardCommandHandler(self)
        self.cache_handler = CacheManager()
        self.upload_manager = UploadManager()
        self.two_factor_manager = TwoFactorManager()

    async def run_in_executor(self, executor_type, func, *args):
        loop = asyncio.get_running_loop()
        executor = self.thread_executor if executor_type == 'thread' else self.process_executor
        start_time = time.time()
        result = await loop.run_in_executor(executor, func, *args)
        end_time = time.time()
        return result

    async def process_command(self, command_line, sid=False, websocket=None):
        command, args = self.parse_command_line(command_line)
        if command:
            await self.notify_if_sid(sid, command_line)
            result = await self.execute_command(command, args, sid, websocket)
            return await self.handle_result(result)
        else:
            return "ERROR: Invalid command"

    def parse_command_line(self, command_line):
        command_line = command_line.replace('-f', '')
        tokens = command_line.split(maxsplit=1)
        command = tokens[0].upper()
        args = tokens[1] if len(tokens) > 1 else ""
        return command, args

    async def notify_if_sid(self, sid, command_line):
        if sid:
            await self.sub_pub_manager.notify_monitors(command_line, sid)

    async def execute_command(self, command, args, sid, websocket):
        commands = {
            'CHECKUPDATE': self.updater.check_update,
            'CONFIG': self.config_handler.handle_command,
            'SERVERSTOP': self.server_stop,
            'BACKUP': self.backup_manager.handle_backup_command,
            'INDICES': self.indices_manager.indice_command,
            'KEYS': self.data_handler.keys_command,
            'COUNT': self.data_handler.count_command,
            'SET': self.data_handler.set_command,
            'RENAME': self.data_handler.rename_command,
            'DEL': self.data_handler.del_command,
            'INCR': self.data_handler.incr_decr_command,
            'DECR': self.data_handler.incr_decr_command,
            'QUERY': self.query_handler.query_command,
            'SUBLIST': self.sub_pub_manager.list_subscriptions,
            'SUB': self.sub_pub_manager.subscribe_command,
            'UNSUB': self.sub_pub_manager.unsubscribe_command,
            'SCHEDULE': self.scheduler_manager.schedule_command,
            'FLUSHALL': self.data_handler.flush_all_command,
            'FLUSHCACHE': self.cache_handler.flush_cache,
            'REPLICATE': self.shard_handler.replicate_command,
            'RESHARD': self.shard_handler.reshard_command,
            'ROLLBACK': self.backup_manager.backup_rollback,
            'BLOCKCHAIN': self.blockchain_manager.get_blockchain,
            'NEW_WALLET': self.blockchain_manager.new_wallet,
            'GET_WALLET': self.blockchain_manager.get_wallet,
            'BLOCK': self.blockchain_manager.add_block,
            'UPLOAD_FILE': self.upload_manager.save_file,
            'READ_FILE': self.upload_manager.read_file,
            'NEW_2FA': self.two_factor_manager.generate,
            'VERIFY_2FA': self.two_factor_manager.verify,
        }

        if command in commands:
            func = commands[command]
            if asyncio.iscoroutinefunction(func):
                if command == 'UPLOAD_FILE':
                    key, data = args.split(' ', 1)
                    return await func(key, data.encode())
                elif command in {'INCR', 'DECR'}:
                    return await func(args, True if command == 'INCR' else False)
                elif command in {'CONFIG', 'SUB', 'UNSUB', 'MONITOR'}:
                    return await func(args, sid)
                elif command == 'BLOCK':
                    return await func(args)
                elif command == 'NEW_2FA':
                    account_name, issuer_name = args.split(' ')
                    return await func(account_name, issuer_name)
                elif command == 'VERIFY_2FA':
                    secret, code = args.split(' ')
                    return await func(secret, code)
                elif command == 'BLOCKCHAIN':
                    async_gen = await func(args)
                    async for chunk in async_gen:
                        await websocket.send(chunk)
                    return None
                else:
                    return await func(args)
            else:
                if command == 'UPLOAD_FILE':
                    key, data = args.split(' ', 1)
                    return await self.run_in_executor('thread', func, key, data.encode())
                elif command in {'INCR', 'DECR'}:
                    return await self.run_in_executor('thread', func, args, True if command == 'INCR' else False)
                elif command in {'CONFIG', 'SUB', 'UNSUB', 'MONITOR'}:
                    return await self.run_in_executor('thread', func, args, sid)
                elif command == 'NEW_2FA':
                    account_name, issuer_name = args.split(' ')
                    return await self.run_in_executor('thread', func, account_name, issuer_name)
                elif command == 'VERIFY_2FA':
                    secret, code = args.split(' ')
                    return await self.run_in_executor('thread', func, secret, code)
                else:
                    return await self.run_in_executor('thread', func, args)
        else:
            return None


    async def handle_result(self, result):
        """Handles the result of a command execution."""
        if asyncio.iscoroutine(result):
            return await result
        else:
            return result

    async def server_stop(self, *args, **kwargs):
        """Stops the server."""
        await signal_stop()
        return 'exit'

class ConfigCommandHandler:
    def __init__(self, processor):
        self.processor = processor  # Reference to the CommandProcessor

    async def handle_command(self, args, sid=None):
        """Handles CONFIG commands."""
        parts = [part.strip() for part in args.split(maxsplit=2)]
        if not parts or parts[0].upper() == 'SHOW':
            return await self.handle_config_show_command()  # Show configuration if no arguments or 'SHOW'
        if len(parts) < 2:
            return "ERROR: Insufficient arguments for CONFIG command"

        action = parts[0].upper()
        key = parts[1]

        if action == 'SET':
            return await self.handle_config_set_command(parts)  # Handle 'SET' action
        elif action == 'DEL':
            return await self.handle_config_del_command(key)  # Handle 'DEL' action
        elif action == 'SHOW':
            return await self.handle_config_show_command()  # Handle 'SHOW' action
        else:
            return "ERROR: Unsupported CONFIG command"  # Return error for unsupported actions

    async def handle_config_show_command(self):
        """Shows the current configuration."""
        return ujson.dumps(AppState().config_store)  # Return the configuration as a JSON string

    async def handle_config_set_command(self, parts):
        """Sets a configuration value."""
        if len(parts) < 3:
            return "ERROR: Missing value for setting key"
        key = parts[1]
        value = parts[2]

        if key == 'SHARDING' and value == '1' and (not AppState().config_store.get('SHARDS') or len(AppState().config_store.get('SHARDS')) == 0):
            return "ERROR: Cannot enable SHARDING without defined SHARDS. First run the command CONFIG SET SHARDS ADD serverip/domain"

        if key in ['REPLICATION_AUTHORIZED_SLAVES', 'SHARDS']:
            return await self.handle_config_set_replication_shards(key, value)  # Handle replication and shards configuration
        else:
            return await self.handle_config_set_value(key, value)  # Handle other configuration values

    async def handle_config_set_replication_shards(self, key, value):
        """Handles setting replication and shards configuration."""
        operation, value = value.split(maxsplit=1)
        operation = operation.upper()

        if key not in AppState().config_store or not isinstance(AppState().config_store[key], list):
            AppState().config_store[key] = []

        if operation == 'ADD':
            return await self.handle_config_add_to_list(key, value)  # Add value to the list
        elif operation == 'DEL':
            return await self.handle_config_remove_from_list(key, value)  # Remove value from the list
        else:
            return f"ERROR: Invalid operation for {key} (ADD/DEL required)"

    async def handle_config_add_to_list(self, key, value):
        """Adds a value to a configuration list."""
        if value not in AppState().config_store[key]:
            AppState().config_store[key].append(value)
            save_config()  # Save the updated configuration
            res = f"Added {value} to {key}"
        else:
            res = f"{value} already in {key}"
        if key == 'SHARDS' and await self.processor.sharding_manager.has_sharding() and await self.processor.sharding_manager.is_sharding_master():
            await self.processor.shard_handler.reshard_command()  # Reshard data if sharding is enabled
            res += " and resharded data to shards"
        return res

    async def handle_config_remove_from_list(self, key, value):
        """Removes a value from a configuration list."""
        if value in AppState().config_store[key]:
            if key == 'SHARDS' and value == AppState().config_store.get('HOST') and AppState().config_store.get('SHARDING') == '1':
                return "ERROR: Cannot remove the host from SHARDS while SHARDING is enabled. First run the command CONFIG SET SHARDING 0"
            AppState().config_store[key].remove(value)
            save_config()  # Save the updated configuration
            res = f"Removed {value} from {key}"
        else:
            res = f"{value} not found in {key}"
        if key == 'SHARDS' and await self.processor.sharding_manager.has_sharding() and await self.processor.sharding_manager.is_sharding_master():
            await self.processor.shard_handler.reshard_command()  # Reshard data if sharding is enabled
            res += " and resharded data to shards"
        return res

    async def handle_config_set_value(self, key, value):
        """Sets a configuration value."""
        if AppState().config_store.get(key) == value:
            return f"{key} already set to {value}"

        AppState().config_store[key] = value
        save_config()  # Save the updated configuration

        response = f"Config updated: {key} set to {value}"

        restart_required = key in ['USERNAME', 'PASSWORD']

        if restart_required:
            response += " - Restart required to apply new settings."
            AppState().restart_required = True

        if key == 'SHARDING' and await self.processor.sharding_manager.is_sharding_master():
            reshard_response = await self.processor.shard_handler.reshard_command()
            response += f" - {reshard_response}" if value == '1' else " - Data stored on Master"

        if key == 'SCHEDULER':
            if value == '0':
                scheduler_response = await self.processor.scheduler_manager.stop_scheduler()
            elif value == '1':
                scheduler_response = await self.processor.scheduler_manager.start_scheduler()
            else:
                return f"Error updating {key}! Value has to be 0 or 1"
            response += f" - {scheduler_response}"
        return response

    async def handle_config_del_command(self, key):
        """Deletes a configuration key."""
        protected_keys = ['HOST', 'PORT', 'USERNAME', 'PASSWORD', 'BACKUP_ON_SHUTDOWN', 'SCHEDULER', 'REPLICATION', 'REPLICATION_TYPE', 'REPLICATION_AUTHORIZED_SLAVES', 'SHARDING_TYPE', 'SHARDING', 'SHARDING_BATCH_SIZE', 'SHARDS']
        if key in protected_keys:
            return f"ERROR: Cannot delete essential configuration key '{key}'"
        if key in AppState().config_store:
            del AppState().config_store[key]
            save_config()  # Save the updated configuration
            return f"Configuration key '{key}' has been deleted"
        else:
            return f"Configuration key '{key}' does not exist"

class DataCommandHandler:
    def __init__(self, processor):
        self.processor = processor  # Reference to the CommandProcessor

    def keys_command(self, *args, **kwargs):
        """Handles the KEYS command."""
        data_store = AppState().data_store  # Access the data store
        main_keys = list(data_store.keys())  # Get the list of main keys
        return ujson.dumps(sorted(main_keys))  # Return the sorted list of keys as a JSON string

    async def count_command(self, args):
        """Handles the COUNT command."""
        parts = args.split('WHERE', 1)  # Split the arguments by 'WHERE'
        root = parts[0].strip()  # Get the root key
        conditions = parts[1].strip() if len(parts) > 1 else ""  # Get the conditions

        # Use process_local_query to get the filtered results
        results = await self.processor.query_handler.process_local_query(root, f"WHERE {conditions}" if conditions else "")

        if isinstance(results, list):
            return len(results)  # Return the number of results if it's a list
        elif isinstance(results, dict):
            return len(results.keys())  # Return the number of keys if it's a dictionary
        return 0  # Return 0 if no results

    async def set_command(self, args):
        """Handles the SET command."""
        commands = args.split('|')  # Split the commands by '|'
        responses = []
        sharding_active = await self.processor.sharding_manager.has_sharding()  # Check if sharding is active
        for command in commands:
            # Extract wallet if present
            wallet_sender_match = re.search(r"FOR WALLET:([A-Za-z0-9]+)", command.strip())  # Match the wallet pattern
            wallet_receiver_match = re.search(r"TO WALLET:([A-Za-z0-9]+)", command.strip())  # Match the wallet pattern

            if wallet_sender_match:
                wallet_sender = wallet_sender_match.group(1)  # Extract the wallet value
                command = re.sub(r"FOR WALLET:[A-Za-z0-9]+", "", command.strip())  # Remove the wallet part from the command
            else:
                wallet_sender = None  # No wallet found
            
            if wallet_receiver_match:
                wallet_receiver = wallet_receiver_match.group(1)  # Extract the wallet value
                command = re.sub(r"TO WALLET:[A-Za-z0-9]+", "", command.strip())  # Remove the wallet part from the command
            else:
                wallet_receiver = None  # No wallet found
            
            # Extract command
            match = re.match(r"([^ ]+) (.+)", command.strip())  # Match the command pattern
            if not match:
                responses.append("ERROR: Invalid SET syntax")
                continue

            key_pattern = match.group(1)  # Get the key pattern
            raw_value = match.group(2)  # Get the raw value

            if 'EXPIRE' in raw_value and not self.processor.scheduler_manager.is_scheduler_active():
                return "Scheduler is not active. Run the command CONFIG SET SCHEDULER 1 to activate"
            
            value, expiry = self.processor.command_utils_manager.parse_value_instructions(raw_value)  # Parse the value instructions
            context = self.processor.command_utils_manager.get_context_from_key(AppState().data_store, key_pattern)  # Get the context from the key
            value = self.processor.command_utils_manager.handle_expression_functions(value, context)  # Handle expression functions
            parts = key_pattern.split(':')  # Split the key pattern by ':'
            sharding_key = ':'.join(parts[:2]) if len(parts) > 1 else parts[0]  # Get the sharding key

            if '*' in parts and sharding_active:
                responses.append("ERROR: Wildcard operations are not supported in sharding mode.")
                continue

            shard_result = await self.processor.sharding_manager.check_sharding('SET', command, sharding_key)  # Check sharding for SET command
            if shard_result != "ERROR" and shard_result != 'LOCAL':
                responses.append("OK")
                continue
            elif shard_result == "ERROR":
                responses.append("ERROR: Sharding failed")
                continue

            if '*' in parts:
                base_path = parts[:parts.index('*')]  # Get the base path
                last_key = parts[-1]  # Get the last key
                updated = await self.set_keys_wildcard(base_path, last_key, value, AppState().data_store)  # Set keys with wildcard
                responses.append(f"Updated {updated} entries.")
            else:
                response = await self.set_specific_key(parts, value)  # Set a specific key
                responses.append(response)

            if expiry:
                AppState().expires_store[key_pattern] = expiry  # Set the expiry
            
            # Add transaction to the blockchain
            if await self.processor.blockchain_manager.has_blockchain():
                sender = wallet_sender  # Example sender, you might replace this with the actual sender information
                receiver = wallet_receiver  # Example receiver, you might replace this with the actual receiver information
                amount = 0  # Amount associated with the transaction, you might adjust this based on your use case
                data = str({'command': 'SET', 'key': key_pattern, 'value': value})
                await self.processor.blockchain_manager.add_transaction(sender, receiver, amount, data)

            if await self.processor.replication_manager.has_replication_is_replication_master():
                replication_command = f"SET {':'.join(parts)} {value}"
                await self.processor.replication_manager.send_command_to_slaves(replication_command)  # Replicate the command to slaves

        return '\n'.join(responses)

    async def set_keys_wildcard(self, base_path, last_key, value, data_store):
        """Sets keys using wildcard in the base path."""
        try:
            parsed_value = ujson.loads(value)  # Parse the value as JSON
        except ujson.JSONDecodeError:
            parsed_value = value

        if isinstance(parsed_value, dict):
            total_updated_count = 0
            for key, val in parsed_value.items():
                nested_last_key = f"{last_key}:{key}"
                updated_count = await self.set_keys_wildcard(base_path, nested_last_key, ujson.dumps(val), data_store)  # Recursively set keys
                total_updated_count += updated_count
            return total_updated_count
        else:
            async def recursive_set(current, depth):
                count = 0
                if depth == len(base_path):
                    for item_key, item_value in current.items():
                        full_path = base_path + [item_key, last_key]
                        entity_key = ':'.join(full_path[:2]) if len(full_path) > 2 else full_path[0]
                        old_value = item_value.get(last_key)
                        if old_value is not None and old_value != parsed_value:
                            await self.processor.indices_manager.update_index_on_remove(full_path, last_key, old_value, entity_key)
                        item_value[last_key] = parsed_value
                        await self.processor.indices_manager.update_index_on_add(full_path, last_key, parsed_value, entity_key)
                        count += 1
                    return count
                elif base_path[depth] in current:
                    return await recursive_set(current[base_path[depth]], depth + 1)
                return 0

            updated_count = await recursive_set(data_store, 0)  # Recursively set keys
            if updated_count > 0:
                full_key = ':'.join(base_path + [last_key])
                full_data = data_store
                await self.processor.cache_handler.remove_from_cache(base_path)  # Invalidate cache entries
                await self.processor.sub_pub_manager.notify_subscribers(full_key, full_data)  # Notify subscribers

            AppState().data_has_changed = True

            if not self.processor.scheduler_manager.is_scheduler_active():
                self.processor.data_manager.save_data()  # Save data if scheduler is not active
            return updated_count

    async def set_specific_key(self, parts, value):
        """Sets a specific key."""
        try:
            parsed_value = ujson.loads(value)  # Parse the value as JSON
            if isinstance(parsed_value, dict):
                for key, val in parsed_value.items():
                    nested_parts = parts + [key]
                    await self.set_individual_key(nested_parts, val)  # Set individual keys recursively
                return "OK"
        except ujson.JSONDecodeError:
            parsed_value = value
        return await self.set_individual_key(parts, parsed_value)  # Set the individual key

    async def set_individual_key(self, parts, value):
        """Sets an individual key."""
        current = AppState().data_store
        for part in parts[:-1]:
            current = current.setdefault(part, {})  # Navigate to the appropriate part of the data store
        base_key = parts[0]
        last_key = parts[-1]
        entity_key = ':'.join(parts[:2]) if len(parts) > 2 else parts[0]

        if last_key in current and current[last_key] != value:
            await self.processor.indices_manager.update_index_on_remove(parts, last_key, current[last_key], entity_key)

        if isinstance(value, str) and value.startswith('[') and value.endswith(']'):
            try:
                value = ujson.loads(value)
            except ujson.JSONDecodeError:
                pass

        current[last_key] = value  # Set the value
        await self.processor.indices_manager.update_index_on_add(parts, last_key, value, entity_key)  # Update the index
        await self.processor.cache_handler.remove_from_cache(base_key)  # Invalidate cache entries

        full_key = ':'.join(parts)
        await self.processor.sub_pub_manager.notify_subscribers(full_key, current)  # Notify subscribers

        AppState().data_has_changed = True

        if not self.processor.scheduler_manager.is_scheduler_active():
            self.processor.data_manager.save_data()  # Save data if scheduler is not active

        return "OK"

    async def del_command(self, args):
        """Handles the DEL command."""
        commands = args.split('|')  # Split the commands by '|'
        responses = []
        sharding_active = await self.processor.sharding_manager.has_sharding()  # Check if sharding is active

        for command in commands:
            parts = command.strip().split(':')
            if not parts or '' in parts:
                responses.append("ERROR: Invalid DEL syntax")
                continue
            shard_key = ':'.join(parts[:2]) if len(parts) > 1 else parts[0]  # Get the sharding key

            if '*' in parts and sharding_active:
                responses.append("ERROR: Wildcard deletions are not supported in sharding mode.")
                continue

            shard_result = await self.processor.sharding_manager.check_sharding('DEL', command, shard_key)  # Check sharding for DEL command
            if shard_result not in ["ERROR", "LOCAL"]:
                responses.append("OK")
                continue
            elif shard_result == "ERROR":
                responses.append(shard_result)
                continue

            if '*' in parts:
                base_path = parts[:parts.index('*')]  # Get the base path
                last_key = parts[-1]  # Get the last key
                deleted_count = await self.delete_keys_wildcard(base_path, last_key, AppState().data_store)  # Delete keys with wildcard
                responses.append(f"Deleted {deleted_count} entries.")
            else:
                response = await self.delete_specific_key(parts)  # Delete a specific key
                responses.append(response)

            if await self.processor.replication_manager.has_replication_is_replication_master():
                await self.processor.replication_manager.send_command_to_slaves(f"DEL {command}")  # Replicate the command to slaves

        return '\n'.join(responses)

    async def delete_keys_wildcard(self, base_path, last_key, data_store):
        """Deletes keys using wildcard in the base path."""
        def recursive_delete(current):
            count = 0
            if isinstance(current, dict):
                for key in list(current.keys()):
                    if key == last_key:
                        del current[key]
                        count += 1
                    else:
                        count += recursive_delete(current[key])
            return count
        current = data_store
        try:
            for key in base_path:
                current = current[key]
            deleted_count = recursive_delete(current)  # Recursively delete keys
            await self.processor.cache_handler.remove_from_cache(base_path)  # Invalidate cache entries
            AppState().data_has_changed = True
            if not self.processor.scheduler_manager.is_scheduler_active():
                self.processor.data_manager.save_data()  # Save data if scheduler is not active
            return deleted_count
        except KeyError:
            return 0

    async def delete_specific_key(self, parts):
        """Deletes a specific key."""
        try:
            base_key = parts[0]
            current_data = AppState().data_store
            for part in parts[:-1]:
                current_data = current_data[part]

            key_to_delete = parts[-1]
            if key_to_delete in current_data:
                value_to_remove = current_data[key_to_delete]
                if isinstance(value_to_remove, dict):
                    await self.processor.indices_manager.remove_entity_from_index(parts, value_to_remove)  # Remove entity from index
                else:
                    if len(parts) > 3:
                        remove_field_parts = parts[:1] + parts[2:]
                    else:
                        remove_field_parts = parts
                    await self.processor.indices_manager.remove_field_from_index(remove_field_parts[:-1], key_to_delete, value_to_remove)  # Remove field from index

                del current_data[key_to_delete]  # Delete the key
                self.processor.command_utils_manager.cleanup_empty_dicts(AppState().data_store, parts[:-1])  # Clean up empty dictionaries

                AppState().data_has_changed = True

                if not self.processor.scheduler_manager.is_scheduler_active():
                    self.processor.data_manager.save_data()  # Save data if scheduler is not active

                # Invalidate cache entries related to the base key
                await self.processor.cache_handler.remove_from_cache(base_key)  # Invalidate cache entries
                
                return "OK"
            else:
                return "ERROR: Key does not exist"
        except KeyError:
            return "ERROR: Path not found"
        except Exception as e:
            return f"ERROR: Exception occurred - {str(e)}"

    async def incr_decr_command(self, args, increment):
        """Handles the INCR and DECR commands."""
        commands = args.split('|')  # Split the commands by '|'
        responses = []

        for command in commands:
            parts = command.strip().split()
            if len(parts) < 2:
                responses.append("ERROR: Invalid syntax")
                continue
            keys = parts[0].split(':')

            try:
                amount = float(parts[1]) if '.' in parts[1] else int(parts[1])
            except ValueError:
                responses.append("ERROR: Invalid amount")
                continue

            shard_key = f'{keys[0]}:{keys[1]}' if len(keys) > 1 else keys[0]  # Get the sharding key
            shard_command = 'INCR' if increment else 'DECR'
            shard_result = await self.processor.sharding_manager.check_sharding(shard_command, command.strip(), shard_key)  # Check sharding for INCR/DECR command

            if shard_result != "ERROR" and shard_result != 'LOCAL':
                responses.append("OK")
                continue
            elif shard_result == "ERROR":
                responses.append(shard_result)
                continue

            data = self.processor.command_utils_manager.get_nested_value(AppState().data_store, keys)  # Get the current value
            if data is None:
                data = 0
            original_type = float if isinstance(data, float) or isinstance(amount, float) else int
            new_value = data + amount if increment else data - amount  # Calculate the new value

            if original_type is int:
                new_value = int(new_value)

            new_data = self.processor.command_utils_manager.set_nested_value(AppState().data_store, keys, new_value)  # Set the new value

            AppState().data_has_changed = True

            if not self.processor.scheduler_manager.is_scheduler_active():
                self.processor.data_manager.save_data()  # Save data if scheduler is not active

            key = ":".join(keys)
            await self.processor.sub_pub_manager.notify_subscribers(key, new_data)  # Notify subscribers
            responses.append("OK")

            if await self.processor.replication_manager.has_replication_is_replication_master():
                replication_command = 'INCR' if increment else 'DECR'
                await self.processor.replication_manager.send_command_to_slaves(f"{replication_command} {command}")  # Replicate the command to slaves

        return '\n'.join(responses)

    async def rename_command(self, args):
        """Handles the RENAME command."""
        if ' TO ' not in args:
            return "ERROR: Invalid RENAME syntax"
        command = args
        path, new_key = args.split(' TO ')
        path_parts = path.split(':')
        new_key = new_key.strip()
        sharding_key = ':'.join(path_parts[:2]) if len(path_parts) > 1 else path_parts[0]  # Get the sharding key

        shard_result = await self.processor.sharding_manager.check_sharding('RENAME', path, sharding_key)  # Check sharding for RENAME command
        if shard_result != "ERROR" and shard_result != 'LOCAL':
            return "OK"
        elif shard_result == "ERROR":
            return "ERROR: Sharding failed"

        if '*' in path_parts:
            base_path = path_parts[:path_parts.index('*')]  # Get the base path
            target_key = path_parts[-1]  # Get the target key

            def recursive_rename(current, depth):
                if depth == len(base_path):
                    keys_renamed = 0
                    for item in current.values():
                        if target_key in item:
                            item[new_key] = item.pop(target_key)
                            keys_renamed += 1
                    return keys_renamed
                elif base_path[depth] in current:
                    return recursive_rename(current[base_path[depth]], depth + 1)
                return 0

            keys_renamed = recursive_rename(AppState().data_store, 0)  # Recursively rename keys

            AppState().data_has_changed = True

            if not self.processor.scheduler_manager.is_scheduler_active():
                self.processor.data_manager.save_data()  # Save data if scheduler is not active

            if await self.processor.replication_manager.has_replication_is_replication_master():
                await self.processor.replication_manager.send_command_to_slaves(f"RENAME {command}")  # Replicate the command to slaves

            return f"RENAME successful: {keys_renamed} keys renamed." if keys_renamed else "Nothing to rename"
        else:
            current = AppState().data_store
            for part in path_parts[:-1]:
                if part in current:
                    current = current[part]
                else:
                    return "ERROR: Path not found"

            if path_parts[-1] in current:
                current[new_key] = current.pop(path_parts[-1])  # Rename the key
                AppState().data_has_changed = True

                if not self.processor.scheduler_manager.is_scheduler_active():
                    self.processor.data_manager.save_data()  # Save data if scheduler is not active

                if await self.processor.replication_manager.has_replication_is_replication_master():
                    await self.processor.replication_manager.send_command_to_slaves(f"RENAME {command}")  # Replicate the command to slaves
                return "RENAME successful: 1 key renamed."
            else:
                return "ERROR: Key not found to rename."

    async def flush_all_command(self, *args, **kwargs):
        """Handles the FLUSHALL command."""
        host = AppState().config_store.get('HOST')
        port = AppState().config_store.get('PORT')
        AppState().data_store.clear()  # Clear the data store
        AppState().indices.clear()  # Clear the indices
        self.processor.cache_handler.flush_cache  # Clear cached data
        self.processor.data_manager.save_data()  # Save the data
        self.processor.indices_manager.save_indices()  # Save the indices

        if await self.processor.sharding_manager.has_sharding_is_sharding_master():
            shards = AppState().config_store.get('SHARDS')
            shard_uris = [f"{shard}:{port}" for shard in shards if shard != host]  # Get the URIs of the shards
            results = await self.processor.sharding_manager.broadcast_query('FLUSHALL', shard_uris)  # Broadcast FLUSHALL command to shards
            print("Flush results from remote shards:", results)

        return "All indices and data flushed successfully."

class QueryCommandHandler:
    def __init__(self, processor):
        self.processor = processor  # Reference to the CommandProcessor

    async def batch_and_send_results(self, results, websocket, CHUNK_SIZE):
        """Batches the results into smaller chunks and sends them via WebSocket."""
        total_results = len(results)
        for i in range(0, total_results, CHUNK_SIZE):
            batch = results[i:i + CHUNK_SIZE]
            await websocket.send(ujson.dumps(batch))

    async def query_command(self, args):
        """Handles the QUERY command."""
        websocket = AppState().websocket  # Get the WebSocket from AppState
        CHUNK_SIZE = 1000
        parts = args.split(' ', 1)  # Split the arguments by space
        root = parts[0]  # Get the root key
        conditions = parts[1] if len(parts) > 1 else ""  # Get the conditions
        sharding_active = await self.processor.sharding_manager.has_sharding()  # Check if sharding is active

        # Check if the root key exists
        keys = root.split(':')
        
        # Traverse the data_store dictionary to check if the nested key exists
        data_store = AppState().data_store
        for key in keys:
            if key in data_store:
                data_store = data_store[key]
            else:
                return "[]"  # Return an empty JSON array if any part of the key doesn't exist

        if 'JOIN' in conditions and sharding_active:
            return "JOIN operations are not supported in sharding mode."
        modifiers, conditions = self.processor.command_utils_manager.parse_modifiers(conditions)  # Parse the modifiers
        group_by_key = None
        limit_values = None

        if 'group_by' in modifiers:
            group_by_key = modifiers['group_by']
            conditions = conditions.replace(f"GROUPBY({group_by_key})", "").strip()

        if 'limit' in modifiers:
            limit_values = modifiers['limit']
            limit_str = f"LIMIT({limit_values})" if isinstance(limit_values, str) else f"LIMIT({limit_values[0]},{limit_values[1]})"
            conditions = conditions.replace(limit_str, "").strip()

        if not await self.processor.sharding_manager.has_sharding_is_sharding_master():
            local_results = await self.process_local_query(root, conditions, modifiers)  # Process the query locally

            if isinstance(local_results, str):
                return local_results

            if len(local_results) > CHUNK_SIZE:
                await self.batch_and_send_results(local_results, websocket, CHUNK_SIZE)

            return ujson.dumps(local_results)

        host = AppState().config_store.get('HOST')
        port = AppState().config_store.get('PORT')
        shard_uris = [f"{shard}:{port}" for shard in AppState().config_store.get('SHARDS') if shard != host]  # Get the URIs of the shards
        remote_results = await self.processor.sharding_manager.broadcast_query(f'QUERY {root} {conditions}', shard_uris)  # Broadcast the query to shards
        local_results = await self.process_local_query(root, conditions, modifiers)  # Process the query locally

        if isinstance(remote_results, list) and len(remote_results) == 1 and isinstance(remote_results[0], str):
            remote_results = remote_results[0]

        combined_results = local_results if isinstance(local_results, (str, int, float)) else local_results + remote_results  # Combine local and remote results
        final_results = self.processor.command_utils_manager.apply_query_modifiers(combined_results, modifiers)  # Apply query modifiers

        if isinstance(final_results, (str, int, float)):
            return str(final_results)

        if len(final_results) > CHUNK_SIZE:
            await self.batch_and_send_results(final_results, websocket, CHUNK_SIZE)
            return "Results sent in batches via WebSocket."

        return ujson.dumps(final_results)

    async def process_local_query(self, root, conditions, modifiers=None):
        """Processes a query locally."""
        key_parts = root.split(':')
        main_key = key_parts[0]  # Get the main key
        specific_key = key_parts[1] if len(key_parts) > 1 else None  # Get the specific key
        sub_key_path = key_parts[2:] if len(key_parts) > 2 else []  # Get the sub-key path

        # Check the cache first
        command = f"QUERY {root} {conditions if conditions else ''} {modifiers if modifiers else ''}".strip() # Construct the full command for caching purposes
        cache_result = await self.processor.cache_handler.get_cache(command)
        if cache_result is not None:
            return cache_result

        if conditions.startswith("WHERE "):
            conditions = conditions[6:].strip()  # Remove 'WHERE' from the conditions

        include_fields, exclude_fields, conditions = self.processor.command_utils_manager.parse_and_clean_fields(conditions)  # Parse and clean fields
        joins, conditions = self.processor.command_utils_manager.extract_join_clauses(conditions)  # Extract join clauses
        data_to_query = AppState().data_store.get(main_key, {})  # Get the data to query

        if specific_key:
            specific_entry = data_to_query.get(specific_key)

            if isinstance(specific_entry, dict) and sub_key_path:
                for key in sub_key_path:
                    if key in specific_entry:
                        specific_entry = specific_entry[key]
                    else:
                        break

            if not isinstance(specific_entry, dict):
                specific_entry = self.processor.data_manager.process_nested_data(specific_entry)  # Process nested data

            for join_table, join_key in joins:
                join_value = str(specific_entry.get(join_key))
                join_ids = AppState().indices.get(join_table, {}).get(join_key, {}).get(join_value, [])
                joined_data = [
                    {'key': jid.split(':')[1], **AppState().data_store[join_table][jid.split(':')[1]]}
                    for jid in join_ids if jid.split(':')[1] in AppState().data_store[join_table]
                ]
                specific_entry.setdefault(join_table, []).extend(joined_data)

            results = self.processor.command_utils_manager.format_as_list(specific_entry)  # Format as list

        elif conditions:
            results = self.processor.command_utils_manager.eval_conditions_using_indices(conditions, AppState().indices, main_key, joins)  # Evaluate conditions using indices
            results = self.processor.command_utils_manager.format_as_list(results)  # Format as list
        else:
            results = self.processor.command_utils_manager.format_as_list(self.processor.data_manager.process_nested_data(data_to_query))  # Process nested data

        if modifiers:
            results = self.processor.command_utils_manager.apply_query_modifiers(results, modifiers)  # Apply query modifiers

        results = [self.processor.command_utils_manager.filter_fields(entry, include_fields, exclude_fields) for entry in results]  # Filter fields

        # Add the result to the cache
        if results:
            await self.processor.cache_handler.add_to_cache(command, root, results)

        return results

class ShardCommandHandler:
    def __init__(self, processor):
        self.processor = processor  # Reference to the CommandProcessor

    async def replicate_command(self, *args, **kwargs):
        """Handles the REPLICATE command."""
        websocket = AppState().websocket

        if await self.processor.replication_manager.has_replication_is_replication_master():
            data = ujson.dumps(AppState().data_store)  # Get the data as a JSON string
            indices = ujson.dumps(AppState().indices)  # Get the indices as a JSON string
            CHUNK_SIZE = 1000
            data_chunks = [data[i:i+CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]  # Split the data into chunks
            indices_chunks = [indices[i:i+CHUNK_SIZE] for i in range(0, len(indices), CHUNK_SIZE)]  # Split the indices into chunks

            for data_chunk in data_chunks:
                await websocket.send(ujson.dumps({'data_chunks': [data_chunk], 'indices_chunks': []}))  # Send data chunks

            for indices_chunk in indices_chunks:
                await websocket.send(ujson.dumps({'data_chunks': [], 'indices_chunks': [indices_chunk]}))  # Send indices chunks

            await websocket.send('DONE')  # Send DONE signal

    async def reshard_command(self, *args, **kwargs):
        """Handles the RESHARD command."""
        AppState().data_has_changed = True
        AppState().indices_has_changed = True

        self.processor.data_manager.save_data()  # Save the data
        self.processor.indices_manager.save_indices()  # Save the indices
        self.processor.backup_manager.backup_data()  # Backup the data

        if await self.processor.sharding_manager.is_sharding_master():
            return ujson.dumps(await self.processor.sharding_manager.reshard())  # Reshard if sharding master
        else:
            local_data = self.processor.data_manager.get_all_local_data()  # Get all local data
            local_indices = self.processor.indices_manager.get_all_local_indices()  # Get all local indices

            AppState().data_store.clear()  # Clear the data store
            AppState().indices.clear()  # Clear the indices
            AppState().data_has_changed = True
            AppState().indices_has_changed = True

            self.processor.data_manager.save_data()  # Save the data
            self.processor.indices_manager.save_indices()  # Save the indices
            prepared_data = self.processor.sharding_manager.prepare_data_for_transmission(local_data)  # Prepare data for transmission
            prepared_indices = self.processor.sharding_manager.prepare_data_for_transmission(local_indices)  # Prepare indices for transmission

            response = {
                "local_data": prepared_data,
                "local_indices": prepared_indices
            }

            return ujson.dumps(response)  # Return the response as a JSON string