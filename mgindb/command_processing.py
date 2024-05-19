# Import necessary modules and classes
import json
import re
from .app_state import AppState
from .connection_handler import asyncio, signal_stop
from .config import save_config
from .update_manager import UpdateManager
from .scheduler import SchedulerManager
from .sharding_manager import ShardingManager
from .data_manager import DataManager
from .indices_manager import IndicesManager
from .backup_manager import BackupManager
from .sub_pub_manager import SubPubManager
from .command_utils import CommandUtilsManager
from .replication_manager import ReplicationManager

class CommandProcessor:
    def __init__(self):
        self.updater = UpdateManager()
        self.scheduler_manager = SchedulerManager()
        self.sharding_manager = ShardingManager()
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

    async def process_command(self, command_line, sid=False):
        command, args = self.parse_command_line(command_line)
        if command:
            await self.notify_if_sid(sid, command_line)
            result = self.execute_command(command, args, sid)
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

    def execute_command(self, command, args, sid):
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
            'REPLICATE': self.shard_handler.replicate_command,
            'RESHARD': self.shard_handler.reshard_command,
            'ROLLBACK': self.backup_manager.backup_rollback,
        }

        if command in commands:
            func = commands[command]
            if command in {'INCR', 'DECR'}:
                return func(args, True if command == 'INCR' else False)
            elif command in {'CONFIG', 'SUB', 'UNSUB', 'MONITOR'}:
                return func(args, sid)
            else:
                return func(args)
        else:
            return None

    async def handle_result(self, result):
        if asyncio.iscoroutine(result):
            return await result
        else:
            return result

    async def server_stop(self, *args, **kwargs):
        await signal_stop()
        return 'exit'

class ConfigCommandHandler:
    def __init__(self, processor):
        self.processor = processor

    async def handle_command(self, args, sid=None):
        parts = [part.strip() for part in args.split(maxsplit=2)]
        if not parts or parts[0].upper() == 'SHOW':
            return await self.handle_config_show_command()
        if len(parts) < 2:
            return "ERROR: Insufficient arguments for CONFIG command"

        action = parts[0].upper()
        key = parts[1]

        if action == 'SET':
            return await self.handle_config_set_command(parts)
        elif action == 'DEL':
            return await self.handle_config_del_command(key)
        elif action == 'SHOW':
            return await self.handle_config_show_command()
        else:
            return "ERROR: Unsupported CONFIG command"

    async def handle_config_show_command(self):
        return json.dumps(AppState().config_store)

    async def handle_config_set_command(self, parts):
        if len(parts) < 3:
            return "ERROR: Missing value for setting key"
        key = parts[1]
        value = parts[2]

        if key == 'SHARDING' and value == '1' and (not AppState().config_store.get('SHARDS') or len(AppState().config_store.get('SHARDS')) == 0):
            return "ERROR: Cannot enable SHARDING without defined SHARDS. First run the command CONFIG SET SHARDS ADD serverip/domain"

        if key in ['REPLICATION_AUTHORIZED_SLAVES', 'SHARDS']:
            return await self.handle_config_set_replication_shards(key, value)
        else:
            return await self.handle_config_set_value(key, value)

    async def handle_config_set_replication_shards(self, key, value):
        operation, value = value.split(maxsplit=1)
        operation = operation.upper()

        if key not in AppState().config_store or not isinstance(AppState().config_store[key], list):
            AppState().config_store[key] = []

        if operation == 'ADD':
            return await self.handle_config_add_to_list(key, value)
        elif operation == 'DEL':
            return await self.handle_config_remove_from_list(key, value)
        else:
            return f"ERROR: Invalid operation for {key} (ADD/DEL required)"

    async def handle_config_add_to_list(self, key, value):
        if value not in AppState().config_store[key]:
            AppState().config_store[key].append(value)
            save_config()
            res = f"Added {value} to {key}"
        else:
            res = f"{value} already in {key}"
        if key == 'SHARDS' and await self.processor.sharding_manager.has_sharding() and await self.processor.sharding_manager.is_sharding_master():
            await self.processor.shard_handler.reshard_command()
            res += " and resharded data to shards"
        return res

    async def handle_config_remove_from_list(self, key, value):
        if value in AppState().config_store[key]:
            if key == 'SHARDS' and value == AppState().config_store.get('HOST') and AppState().config_store.get('SHARDING') == '1':
                return "ERROR: Cannot remove the host from SHARDS while SHARDING is enabled. First run the command CONFIG SET SHARDING 0"
            AppState().config_store[key].remove(value)
            save_config()
            res = f"Removed {value} from {key}"
        else:
            res = f"{value} not found in {key}"
        if key == 'SHARDS' and await self.processor.sharding_manager.has_sharding() and await self.processor.sharding_manager.is_sharding_master():
            await self.processor.shard_handler.reshard_command()
            res += " and resharded data to shards"
        return res

    async def handle_config_set_value(self, key, value):
        if AppState().config_store.get(key) == value:
            return f"{key} already set to {value}"

        AppState().config_store[key] = value
        save_config()

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
        protected_keys = ['HOST', 'PORT', 'USERNAME', 'PASSWORD', 'BACKUP_ON_SHUTDOWN', 'SCHEDULER', 'REPLICATION', 'REPLICATION_TYPE', 'REPLICATION_AUTHORIZED_SLAVES', 'SHARDING_TYPE', 'SHARDING', 'SHARDING_BATCH_SIZE', 'SHARDS']
        if key in protected_keys:
            return f"ERROR: Cannot delete essential configuration key '{key}'"
        if key in AppState().config_store:
            del AppState().config_store[key]
            save_config()
            return f"Configuration key '{key}' has been deleted"
        else:
            return f"Configuration key '{key}' does not exist"

class DataCommandHandler:
    def __init__(self, processor):
        self.processor = processor

    def keys_command(self, *args, **kwargs):
        data_store = AppState().data_store
        main_keys = list(data_store.keys())
        return json.dumps(sorted(main_keys))

    def count_command(self, args):
            parts = args.split('WHERE', 1)
            root = parts[0].strip()
            conditions = parts[1].strip() if len(parts) > 1 else ""

            # Use process_local_query to get the filtered results
            results = self.processor.query_handler.process_local_query(root, f"WHERE {conditions}" if conditions else "")

            if isinstance(results, list):
                return len(results)
            elif isinstance(results, dict):
                return len(results.keys())
            return 0

    async def set_command(self, args):
        commands = args.split('|')
        responses = []
        sharding_active = await self.processor.sharding_manager.has_sharding()
        for command in commands:
            match = re.match(r"([^ ]+) (.+)", command.strip())
            if not match:
                responses.append("ERROR: Invalid SET syntax")
                continue
            key_pattern = match.group(1)
            raw_value = match.group(2)
            if 'EXPIRE' in raw_value and not self.processor.scheduler_manager.is_scheduler_active():
                return "Scheduler is not active. Run the command CONFIG SET SCHEDULER 1 to activate"
            value, expiry = self.processor.command_utils_manager.parse_value_instructions(raw_value)
            context = self.processor.command_utils_manager.get_context_from_key(AppState().data_store, key_pattern)
            value = self.processor.command_utils_manager.handle_expression_functions(value, context)
            parts = key_pattern.split(':')
            sharding_key = ':'.join(parts[:2]) if len(parts) > 1 else parts[0]
            if '*' in parts and sharding_active:
                responses.append("ERROR: Wildcard operations are not supported in sharding mode.")
                continue
            shard_result = await self.processor.sharding_manager.check_sharding('SET', command, sharding_key)
            if shard_result != "ERROR" and shard_result != 'LOCAL':
                responses.append("OK")
                continue
            elif shard_result == "ERROR":
                responses.append("ERROR: Sharding failed")
                continue
            if '*' in parts:
                base_path = parts[:parts.index('*')]
                last_key = parts[-1]
                updated = await self.set_keys_wildcard(base_path, last_key, value, AppState().data_store)
                responses.append(f"Updated {updated} entries.")
            else:
                response = await self.set_specific_key(parts, value)
                responses.append(response)
            if expiry:
                AppState().expires_store[key_pattern] = expiry
            if await self.processor.replication_manager.has_replication_is_replication_master():
                replication_command = f"SET {':'.join(parts)} {value}"
                await self.processor.replication_manager.send_command_to_slaves(replication_command)
        return '\n'.join(responses)

    async def set_keys_wildcard(self, base_path, last_key, value, data_store):
        try:
            parsed_value = json.loads(value)
        except json.JSONDecodeError:
            parsed_value = value
        if isinstance(parsed_value, dict):
            total_updated_count = 0
            for key, val in parsed_value.items():
                nested_last_key = f"{last_key}:{key}"
                updated_count = await self.set_keys_wildcard(base_path, nested_last_key, json.dumps(val), data_store)
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
            updated_count = await recursive_set(data_store, 0)
            if updated_count > 0:
                full_key = ':'.join(base_path + [last_key])
                full_data = data_store
                await self.processor.sub_pub_manager.notify_subscribers(full_key, full_data)
            AppState().data_has_changed = True
            if not self.processor.scheduler_manager.is_scheduler_active():
                self.processor.data_manager.save_data()
            return updated_count

    async def set_specific_key(self, parts, value):
        try:
            parsed_value = json.loads(value)
            if isinstance(parsed_value, dict):
                for key, val in parsed_value.items():
                    nested_parts = parts + [key]
                    await self.set_individual_key(nested_parts, val)
                return "OK"
        except json.JSONDecodeError:
            parsed_value = value
        return await self.set_individual_key(parts, parsed_value)

    async def set_individual_key(self, parts, value):
        current = AppState().data_store
        for part in parts[:-1]:
            current = current.setdefault(part, {})
        last_key = parts[-1]
        entity_key = ':'.join(parts[:2]) if len(parts) > 2 else parts[0]
        if last_key in current and current[last_key] != value:
            await self.processor.indices_manager.update_index_on_remove(parts, last_key, current[last_key], entity_key)
        if isinstance(value, str) and value.startswith('[') and value.endswith(']'):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                pass
        current[last_key] = value
        await self.processor.indices_manager.update_index_on_add(parts, last_key, value, entity_key)
        full_key = ':'.join(parts)
        await self.processor.sub_pub_manager.notify_subscribers(full_key, current)
        AppState().data_has_changed = True
        if not self.processor.scheduler_manager.is_scheduler_active():
            self.processor.data_manager.save_data()
        return "OK"

    async def del_command(self, args):
        commands = args.split('|')
        responses = []
        sharding_active = await self.processor.sharding_manager.has_sharding()
        for command in commands:
            parts = command.strip().split(':')
            if not parts or '' in parts:
                responses.append("ERROR: Invalid DEL syntax")
                continue
            shard_key = ':'.join(parts[:2]) if len(parts) > 1 else parts[0]
            if '*' in parts and sharding_active:
                responses.append("ERROR: Wildcard deletions are not supported in sharding mode.")
                continue
            shard_result = await self.processor.sharding_manager.check_sharding('DEL', command, shard_key)
            if shard_result not in ["ERROR", "LOCAL"]:
                responses.append("OK")
                continue
            elif shard_result == "ERROR":
                responses.append(shard_result)
                continue
            if '*' in parts:
                base_path = parts[:parts.index('*')]
                last_key = parts[-1]
                deleted_count = await self.delete_keys_wildcard(base_path, last_key, AppState().data_store)
                responses.append(f"Deleted {deleted_count} entries.")
            else:
                response = await self.delete_specific_key(parts)
                responses.append(response)
            if await self.processor.replication_manager.has_replication_is_replication_master():
                await self.processor.replication_manager.send_command_to_slaves(f"DEL {command}")
        return '\n'.join(responses)

    async def delete_keys_wildcard(self, base_path, last_key, data_store):
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
            deleted_count = recursive_delete(current)
            AppState().data_has_changed = True
            if not self.processor.scheduler_manager.is_scheduler_active():
                self.processor.data_manager.save_data()
            return deleted_count
        except KeyError:
            return 0

    async def delete_specific_key(self, parts):
        try:
            current_data = AppState().data_store
            for part in parts[:-1]:
                current_data = current_data[part]
            key_to_delete = parts[-1]
            if key_to_delete in current_data:
                value_to_remove = current_data[key_to_delete]
                if isinstance(value_to_remove, dict):
                    await self.processor.indices_manager.remove_entity_from_index(parts, value_to_remove)
                else:
                    if len(parts) > 3:
                        remove_field_parts = parts[:1] + parts[2:]
                    else:
                        remove_field_parts = parts
                    await self.processor.indices_manager.remove_field_from_index(remove_field_parts[:-1], key_to_delete, value_to_remove)
                del current_data[key_to_delete]
                self.processor.command_utils_manager.cleanup_empty_dicts(AppState().data_store, parts[:-1])
                AppState().data_has_changed = True
                if not self.processor.scheduler_manager.is_scheduler_active():
                    self.processor.data_manager.save_data()
                return "OK"
            else:
                return "ERROR: Key does not exist"
        except KeyError:
            return "ERROR: Path not found"
        except Exception as e:
            return f"ERROR: Exception occurred - {str(e)}"

    async def incr_decr_command(self, args, increment):
        commands = args.split('|')
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
            shard_key = f'{keys[0]}:{keys[1]}' if len(keys) > 1 else keys[0]
            shard_command = 'INCR' if increment else 'DECR'
            shard_result = await self.processor.sharding_manager.check_sharding(shard_command, command.strip(), shard_key)
            if shard_result != "ERROR" and shard_result != 'LOCAL':
                responses.append("OK")
                continue
            elif shard_result == "ERROR":
                responses.append(shard_result)
                continue
            data = self.processor.command_utils_manager.get_nested_value(AppState().data_store, keys)
            if data is None:
                data = 0
            original_type = float if isinstance(data, float) or isinstance(amount, float) else int
            new_value = data + amount if increment else data - amount
            if original_type is int:
                new_value = int(new_value)
            new_data = self.processor.command_utils_manager.set_nested_value(AppState().data_store, keys, new_value)
            AppState().data_has_changed = True
            if not self.processor.scheduler_manager.is_scheduler_active():
                self.processor.data_manager.save_data()
            key = ":".join(keys)
            await self.processor.sub_pub_manager.notify_subscribers(key, new_data)
            responses.append("OK")
            if await self.processor.replication_manager.has_replication_is_replication_master():
                replication_command = 'INCR' if increment else 'DECR'
                await self.processor.replication_manager.send_command_to_slaves(f"{replication_command} {command}")
        return '\n'.join(responses)

    async def rename_command(self, args):
        if ' TO ' not in args:
            return "ERROR: Invalid RENAME syntax"
        command = args
        path, new_key = args.split(' TO ')
        path_parts = path.split(':')
        new_key = new_key.strip()
        sharding_key = ':'.join(path_parts[:2]) if len(path_parts) > 1 else path_parts[0]
        shard_result = await self.processor.sharding_manager.check_sharding('RENAME', path, sharding_key)
        if shard_result != "ERROR" and shard_result != 'LOCAL':
            return "OK"
        elif shard_result == "ERROR":
            return "ERROR: Sharding failed"
        if '*' in path_parts:
            base_path = path_parts[:path_parts.index('*')]
            target_key = path_parts[-1]
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
            keys_renamed = recursive_rename(AppState().data_store, 0)
            AppState().data_has_changed = True
            if not self.processor.scheduler_manager.is_scheduler_active():
                self.processor.data_manager.save_data()
            if await self.processor.replication_manager.has_replication_is_replication_master():
                await self.processor.replication_manager.send_command_to_slaves(f"RENAME {command}")
            return f"RENAME successful: {keys_renamed} keys renamed." if keys_renamed else "Nothing to rename"
        else:
            current = AppState().data_store
            for part in path_parts[:-1]:
                if part in current:
                    current = current[part]
                else:
                    return "ERROR: Path not found"
            if path_parts[-1] in current:
                current[new_key] = current.pop(path_parts[-1])
                AppState().data_has_changed = True
                if not self.processor.scheduler_manager.is_scheduler_active():
                    self.processor.data_manager.save_data()
                if await self.processor.replication_manager.has_replication_is_replication_master():
                    await self.processor.replication_manager.send_command_to_slaves(f"RENAME {command}")
                return "RENAME successful: 1 key renamed."
            else:
                return "ERROR: Key not found to rename."

    async def flush_all_command(self, *args, **kwargs):
        host = AppState().config_store.get('HOST')
        port = AppState().config_store.get('PORT')
        AppState().data_store.clear()
        AppState().indices.clear()
        self.processor.data_manager.save_data()
        self.processor.indices_manager.save_indices()
        if await self.processor.sharding_manager.has_sharding_is_sharding_master():
            shards = AppState().config_store.get('SHARDS')
            shard_uris = [f"{shard}:{port}" for shard in shards if shard != host]
            results = await self.processor.sharding_manager.broadcast_query('FLUSHALL', shard_uris)
            print("Flush results from remote shards:", results)
        return "All indices and data flushed successfully."

class QueryCommandHandler:
    def __init__(self, processor):
        self.processor = processor

    async def query_command(self, args):
        parts = args.split(' ', 1)
        root = parts[0]
        conditions = parts[1] if len(parts) > 1 else ""
        sharding_active = await self.processor.sharding_manager.has_sharding()
        if 'JOIN' in conditions and sharding_active:
            return "JOIN operations are not supported in sharding mode."
        modifiers, conditions = self.processor.command_utils_manager.parse_modifiers(conditions)
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
            local_results = self.process_local_query(root, conditions, modifiers)
            if isinstance(local_results, str):
                return local_results
            return json.dumps(local_results)
        host = AppState().config_store.get('HOST')
        port = AppState().config_store.get('PORT')
        shard_uris = [f"{shard}:{port}" for shard in AppState().config_store.get('SHARDS') if shard != host]
        remote_results = await self.processor.sharding_manager.broadcast_query(f'QUERY {root} {conditions}', shard_uris)
        local_results = self.process_local_query(root, conditions, modifiers)
        if isinstance(remote_results, list) and len(remote_results) == 1 and isinstance(remote_results[0], str):
            remote_results = remote_results[0]
        combined_results = local_results if isinstance(local_results, (str, int, float)) else local_results + remote_results
        final_results = self.processor.command_utils_manager.apply_query_modifiers(combined_results, modifiers)
        if isinstance(final_results, (str, int, float)):
            return str(final_results)
        return json.dumps(final_results)

    def process_local_query(self, root, conditions, modifiers=None):
        key_parts = root.split(':')
        main_key = key_parts[0]
        specific_key = key_parts[1] if len(key_parts) > 1 else None
        sub_key_path = key_parts[2:] if len(key_parts) > 2 else []
        if conditions.startswith("WHERE "):
            conditions = conditions[6:].strip()
        include_fields, exclude_fields, conditions = self.processor.command_utils_manager.parse_and_clean_fields(conditions)
        joins, conditions = self.processor.command_utils_manager.extract_join_clauses(conditions)
        data_to_query = AppState().data_store.get(main_key, {})
        if specific_key:
            specific_entry = data_to_query.get(specific_key)
            if isinstance(specific_entry, dict) and sub_key_path:
                for key in sub_key_path:
                    if key in specific_entry:
                        specific_entry = specific_entry[key]
                    else:
                        break
            if not isinstance(specific_entry, dict):
                specific_entry = self.processor.data_manager.process_nested_data(specific_entry)
            for join_table, join_key in joins:
                join_value = str(specific_entry.get(join_key))
                join_ids = AppState().indices.get(join_table, {}).get(join_key, {}).get(join_value, [])
                joined_data = [
                    {'key': jid.split(':')[1], **AppState().data_store[join_table][jid.split(':')[1]]}
                    for jid in join_ids if jid.split(':')[1] in AppState().data_store[join_table]
                ]
                specific_entry.setdefault(join_table, []).extend(joined_data)
            results = self.processor.command_utils_manager.format_as_list(specific_entry)
        elif conditions:
            results = self.processor.command_utils_manager.eval_conditions_using_indices(conditions, AppState().indices, main_key, joins)
            results = self.processor.command_utils_manager.format_as_list(results)
        else:
            results = self.processor.command_utils_manager.format_as_list(self.processor.data_manager.process_nested_data(data_to_query))
        if modifiers:
            results = self.processor.command_utils_manager.apply_query_modifiers(results, modifiers)
        results = [self.processor.command_utils_manager.filter_fields(entry, include_fields, exclude_fields) for entry in results]
        return results

class ShardCommandHandler:
    def __init__(self, processor):
        self.processor = processor

    async def replicate_command(self, *args, **kwargs):
        websocket = AppState().websocket
        if await self.processor.replication_manager.has_replication_is_replication_master():
            data = json.dumps(AppState().data_store)
            indices = json.dumps(AppState().indices)
            CHUNK_SIZE = 5000
            data_chunks = [data[i:i+CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]
            indices_chunks = [indices[i:i+CHUNK_SIZE] for i in range(0, len(indices), CHUNK_SIZE)]
            for data_chunk in data_chunks:
                await websocket.send(json.dumps({'data_chunks': [data_chunk], 'indices_chunks': []}))
            for indices_chunk in indices_chunks:
                await websocket.send(json.dumps({'data_chunks': [], 'indices_chunks': [indices_chunk]}))
            await websocket.send('DONE')

    async def reshard_command(self, *args, **kwargs):
        AppState().data_has_changed = True
        AppState().indices_has_changed = True
        self.processor.data_manager.save_data()
        self.processor.indices_manager.save_indices()
        self.processor.backup_manager.backup_data()
        if await self.processor.sharding_manager.is_sharding_master():
            return json.dumps(await self.processor.sharding_manager.reshard())
        else:
            local_data = self.processor.data_manager.get_all_local_data()
            local_indices = self.processor.indices_manager.get_all_local_indices()
            AppState().data_store.clear()
            AppState().indices.clear()
            AppState().data_has_changed = True
            AppState().indices_has_changed = True
            self.processor.data_manager.save_data()
            self.processor.indices_manager.save_indices()
            prepared_data = self.processor.sharding_manager.prepare_data_for_transmission(local_data)
            prepared_indices = self.processor.sharding_manager.prepare_data_for_transmission(local_indices)
            response = {
                "local_data": prepared_data,
                "local_indices": prepared_indices
            }
            return json.dumps(response)