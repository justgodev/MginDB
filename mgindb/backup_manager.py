from .app_state import AppState
import os
import shutil
import datetime
import json
from .constants import DATA_FILE, INDICES_FILE, SCHEDULER_FILE, BACKUP_DIR

from .data_manager import DataManager  # Importing data management functions from data_manager module
data_manager = DataManager()

from .indices_manager import IndicesManager  # Importing function to load indices
indices_manager = IndicesManager() # Importing index management functions from indices_manager module

from .sharding_manager import ShardingManager  # Importing data management functions from sharding_manager module
sharding_manager = ShardingManager()

class BackupManager:
    def __init__(self):
        self.app_state = AppState()
        self.data_file = DATA_FILE
        self.indice_file = INDICES_FILE
        self.scheduler_file = SCHEDULER_FILE

    def handle_backup_command(self, args):
        if args.upper() == 'LIST':
            return self.backups_list()
        elif args.upper().startswith('RESTORE'):
            filename = args.split(maxsplit=1)[1] if len(args.split(maxsplit=1)) > 1 else ""
            return self.backup_restore(filename)
        elif args.upper().startswith('DEL'):
            subcommand = args.split(maxsplit=1)[1].upper()
            if subcommand == 'ALL':
                return self.backup_delete_all_files()
            else:
                filename = args.split(maxsplit=1)[1] if len(args.split(maxsplit=1)) > 1 else ""
                return self.backup_delete_file(filename)
        else:
            return self.backup_data()

    def get_latest_backup(self):
        try:
            backups = [file for file in os.listdir(BACKUP_DIR) if file.endswith('.backup')]
            backups.sort(key=lambda x: datetime.datetime.strptime(x.split('_')[1].split('.')[0], '%Y%m%d%H%M%S'), reverse=True)
            latest_data_backup = next((file for file in backups if file.startswith('data_')), None)
            latest_indices_backup = next((file for file in backups if file.startswith('indices_')), None)
            latest_scheduler_backup = next((file for file in backups if file.startswith('scheduler_')), None)
            return latest_data_backup, latest_indices_backup, latest_scheduler_backup
        except FileNotFoundError:
            return None, None

    def backups_list(self):
        try:
            backups = [file for file in os.listdir(BACKUP_DIR) if file.endswith('.backup')]
            backups.sort(key=lambda x: datetime.datetime.strptime(x.split('_')[1].split('.')[0], '%Y%m%d%H%M%S'), reverse=True)
            
            backup_info = []  # Initialize an empty list to store backup information
            for file in backups:
                backup_date = datetime.datetime.strptime(file.split('_')[1].split('.')[0], '%Y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')
                backup_info.append({"file": file, "date": backup_date})  # Append backup information as dictionary
            
            if backup_info:
                return json.dumps(backup_info)  # Return JSON string with indentation for readability
            else:
                return json.dumps([{"message": "No backup files found."}])  # Return JSON string with a single element indicating the message
        except FileNotFoundError:
            return json.dumps([{"message": "Backup directory not found."}])  # Return JSON string with a single element indicating the message

    def backup_data(self):
        current_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        results = []  # List to keep track of backup messages for each type

        # Backup data file if it exists and is not empty
        if os.path.exists(self.data_file) and self.app_state.data_store:
            backup_filename_data = f"data_{current_time}.backup"
            backup_path_data = os.path.join(BACKUP_DIR, backup_filename_data)
            # Convert all sets to lists in the data store before backup
            serializable_data_store = data_manager.convert_sets_to_lists(self.app_state.data_store)
            try:
                with open(backup_path_data, 'w', encoding='utf-8') as backup_file:
                    json.dump(serializable_data_store, backup_file, indent=4)
                results.append("Data backup completed successfully.")
            except IOError as e:
                results.append(f"Failed to backup data file: {e}")

        # Similar conversion for indices and scheduler
        if os.path.exists(self.indice_file) and self.app_state.indices:
            backup_filename_indices = f"indices_{current_time}.backup"
            backup_path_indices = os.path.join(BACKUP_DIR, backup_filename_indices)
            serializable_indices = data_manager.convert_sets_to_lists(self.app_state.indices)
            try:
                with open(backup_path_indices, 'w', encoding='utf-8') as backup_file:
                    json.dump(serializable_indices, backup_file, indent=4)
                results.append("Indices backup completed successfully.")
            except IOError as e:
                results.append(f"Failed to backup indices file: {e}")

        if os.path.exists(self.scheduler_file) and self.app_state.scheduled_tasks:
            backup_filename_scheduler = f"scheduler_{current_time}.backup"
            backup_path_scheduler = os.path.join(BACKUP_DIR, backup_filename_scheduler)
            serializable_scheduler = data_manager.convert_sets_to_lists(self.app_state.scheduled_tasks)
            try:
                with open(backup_path_scheduler, 'w', encoding='utf-8') as backup_file:
                    json.dump(serializable_scheduler, backup_file, indent=4)
                results.append("Scheduler backup completed successfully.")
            except IOError as e:
                results.append(f"Failed to backup scheduler file: {e}")

        if not results:
            return "No files to backup or data is empty."
        else:
            return '\n'.join(results)

    def backup_restore(self, filename):
        backup_path = os.path.join(BACKUP_DIR, filename)
        if not os.path.exists(backup_path):
            return f"ERROR: Backup file {filename} does not exist"

        if 'data_' in filename:
            target_file = self.data_file
        elif 'indices_' in filename:
            target_file = self.indice_file
        elif 'scheduler_' in filename:
            target_file = self.scheduler_file
        else:
            return "ERROR: Unknown file type for restore"

        shutil.copyfile(backup_path, target_file)

        if target_file == self.data_file:
            data_manager.load_data()
        elif target_file == self.indice_file:
            indices_manager.load_indices()

        return "Restore completed successfully. Data reloaded."

    async def backup_rollback(self):
        # Find the latest backups for data and indices
        latest_data_backup, latest_indices_backup, latest_scheduler_backup = self.get_latest_backup()

        if not latest_data_backup or not latest_indices_backup or not latest_scheduler_backup:
            #print("ERROR: No backup files available for rollback.")
            return "ERROR: No backup files available for rollback."

        # Restore the latest backups
        data_restore_result = self.backup_restore(latest_data_backup)
        indices_restore_result = self.backup_restore(latest_indices_backup)
        scheduler_restore_result = self.backup_restore(latest_scheduler_backup)

        #print(f"Rollback results - Data: {data_restore_result}, Indices: {indices_restore_result}, Scheduler: {scheduler_restore_result}")
        return f"Rollback completed - Data: {data_restore_result}, Indices: {indices_restore_result}"

    def backup_delete_file(self, filename):
        backup_path = os.path.join(BACKUP_DIR, filename)
        if os.path.exists(backup_path):
            os.remove(backup_path)
            return f"Backup file {filename} has been deleted."
        else:
            return f"Backup file {filename} does not exist."

    async def backup_delete_all_files(self):
        host = AppState().config_store.get('HOST')
        port = AppState().config_store.get('PORT')

        # Perform local deletion
        deleted_files = 0
        try:
            for file in os.listdir(BACKUP_DIR):
                if file.endswith('.backup'):
                    os.remove(os.path.join(BACKUP_DIR, file))
                    deleted_files += 1
        except FileNotFoundError:
            return "Backup directory not found."

        # Check if sharding is enabled and we are in MASTER mode
        if await sharding_manager.has_sharding_is_sharding_master():
            shards = AppState().config_store.get('SHARDS')
            shard_uris = [f"{shard}:{port}" for shard in shards if shard != host]

            # Broadcast DELETE BACKUP to all remote shards
            results = await sharding_manager.broadcast_query('BACKUP DEL ALL', shard_uris)
            #print("Delete backup results from remote shards:", results)
            deleted_files += sum(int(result.split()[0]) for result in results if "backup files have been deleted." in result)

        if deleted_files > 0:
            return f"{deleted_files} backup files have been deleted."
        else:
            return "No backup files found to delete."
