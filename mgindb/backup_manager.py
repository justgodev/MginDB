# Import necessary modules and classes
from .app_state import AppState  # Application state management
import os  # Module for interacting with the operating system
import shutil  # Module for file operations
import datetime  # Module for date and time manipulation
import json  # Module for JSON operations
from .constants import DATA_FILE, INDICES_FILE, SCHEDULER_FILE, BACKUP_DIR  # Import constants

from .data_manager import DataManager  # Importing data management functions from data_manager module
data_manager = DataManager()  # Instantiate DataManager

from .indices_manager import IndicesManager  # Importing function to load indices
indices_manager = IndicesManager()  # Instantiate IndicesManager

from .sharding_manager import ShardingManager  # Importing data management functions from sharding_manager module
sharding_manager = ShardingManager()  # Instantiate ShardingManager

class BackupManager:
    def __init__(self):
        # Initialize application state and file paths
        self.app_state = AppState()  # Manages application state
        self.data_file = DATA_FILE  # Path to the data file
        self.indice_file = INDICES_FILE  # Path to the indices file
        self.scheduler_file = SCHEDULER_FILE  # Path to the scheduler file

    def handle_backup_command(self, args):
        """
        Handles backup-related commands based on the provided arguments.

        This function processes backup commands such as listing backups, restoring a backup,
        deleting a backup, and performing a new backup. The specific command is determined by
        the provided arguments.

        Args:
            args (str): The backup command and its arguments.

        Returns:
            str: The result of the backup command.
        """
        if args.upper() == 'LIST':
            return self.backups_list()  # List all backup files
        elif args.upper().startswith('RESTORE'):
            filename = args.split(maxsplit=1)[1] if len(args.split(maxsplit=1)) > 1 else ""
            return self.backup_restore(filename)  # Restore a specific backup file
        elif args.upper().startswith('DEL'):
            subcommand = args.split(maxsplit=1)[1].upper()
            if subcommand == 'ALL':
                return self.backup_delete_all_files()  # Delete all backup files
            else:
                filename = args.split(maxsplit=1)[1] if len(args.split(maxsplit=1)) > 1 else ""
                return self.backup_delete_file(filename)  # Delete a specific backup file
        else:
            return self.backup_data()  # Perform a backup

    def get_latest_backup(self):
        """
        Retrieves the latest backup files for data, indices, and scheduler.

        This function searches the backup directory for the latest backup files
        for data, indices, and scheduler based on their filenames and timestamps.

        Returns:
            tuple: The filenames of the latest data, indices, and scheduler backups.
        """
        try:
            backups = [file for file in os.listdir(BACKUP_DIR) if file.endswith('.backup')]
            backups.sort(key=lambda x: datetime.datetime.strptime(x.split('_')[1].split('.')[0], '%Y%m%d%H%M%S'), reverse=True)
            latest_data_backup = next((file for file in backups if file.startswith('data_')), None)
            latest_indices_backup = next((file for file in backups if file.startswith('indices_')), None)
            latest_scheduler_backup = next((file for file in backups if file.startswith('scheduler_')), None)
            return latest_data_backup, latest_indices_backup, latest_scheduler_backup
        except FileNotFoundError:
            return None, None, None  # Return None if the backup directory is not found

    def backups_list(self):
        """
        Lists all backup files in the backup directory.

        This function generates a list of all backup files in the backup directory,
        including their filenames and the dates they were created.

        Returns:
            str: A JSON string containing the list of backup files and their dates.
        """
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
        """
        Performs a backup of data, indices, and scheduler files.

        This function creates backups of the data, indices, and scheduler files,
        storing them in the backup directory with filenames that include the
        current timestamp.

        Returns:
            str: A message indicating the result of the backup operation.
        """
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

        # Backup indices file if it exists and is not empty
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

        # Backup scheduler file if it exists and is not empty
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
            return "No files to backup or data is empty."  # Return message if no files were backed up
        else:
            return '\n'.join(results)  # Join and return backup messages

    def backup_restore(self, filename):
        """
        Restores a backup file based on the provided filename.

        This function restores a backup file (data, indices, or scheduler) by copying
        it from the backup directory to its original location and reloading the data
        if applicable.

        Args:
            filename (str): The name of the backup file to restore.

        Returns:
            str: A message indicating the result of the restore operation.
        """
        backup_path = os.path.join(BACKUP_DIR, filename)  # Construct the full backup file path
        if not os.path.exists(backup_path):
            return f"ERROR: Backup file {filename} does not exist"  # Return error if the backup file does not exist

        # Determine the target file based on the backup filename
        if 'data_' in filename:
            target_file = self.data_file
        elif 'indices_' in filename:
            target_file = self.indice_file
        elif 'scheduler_' in filename:
            target_file = self.scheduler_file
        else:
            return "ERROR: Unknown file type for restore"  # Return error if the file type is unknown

        shutil.copyfile(backup_path, target_file)  # Copy the backup file to the target file

        # Reload data or indices if applicable
        if target_file == self.data_file:
            data_manager.load_data()  # Reload data
        elif target_file == self.indice_file:
            indices_manager.load_indices()  # Reload indices

        return "Restore completed successfully. Data reloaded."  # Return success message

    async def backup_rollback(self):
        """
        Rolls back to the latest backup files.

        This function restores the latest backups for data, indices, and scheduler files,
        effectively rolling back the application state to the last known good state.

        Returns:
            str: A message indicating the result of the rollback operation.
        """
        # Find the latest backups for data, indices, and scheduler
        latest_data_backup, latest_indices_backup, latest_scheduler_backup = self.get_latest_backup()

        if not latest_data_backup or not latest_indices_backup or not latest_scheduler_backup:
            return "ERROR: No backup files available for rollback."  # Return error if no backup files are found

        # Restore the latest backups
        data_restore_result = self.backup_restore(latest_data_backup)
        indices_restore_result = self.backup_restore(latest_indices_backup)
        scheduler_restore_result = self.backup_restore(latest_scheduler_backup)

        return f"Rollback completed - Data: {data_restore_result}, Indices: {indices_restore_result}, Scheduler: {scheduler_restore_result}"  # Return rollback results

    def backup_delete_file(self, filename):
        """
        Deletes a specific backup file.

        This function deletes a specific backup file from the backup directory.

        Args:
            filename (str): The name of the backup file to delete.

        Returns:
            str: A message indicating the result of the delete operation.
        """
        backup_path = os.path.join(BACKUP_DIR, filename)  # Construct the full backup file path
        if os.path.exists(backup_path):
            os.remove(backup_path)  # Remove the backup file
            return f"Backup file {filename} has been deleted."  # Return success message
        else:
            return f"Backup file {filename} does not exist."  # Return error message if file does not exist

    async def backup_delete_all_files(self):
        """
        Deletes all backup files, including those on remote shards if sharding is enabled.

        This function deletes all backup files from the backup directory. If sharding is enabled
        and the current instance is a sharding master, it also deletes backup files from remote shards.

        Returns:
            str: A message indicating the result of the delete operation.
        """
        host = AppState().config_store.get('HOST')  # Get host from config
        port = AppState().config_store.get('PORT')  # Get port from config

        # Perform local deletion
        deleted_files = 0
        try:
            for file in os.listdir(BACKUP_DIR):
                if file.endswith('.backup'):
                    os.remove(os.path.join(BACKUP_DIR, file))  # Remove the backup file
                    deleted_files += 1  # Increment deleted files counter
        except FileNotFoundError:
            return "Backup directory not found."  # Return error if backup directory is not found

        # Check if sharding is enabled and we are in MASTER mode
        if await sharding_manager.has_sharding_is_sharding_master():
            shards = AppState().config_store.get('SHARDS')  # Get list of shards from config
            shard_uris = [f"{shard}:{port}" for shard in shards if shard != host]  # Create URIs for shards

            # Broadcast DELETE BACKUP to all remote shards
            results = await sharding_manager.broadcast_query('BACKUP DEL ALL', shard_uris)
            deleted_files += sum(int(result.split()[0]) for result in results if "backup files have been deleted." in result)

        if deleted_files > 0:
            return f"{deleted_files} backup files have been deleted."  # Return success message with count
        else:
            return "No backup files found to delete."  # Return message if no files were deleted