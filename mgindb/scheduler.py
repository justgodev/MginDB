import ujson  # Module for JSON operations
import os  # Module for interacting with the operating system
import re  # Module for regular expressions
import time  # Module for time-related functions
from datetime import datetime, timedelta  # Modules for date and time manipulation
from croniter import croniter  # Module for parsing cron schedules
from .app_state import AppState  # Importing AppState class from app_state module
from .connection_handler import asyncio  # Importing asyncio from connection_handler module
from .constants import SCHEDULER_FILE  # Importing SCHEDULER_FILE constant from constants module
from .data_manager import DataManager  # Importing DataManager class
from .indices_manager import IndicesManager  # Importing IndicesManager class
from .cache_manager import CacheManager  # Importing CacheManager class

# Instantiate managers
data_manager = DataManager()
indices_manager = IndicesManager()
cache_manager = CacheManager()

class SchedulerManager:
    def __init__(self):
        """Initialize SchedulerManager with application state and scheduler tasks."""
        self.app_state = AppState()
        self.scheduler_file = SCHEDULER_FILE
        self.scheduler_tasks = SchedulerTasks(self.app_state)

    def is_scheduler_running(self):
        """Check if the scheduler task is running."""
        task = self.app_state.scheduler_task
        return task and not task.done()

    def is_scheduler_active(self):
        """Check if the scheduler is active based on configuration."""
        return self.app_state.config_store.get('SCHEDULER', '0') == '1'

    async def start_scheduler(self):
        """Start the scheduler if it is not already running."""
        if not self.is_scheduler_running():
            self.app_state.scheduler_task = asyncio.create_task(self.scheduler_tasks.run_scheduler())
            print("Scheduler has started...")
            return "Scheduler has started..."

    async def stop_scheduler(self):
        """Stop the scheduler if it is running."""
        if self.is_scheduler_running():
            self.app_state.scheduler_task.cancel()
            print("Scheduler has stopped...")
            return "Scheduler has stopped..."

    def schedule_command(self, args):
        """Handle schedule-related commands."""
        if not self.is_scheduler_active():
            return "Scheduler is not active. Run the command CONFIG SET SCHEDULER 1 to activate"

        return self.scheduler_tasks.schedule_command(args)

    def load_scheduler(self):
        """Load the scheduler tasks from the scheduler file."""
        self.scheduler_tasks.load_scheduler()

    def save_scheduler(self):
        """Save the current scheduler tasks to the scheduler file."""
        self.scheduler_tasks.save_scheduler()

class SchedulerTasks:
    def __init__(self, app_state):
        """Initialize SchedulerTasks with application state."""
        self.app_state = app_state

    def calculate_next_run(self, schedule, last_run_time):
        """
        Calculate the next run time for a given cron schedule.

        Args:
            schedule (str): The cron schedule.
            last_run_time (datetime): The last run time.

        Returns:
            datetime: The next run time.
        """
        cron = croniter(schedule, last_run_time)
        return cron.get_next(datetime)

    async def run_scheduler(self):
        """
        Run the scheduler to execute tasks based on their schedules.

        This function runs in an infinite loop, periodically checking and executing
        scheduled tasks. It also handles saving data and indices at specified intervals.
        """
        leeway = timedelta(seconds=1)
        save_interval = int(self.app_state.config_store.get('SAVE_ON_FILE_INTERVAL'))
        save_timer = 0
        task_execution_count = 0
        tasks_per_save = 1

        while True:
            current_time = datetime.now()
            for schedule, tasks in self.app_state.scheduled_tasks.items():
                for key, task in tasks.items():
                    if task['next'] == 0:  # Initialize the next run time if not set
                        task['next'] = self.calculate_next_run(schedule, datetime.now()).timestamp()
                    if current_time.timestamp() >= task['next']:
                        await self.execute_task(key, task)
                        task['last'] = current_time.timestamp()
                        task['next'] = self.calculate_next_run(schedule, datetime.fromtimestamp(task['last'])).timestamp()
                        print(f"Task {key} executed. Next run at {datetime.fromtimestamp(task['next'])}")
                        task_execution_count += 1

            if task_execution_count >= tasks_per_save:
                self.save_scheduler()
                task_execution_count = 0

            # Cleanup expired keys and entries
            await data_manager.cleanup_expired_keys()
            await cache_manager.cleanup_expired_entries()

            # Save on File
            save_timer += 1
            if save_timer >= save_interval:
                data_manager.save_data()
                indices_manager.save_indices()
                save_timer = 0

            await asyncio.sleep(1)

    def load_scheduler(self):
        """
        Load the scheduler tasks from the scheduler file.

        If the scheduler file does not exist, create it and write an empty dictionary to it.
        """
        if not os.path.exists(SCHEDULER_FILE):
            with open(SCHEDULER_FILE, mode='w', encoding='utf-8') as file:
                ujson.dump({}, file)
        try:
            with open(SCHEDULER_FILE, mode='r', encoding='utf-8') as file:
                loaded_data = ujson.load(file)
            self.app_state.scheduled_tasks.update(loaded_data)
        except ujson.JSONDecodeError as e:
            print(f"Failed to load scheduler data: {e}")
            return {}

    def save_scheduler(self):
        """Save the current scheduler tasks to the scheduler file."""
        try:
            with open(SCHEDULER_FILE, mode='w', encoding='utf-8') as file:
                ujson.dump(self.app_state.scheduled_tasks, file)
        except IOError as e:
            print(f"Failed to save scheduler data: {e}")

    def schedule_command(self, args):
        """
        Handle the SCHEDULE command with various actions.

        Args:
            args (str): The arguments for the SCHEDULE command.

        Returns:
            str: The result of the SCHEDULE command.
        """
        parts = args.split(maxsplit=1)
        if not parts or len(parts) < 2:
            return "ERROR: Missing arguments for SCHEDULE command"

        action = parts[0].upper()
        details = parts[1]

        if action == 'SHOW':
            if len(parts) < 2:
                return "ERROR: Missing details for SHOW"
            detail = parts[1].strip()
            if detail.upper() == 'ALL':
                return self.show_all_schedules()
            elif self.is_cron_format(detail):
                return self.show_specific_schedule(detail)
            else:
                return self.find_task_by_key(detail)
        elif action == 'ADD':
            return self.add_schedule_task(details)
        elif action == 'DEL':
            return self.delete_schedule_task(details)
        elif action == 'FLUSH':
            if len(parts) < 2:
                return "ERROR: Missing details for FLUSH"
            detail = parts[1].strip().upper()
            if detail == 'ALL':
                return self.flush_all_schedules()
            else:
                return self.flush_specific_schedule(detail)
        else:
            return "ERROR: Invalid SCHEDULE command"

    def is_cron_format(self, detail):
        """
        Check if a given detail string is in cron format.

        Args:
            detail (str): The detail string.

        Returns:
            bool: True if the detail string is in cron format, False otherwise.
        """
        cron_regex = r'^(\*|([0-5]?\d))(\s+(\*|([01]?\d|2[0-3])))(\s+(\*|([1-9]|[12]\d|3[01])))(\s+(\*|(1[0-2]|0?[1-9])))(\s+(\*|([0-6]|\?)))(\s+(\*|\d{4}))?$'
        return re.match(cron_regex, detail.strip()) is not None

    def show_all_schedules(self):
        """Show all scheduled tasks."""
        if not self.app_state.scheduled_tasks:
            return None
        schedule_info = ujson.dumps(self.app_state.scheduled_tasks)
        return schedule_info

    def show_specific_schedule(self, schedule):
        """
        Show tasks for a specific schedule.

        Args:
            schedule (str): The cron schedule.

        Returns:
            str: The tasks for the specific schedule in JSON format.
        """
        tasks = self.app_state.scheduled_tasks.get(schedule, None)
        if not tasks:
            return None
        tasks_info = ujson.dumps(tasks, indent=4)
        return tasks_info

    def find_task_by_key(self, key):
        """
        Find tasks by their key.

        Args:
            key (str): The key to search for.

        Returns:
            str: The tasks matching the key in JSON format, or an error message if not found.
        """
        found_tasks = {}
        for schedule, tasks in self.app_state.scheduled_tasks.items():
            if key in tasks:
                found_tasks[schedule] = {key: tasks[key]}

        if not found_tasks:
            return f"No tasks found for key {key}."
        tasks_info = ujson.dumps(found_tasks, indent=4)
        return tasks_info

    def add_schedule_task(self, details):
        """
        Add a new scheduled task.

        Args:
            details (str): The details of the new task.

        Returns:
            str: The result of the addition.
        """
        try:
            schedule_part, command_part = details.split(' COMMAND(', 1)
            schedule = schedule_part.strip()
            command = command_part[:-1]  # Remove trailing ')'
            command_elements = command.split()

            if len(command_elements) < 2:
                return "ERROR: Command format incorrect, missing key"

            key = command_elements[1]

            if schedule not in self.app_state.scheduled_tasks:
                self.app_state.scheduled_tasks[schedule] = {}

            self.app_state.scheduled_tasks[schedule][key] = {
                "command": command,
                "last": 0,
                "next": self.calculate_next_run(schedule, datetime.now()).timestamp()
            }

            self.save_scheduler()
            return "OK"
        except Exception as e:
            return f"ERROR: Failed to add task - {str(e)}"

    def delete_schedule_task(self, details):
        """
        Delete a scheduled task.

        Args:
            details (str): The details of the task to delete.

        Returns:
            str: The result of the deletion.
        """
        key = details.strip()
        found = False

        for schedule, tasks in list(self.app_state.scheduled_tasks.items()):
            if key in tasks:
                del tasks[key]
                if not tasks:
                    del self.app_state.scheduled_tasks[schedule]
                found = True
                break

        if found:
            self.save_scheduler()
            return "OK"
        else:
            return f"ERROR: Task with key {key} not found"

    def flush_all_schedules(self):
        """Flush all scheduled tasks."""
        self.app_state.scheduled_tasks.clear()
        self.save_scheduler()
        return "OK"

    def flush_specific_schedule(self, schedule):
        """
        Flush tasks for a specific schedule.

        Args:
            schedule (str): The cron schedule.

        Returns:
            str: The result of the flush.
        """
        if schedule in self.app_state.scheduled_tasks:
            del self.app_state.scheduled_tasks[schedule]
            self.save_scheduler()
            return "OK"
        return None

    async def execute_task(self, key, task):
        """
        Execute a scheduled task.

        Args:
            key (str): The key of the task.
            task (dict): The task details.

        Returns:
            str: The result of the task execution.
        """
        from .command_processing import process_command
        return await process_command(task['command'])