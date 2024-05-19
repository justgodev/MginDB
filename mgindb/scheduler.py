import json
import os
import re
import time
from .app_state import AppState
from .connection_handler import asyncio
from croniter import croniter
from datetime import datetime, timedelta
from .constants import SCHEDULER_FILE
from .data_manager import DataManager
from .indices_manager import IndicesManager

# Instantiate managers
data_manager = DataManager()
indices_manager = IndicesManager()

class SchedulerManager:
    def __init__(self):
        self.app_state = AppState()
        self.scheduler_file = SCHEDULER_FILE
        self.scheduler_tasks = SchedulerTasks(self.app_state)

    def is_scheduler_running(self):
        task = self.app_state.scheduler_task
        return task and not task.done()

    def is_scheduler_active(self):
        return self.app_state.config_store.get('SCHEDULER', '0') == '1'

    async def start_scheduler(self):
        if not self.is_scheduler_running():
            self.app_state.scheduler_task = asyncio.create_task(self.scheduler_tasks.run_scheduler())
            print("Scheduler has started.")
            return "Scheduler has started."

    async def stop_scheduler(self):
        if self.is_scheduler_running():
            self.app_state.scheduler_task.cancel()
            print("Scheduler has stopped.")
            return "Scheduler has stopped."

    def schedule_command(self, args):
        if not self.is_scheduler_active():
            return "Scheduler is not active. Run the command CONFIG SET SCHEDULER 1 to activate"

        return self.scheduler_tasks.schedule_command(args)

    def load_scheduler(self):
        self.scheduler_tasks.load_scheduler()

    def save_scheduler(self):
        self.scheduler_tasks.save_scheduler()

class SchedulerTasks:
    def __init__(self, app_state):
        self.app_state = app_state

    def calculate_next_run(self, schedule, last_run_time):
        cron = croniter(schedule, last_run_time)
        return cron.get_next(datetime)

    async def run_scheduler(self):
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

            # Cleanup expired keys
            await data_manager.cleanup_expired_keys()

            # Save on File
            save_timer += 1
            if save_timer >= save_interval:
                data_manager.save_data()
                indices_manager.save_indices()
                save_timer = 0

            await asyncio.sleep(1)

    def load_scheduler(self):
        if not os.path.exists(SCHEDULER_FILE):
            with open(SCHEDULER_FILE, mode='w', encoding='utf-8') as file:
                json.dump({}, file)
        try:
            with open(SCHEDULER_FILE, mode='r', encoding='utf-8') as file:
                loaded_data = json.load(file)
            self.app_state.scheduled_tasks.update(loaded_data)
        except json.JSONDecodeError as e:
            # Log the error
            print(f"Failed to load data: {e}")
            return {}

    def save_scheduler(self):
        try:
            with open(SCHEDULER_FILE, mode='w', encoding='utf-8') as file:
                json.dump(self.app_state.scheduled_tasks, file)
        except IOError as e:
            # Log the error
            print(f"Failed to save data: {e}")

    def schedule_command(self, args):
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
        cron_regex = r'^(\*|([0-5]?\d))(\s+(\*|([01]?\d|2[0-3])))(\s+(\*|([1-9]|[12]\d|3[01])))(\s+(\*|(1[0-2]|0?[1-9])))(\s+(\*|([0-6]|\?)))(\s+(\*|\d{4}))?$'
        return re.match(cron_regex, detail.strip()) is not None

    def show_all_schedules(self):
        if not self.app_state.scheduled_tasks:
            return None
        schedule_info = json.dumps(self.app_state.scheduled_tasks)
        return schedule_info

    def show_specific_schedule(self, schedule):
        tasks = self.app_state.scheduled_tasks.get(schedule, None)
        if not tasks:
            return None
        tasks_info = json.dumps(tasks, indent=4)
        return tasks_info

    def find_task_by_key(self, key):
        found_tasks = {}
        for schedule, tasks in self.app_state.scheduled_tasks.items():
            if key in tasks:
                found_tasks[schedule] = {key: tasks[key]}

        if not found_tasks:
            return f"No tasks found for key {key}."
        tasks_info = json.dumps(found_tasks, indent=4)
        return tasks_info

    def add_schedule_task(self, details):
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
        self.app_state.scheduled_tasks.clear()
        self.save_scheduler()
        return "OK"

    def flush_specific_schedule(self, schedule):
        if schedule in self.app_state.scheduled_tasks:
            del self.app_state.scheduled_tasks[schedule]
            self.save_scheduler()
            return "OK"
        return None

    async def execute_task(self, key, task):
        from .command_processing import process_command
        return await process_command(task['command'])
