import asyncio
import websockets
import signal
from .app_state import AppState
from .scheduler import SchedulerManager
from .backup_manager import BackupManager
from .data_manager import DataManager
from .indices_manager import IndicesManager
from .replication_manager import ReplicationManager

# Instantiate managers
scheduler_manager = SchedulerManager()
backup_manager = BackupManager()
data_manager = DataManager()
indices_manager = IndicesManager()
replication_manager = ReplicationManager()

# Event to signal stopping the server
stop_event = asyncio.Event()

def signal_handler(sig, frame):
    """Signal handler to initiate shutdown."""
    asyncio.create_task(signal_stop())

async def signal_stop():
    """Asynchronous function to handle server shutdown procedures."""
    print("Initiating shutdown...")

    # Mark data and indices as changed
    AppState().data_has_changed = True
    AppState().indices_has_changed = True

    # Save changes to data, indices, and scheduler
    print("Saving data...")
    data_manager.save_data()

    print("Saving indices...")
    indices_manager.save_indices()

    print("Saving scheduler...")
    scheduler_manager.save_scheduler()

    # Handle replication shutdown for master and slave
    if await replication_manager.has_replication_is_replication_master():
        print("Stopping master replication...")
    
    if await replication_manager.has_replication_is_replication_slave():
        print("Stopping slave replication...")

    # Perform backup if enabled in configuration
    if AppState().config_store.get('BACKUP_ON_SHUTDOWN') == "1":
        print("Initiating backup...")
        backup_response = await backup_manager.backup_data()
        print(backup_response)

    # Close all active WebSocket sessions
    print("Closing websocket sessions...")
    session_keys = list(AppState().sessions.keys())
    for sid in session_keys:
        session = AppState().sessions.get(sid)
        if session and session['websocket'].open:
            await session['websocket'].close(code=1001, reason='Server shutdown')

    # Stop the scheduler if it is active
    if scheduler_manager.is_scheduler_active():
        print("Stopping scheduler...")
        await scheduler_manager.stop_scheduler()

    # Set the stop event to signal the server shutdown
    stop_event.set()
    print("Server has shutdown!")