import asyncio  # Module for asynchronous programming
import websockets  # WebSocket client and server library for asyncio
import signal  # Module for signal handling
from .app_state import AppState  # Application state management
from .scheduler import SchedulerManager  # Scheduler management
from .backup_manager import BackupManager  # Backup management
from .data_manager import DataManager  # Data management
from .indices_manager import IndicesManager  # Indices management
from .replication_manager import ReplicationManager  # Replication management
from .blockchain_manager import BlockchainManager  # Blockchain management

# Instantiate managers
scheduler_manager = SchedulerManager()
backup_manager = BackupManager()
data_manager = DataManager()
indices_manager = IndicesManager()
replication_manager = ReplicationManager()
blockchain_manager = BlockchainManager()

# Event to signal stopping the server
stop_event = asyncio.Event()

def signal_handler(sig, frame):
    """
    Signal handler to initiate shutdown.

    This function is called when a signal (e.g., SIGINT or SIGTERM) is received.
    It creates an asyncio task to handle the server shutdown procedures.
    """
    asyncio.create_task(signal_stop())

async def signal_stop():
    """
    Asynchronous function to handle server shutdown procedures.

    This function performs the necessary steps to shut down the server gracefully,
    including saving data, stopping replication, performing backups, closing WebSocket
    sessions, and stopping the scheduler.
    """
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

    if await blockchain_manager.has_blockchain():
        print("Saving blockchain...")
        blockchain_manager.save_blockchain()

        print("Saving blockchain pending transactions...")
        blockchain_manager.save_blockchain_pending_transactions()

        print("Saving blockchain wallets...")
        blockchain_manager.save_blockchain_wallets()

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
