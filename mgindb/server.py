# Import necessary modules and classes
from .app_state import AppState  # Application state management
from .connection_handler import asyncio, websockets, signal, stop_event, signal_handler  # Connection and signal handling
from .websocket_handler import WebSocketManager  # WebSocket handling
from .config import load_config  # Configuration loading
from .license_manager import LicenseManager  # License management
from .update_manager import UpdateManager  # Update management
from .scheduler import SchedulerManager  # Scheduler management
from .blockchain_manager import BlockchainManager  # Data management
from .data_manager import DataManager  # Data management
from .indices_manager import IndicesManager  # Indices management
from .replication_manager import ReplicationManager  # Replication management
from .cache_manager import CacheManager  # Cache management
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import uvloop
import os

# Use uvloop for a faster event loop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

class ServerManager:
    def __init__(self):
        # Initialize application state and various managers
        self.app_state = AppState()  # Manages application state
        self.license_manager = LicenseManager()  # Manages licensing
        self.updater = UpdateManager()  # Manages updates
        self.scheduler_manager = SchedulerManager()  # Manages the scheduler
        self.blockchain_manager = BlockchainManager()  # Manages blockchain
        self.data_manager = DataManager()  # Manages data
        self.indices_manager = IndicesManager()  # Manages indices
        self.replication_manager = ReplicationManager()  # Manages replication
        self.cache_manager = CacheManager()  # Manages cache
        
        # Get the number of available CPU cores
        num_cores = os.cpu_count()
        
        # Initialize thread and process pools based on available CPU cores
        self.thread_executor = ThreadPoolExecutor(max_workers=num_cores)  # Thread pool for I/O-bound tasks
        self.process_executor = ProcessPoolExecutor(max_workers=num_cores)  # Process pool for CPU-bound tasks

        self.websocket_manager = WebSocketManager(self.thread_executor, self.process_executor, self.blockchain_manager)

    async def start_server(self):
        """
        Asynchronous function to start the server.

        This function is an entry point to start the server asynchronously.
        It calls the main function which handles the initialization and starting
        of various server components.
        """
        await self.main()  # Call the main function to start the server

    async def load_components(self):
        """
        Initialize the server components.

        This function handles the initialization of the server components such as
        configuration loading, scheduler, data, indices, and replication setup.
        """
        try:
            print("Loading config...")  # Print message indicating config loading
            load_config()  # Load server configuration

            # Display MginDB version
            print(f"MginDB version: {self.app_state.version}")  # Print the current version of MginDB

            # Check and install updates
            if self.updater.has_auto_update():  # Check if auto-update is enabled
                latest_version, message = self.updater.check_update()  # Check for updates
                print(message)  # Print the update message
                if latest_version:  # If a new version is available
                    self.updater.install_update(latest_version)  # Install the update

            # Load scheduler
            print("Loading scheduler...")  # Print message indicating scheduler loading
            self.scheduler_manager.load_scheduler()  # Load the scheduler

            # Load cache manager
            if await self.cache_manager.has_caching():
                print("Loading cache manager...")

            # Load data
            print("Loading data...")  # Print message indicating data loading
            await self.run_in_executor(self.data_manager.load_data)  # Load data asynchronously

            # Load blockchain manager
            if await self.blockchain_manager.has_blockchain():
                print('is blockchain master', await self.blockchain_manager.is_blockchain_master())
                if await self.blockchain_manager.is_blockchain_master():
                    print("Loading blockchain...")
                    await self.blockchain_manager.load_blockchain()  # Load blockchain asynchronously

                    print("Loading blockchain pending transactions...")
                    await self.blockchain_manager.load_blockchain_pending_transactions()  # Load pending transactions asynchronously

                    print("Loading blockchain wallets...")
                    await self.blockchain_manager.load_blockchain_wallets()  # Load wallets asynchronously

            # Load indices
            print("Loading indices...")  # Print message indicating indices loading
            await self.run_in_executor(self.indices_manager.load_indices)  # Load indices asynchronously

            # Setup replication
            if await self.replication_manager.has_replication_is_replication_master():  # Check if this instance is a replication master
                print("Loading master replication...")  # Print message for master replication activation

            if await self.replication_manager.has_replication_is_replication_slave():  # Check if this instance is a replication slave
                print("Loading slave replication...")  # Print message for slave replication activation
                await self.replication_manager.request_full_replication(self.scheduler_manager, self.data_manager, self.indices_manager)  # Request full replication from the master

            # Start scheduler if active
            if self.scheduler_manager.is_scheduler_active():  # Check if the scheduler is active
                print("Loading scheduler...")  # Print message indicating scheduler starting
                await self.scheduler_manager.start_scheduler()  # Start the scheduler

        except Exception as e:
            print(f"Failed to load MginDB components: {e}")  # Print error message if server fails to load components
    
    async def main(self):
        """
        Main function to start the server.

        This function handles starts the WebSocket server and waits for a stop event to gracefully
        shutdown the server.
        """
        try:
            # Load components
            await self.load_components()

            # Start WebSocket server
            print("Starting websocket...")  # Print message indicating WebSocket starting
            host = self.app_state.config_store.get('HOST')  # Get host from config
            port = self.app_state.config_store.get('PORT')  # Get port from config
            
            await websockets.serve(self.websocket_manager.handle_websocket, host, port, max_size=None)  # Start the WebSocket server
            print(f"WebSocket serving on {host}:{port}")  # Print message with WebSocket server details

            # Start Blockchain Websocket connection to the master node if has blockchain
            if await self.blockchain_manager.has_blockchain():
                if not await self.blockchain_manager.is_blockchain_master():
                    await self.websocket_manager.start_blockchain_websocket()

            # Wait for stop signal
            try:
                await stop_event.wait()  # Wait for the stop event
            except Exception as e:
                print(e)  # Print any exceptions that occur
            finally:
                # Ensure the Blockchain Websocket is closed during shutdown
                await self.websocket_manager.close_blockchain_websocket()

        except Exception as e:
            print(f"Failed to start MginDB: {e}")  # Print error message if server fails to start

    async def run_in_executor(self, func, *args):
        """
        Run a blocking function in a separate thread or process.
        
        This helper function allows running blocking functions in a separate thread
        or process to prevent blocking the asyncio event loop.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.thread_executor, func, *args)

def main():
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)  # Handle SIGINT signal
    signal.signal(signal.SIGTERM, signal_handler)  # Handle SIGTERM signal

    try:
        server_manager = ServerManager()  # Create an instance of ServerManager
        asyncio.run(server_manager.main())  # Run the main function asynchronously
    except (KeyboardInterrupt, ConnectionAbortedError):
        pass  # Ignore keyboard interrupt and connection aborted errors
    except Exception as e:
        print(f"Failed to start MginDB: {e}")  # Print error message if server fails to start

if __name__ == '__main__':
    main()
