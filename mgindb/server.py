# Import necessary modules and classes
from .app_state import AppState
from .connection_handler import asyncio, websockets, signal, stop_event, signal_handler
from .websocket_handler import handle_websocket
from .config import load_config
from .license_manager import LicenseManager
from .update_manager import UpdateManager
from .scheduler import SchedulerManager
from .data_manager import DataManager
from .indices_manager import IndicesManager
from .replication_manager import ReplicationManager

class ServerManager:
    def __init__(self):
        self.app_state = AppState()
        self.license_manager = LicenseManager()
        self.updater = UpdateManager()
        self.scheduler_manager = SchedulerManager()
        self.data_manager = DataManager()
        self.indices_manager = IndicesManager()
        self.replication_manager = ReplicationManager()

    async def start_server(self):
        """Asynchronous function to start the server."""
        await self.main()

    async def main(self):
        """Main function to initialize and start the server components."""
        try:
            # Load configuration
            load_config()
            print("Loading config...")

            # Display MginDB version
            print(f"MginDB version: {self.app_state.version}")

            # Check and install updates
            if self.updater.has_auto_update():
                latest_version, message = self.updater.check_update()
                print(message)
                if latest_version:
                    self.updater.install_update(latest_version)

            # Load scheduler
            self.scheduler_manager.load_scheduler()
            print("Loading scheduler...")

            # Load data
            self.data_manager.load_data()
            print("Loading data...")

            # Load indices
            self.indices_manager.load_indices()
            print("Loading indices...")

            # Setup replication
            if await self.replication_manager.has_replication_is_replication_master():
                print("Activating master replication...")

            if await self.replication_manager.has_replication_is_replication_slave():
                print("Activating slave replication...")
                await self.replication_manager.request_full_replication()

            # Start scheduler if active
            if self.scheduler_manager.is_scheduler_active():
                print("Starting scheduler...")
                await self.scheduler_manager.start_scheduler()

            # Start WebSocket server
            print("Starting websocket...")
            host = self.app_state.config_store.get('HOST')
            port = self.app_state.config_store.get('PORT')
            await websockets.serve(handle_websocket, host, port)
            print(f"WebSocket serving on {host}:{port}")

            # Wait for stop signal
            try:
                await stop_event.wait()
            except Exception as e:
                print(e)

        except Exception as e:
            print(f"Failed to start MginDB: {e}")

if __name__ == '__main__':
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        server_manager = ServerManager()
        asyncio.run(server_manager.main())  # Run the main function asynchronously
    except (KeyboardInterrupt, ConnectionAbortedError):
        pass
    except Exception as e:
        print(f"Failed to start MginDB: {e}")
