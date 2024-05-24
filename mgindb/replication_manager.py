# Import necessary modules and classes
import json  # Module for JSON operations
import os  # Module for interacting with the operating system
from .app_state import AppState  # Importing AppState class from app_state module
from .connection_handler import asyncio, websockets  # Importing asyncio and websockets from connection_handler module

class ReplicationManager:
    def __init__(self):
        """Initialize ReplicationManager with application state."""
        self.app_state = AppState()

    async def has_replication(self):
        """
        Check if replication is enabled.

        Returns:
            bool: True if replication is enabled, False otherwise.
        """
        replication = self.app_state.config_store.get('REPLICATION')
        return replication == '1'

    async def is_replication_master(self):
        """
        Check if this node is the replication master.

        Returns:
            bool: True if this node is the replication master, False otherwise.
        """
        replication_type = self.app_state.config_store.get('REPLICATION_TYPE')
        return replication_type == 'MASTER'

    async def is_replication_slave(self):
        """
        Check if this node is the replication slave.

        Returns:
            bool: True if this node is the replication slave, False otherwise.
        """
        replication_type = self.app_state.config_store.get('REPLICATION_TYPE')
        return replication_type == 'SLAVE'

    async def has_replication_is_replication_master(self):
        """
        Check if replication is enabled and this node is the replication master.

        Returns:
            bool: True if replication is enabled and this node is the replication master, False otherwise.
        """
        replication_type = self.app_state.config_store.get('REPLICATION_TYPE')
        replication = self.app_state.config_store.get('REPLICATION')
        return replication_type == 'MASTER' and replication == '1'

    async def has_replication_is_replication_slave(self):
        """
        Check if replication is enabled and this node is the replication slave.

        Returns:
            bool: True if replication is enabled and this node is the replication slave, False otherwise.
        """
        replication_type = self.app_state.config_store.get('REPLICATION_TYPE')
        replication = self.app_state.config_store.get('REPLICATION')
        return replication_type == 'SLAVE' and replication == '1'

    async def request_full_replication(self, scheduler_manager, data_manager, indices_manager):
        """
        Request full replication from the replication master.

        Returns:
            str: The result of the replication request.
        """
        master_uri = self.app_state.config_store.get('REPLICATION_MASTER')
        try:
            async with websockets.connect(f'ws://{master_uri}') as websocket:
                await websocket.send(json.dumps(self.app_state.auth_data))  # Send authentication data
                auth_response = await websocket.recv()
                if 'Welcome!' in auth_response:
                    await websocket.send('REPLICATE')  # Send the replicate command

                    # Initialize empty lists to accumulate chunks
                    data_chunks = []
                    indices_chunks = []

                    # Keep receiving data until a 'done' message is received
                    while True:
                        chunk = await websocket.recv()
                        if chunk == 'DONE':
                            break
                        else:
                            chunk_data = json.loads(chunk)
                            data_chunks.extend(chunk_data['data_chunks'])
                            indices_chunks.extend(chunk_data['indices_chunks'])

                    # After all chunks are received, process them
                    print("Replication chunks received. Processing data...")
                    await self.process_replication_data({'data_chunks': data_chunks, 'indices_chunks': indices_chunks}, scheduler_manager, data_manager, indices_manager)
                    return "Replication data received and processed."
                else:
                    return "Authentication failed at master."
        except Exception as e:
            return f"Failed to communicate with master {master_uri}: {str(e)}"

    async def process_replication_data(self, data, scheduler_manager, data_manager, indices_manager):
        """
        Process replication data received from the master.

        Args:
            data (dict): The replication data.
        """
        try:
            # Joining chunks into complete strings
            data_string = ''.join(data['data_chunks'])
            indices_string = ''.join(data['indices_chunks'])

            # Deserialize the JSON strings into Python objects
            new_data = json.loads(data_string)
            new_indices = json.loads(indices_string)

            # Update AppState
            self.app_state.data_store = new_data
            self.app_state.indices = new_indices

            # Save the new state
            self.app_state.data_has_changed = True
            self.app_state.indices_has_changed = True
            if not scheduler_manager.is_scheduler_active():
                data_manager.save_data()  # Save data if scheduler is not active
                indices_manager.save_indices()  # Save indices if scheduler is not active

            print("Replication data processed successfully.")
        except json.JSONDecodeError as e:
            print(f"Error decoding replication data: {e}")

    async def send_command_to_slaves(self, command):
        """
        Send a command to all replication slaves.

        Args:
            command (str): The command to send.

        Returns:
            list: The results from each slave.
        """
        async def send_command_to_slave(slave_uri):
            try:
                async with websockets.connect(f'ws://{slave_uri}') as websocket:
                    await websocket.send(json.dumps(self.app_state.auth_data))  # Send authentication data
                    auth_response = await websocket.recv()
                    if 'Welcome!' in auth_response:
                        await websocket.send(command)  # Send the command
                        return "OK"
                    else:
                        return f"Authentication failed at {slave_uri}."
            except Exception as e:
                return f"Failed to send command to {slave_uri}: {str(e)}"

        slave_uris = self.app_state.config_store.get('REPLICATION_SLAVES')  # Get the list of slave URIs
        tasks = [send_command_to_slave(slave_uri) for slave_uri in slave_uris]  # Create tasks for each slave
        results = await asyncio.gather(*tasks)  # Gather results from all tasks
        return results  # Return the results from each slave