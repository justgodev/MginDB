# Import necessary modules and classes
import json
import os
from .app_state import AppState
from .connection_handler import asyncio, websockets

class ReplicationManager:
    def __init__(self):
        self.app_state = AppState()
    
    # Function to check if replication is enabled
    async def has_replication(self):
        replication = self.app_state.config_store.get('REPLICATION')
        return replication == '1'
    
    # Function to check if this node is replication master
    async def is_replication_master(self):
        replication_type = self.app_state.config_store.get('REPLICATION_TYPE')
        return replication_type == 'MASTER'
    
    # Function to check if this node is replication slave
    async def is_replication_slave(self):
        replication_type = self.app_state.config_store.get('REPLICATION_TYPE')
        return replication_type == 'SLAVE'

    # Function to check if replication is enabled and this node is the replication master
    async def has_replication_is_replication_master(self):
        replication_type = self.app_state.config_store.get('REPLICATION_TYPE')
        replication = self.app_state.config_store.get('REPLICATION')

        return replication_type == 'MASTER' and replication == '1'
    
    # Function to check if replication is enabled and this node is the replication slave
    async def has_replication_is_replication_slave(self):
        replication_type = self.app_state.config_store.get('REPLICATION_TYPE')
        replication = self.app_state.config_store.get('REPLICATION')

        return replication_type == 'SLAVE' and replication == '1'

    async def request_full_replication(self):
        master_uri = self.app_state.config_store.get('REPLICATION_MASTER')
        try:
            async with websockets.connect(f'ws://{master_uri}') as websocket:
                # Sending authentication data
                await websocket.send(json.dumps(self.app_state.auth_data))
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
                    print(f"Replication chunks received. Processing data...")
                    await self.process_replication_data({'data_chunks': data_chunks, 'indices_chunks': indices_chunks})
                    return "Replication data received and processed."
                else:
                    return "Authentication failed at master."
        except Exception as e:
            return f"Failed to communicate with master {master_uri}: {str(e)}"

    async def process_replication_data(self, data):
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
            print("Replication data processed successfully.")
        except json.JSONDecodeError as e:
            print(f"Error decoding replication data: {e}")

    async def send_command_to_slaves(self, command):
        async def send_command_to_slave(slave_uri):
            try:
                async with websockets.connect(f'ws://{slave_uri}') as websocket:
                    await websocket.send(json.dumps(self.app_state.auth_data))  # Send authentication data
                    auth_response = await websocket.recv()
                    if 'Welcome!' in auth_response:
                        await websocket.send(command)  # Send the command
                        return f"OK"
                    else:
                        return f"Authentication failed at {slave_uri}."
            except Exception as e:
                return f"Failed to send command to {slave_uri}: {str(e)}"

        # Get the list of slave URIs directly from the AppState
        slave_uris = self.app_state.config_store.get('REPLICATION_SLAVES')
        # Create and gather tasks for each slave
        tasks = [send_command_to_slave(slave_uri) for slave_uri in slave_uris]
        results = await asyncio.gather(*tasks)
        return results  # Returns a list of results from each slave