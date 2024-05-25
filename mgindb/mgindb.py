import json  # Module for JSON operations
import asyncio  # Module for asynchronous programming
import websockets  # WebSocket client library for asyncio
import nest_asyncio  # Module to apply nested asyncio event loop support

# Apply nested asyncio to allow running async code in environments that already have a running event loop
nest_asyncio.apply()

class MginDBClient:
    def __init__(self, protocol='ws', host='127.0.0.1', port=6446, username='', password=''):
        """
        Initializes the MginDBClient with connection parameters.

        Args:
            host (str): The host address of the MginDB server.
            port (int): The port number of the MginDB server.
            username (str): The username for authentication.
            password (str): The password for authentication.
        """
        self.uri = f"{protocol}://{host}:{port}"  # WebSocket URI
        self.username = username  # Username for authentication
        self.password = password  # Password for authentication
        self.websocket = None  # WebSocket connection placeholder

    async def connect(self):
        """
        Asynchronously connects to the MginDB server and authenticates the user.

        Raises:
            Exception: If authentication fails.
        """
        self.websocket = await websockets.connect(self.uri)
        auth_data = {'username': self.username, 'password': self.password}
        await self.websocket.send(json.dumps(auth_data))
        response = await self.websocket.recv()
        if response != "MginDB server connected... Welcome!":
            raise Exception("Failed to authenticate: " + response)

    async def send_command(self, command):
        """
        Asynchronously sends a command to the MginDB server and receives the response.

        Args:
            command (str): The command to send.

        Returns:
            str: The server response.
        """
        if not self.websocket or self.websocket.closed:
            await self.connect()
        await self.websocket.send(command)
        return await self.websocket.recv()

    def _send_sync_command(self, *args):
        """
        Sends a command to the MginDB server synchronously by wrapping it in an asyncio event loop.

        Args:
            *args: The command and its arguments.

        Returns:
            str: The server response.
        """
        loop = asyncio.get_event_loop()
        command_str = ' '.join(str(arg) for arg in args if arg is not None)

        # Special handling for JSON data in SET command
        if 'SET' in command_str and '{' in command_str:
            key, json_data = command_str.split(' ', 2)[1:3]
            try:
                # Validate JSON before sending
                parsed_data = json.loads(json_data)
                json_data = json.dumps(parsed_data)
                command_str = f'SET {key} {json_data}'
            except json.JSONDecodeError:
                print("Invalid JSON data provided.")

        if loop.is_running():
            return asyncio.ensure_future(self.send_command(command_str))
        return loop.run_until_complete(self.send_command(command_str))

    async def close(self):
        """
        Asynchronously closes the WebSocket connection to the MginDB server.
        """
        if self.websocket:
            await self.websocket.close()

    def set(self, key, value):
        """
        Sets a value for a given key in the MginDB.

        Args:
            key (str): The key to set.
            value (str): The value to set.

        Returns:
            str: The server response.
        """
        return self._send_sync_command('SET', key, value)

    def indices(self, action, key=None, value=None):
        """
        Manages indices in the MginDB.

        Args:
            action (str): The action to perform (e.g., LIST, ADD).
            key (str, optional): The key for the action.
            value (str, optional): The value for the action.

        Returns:
            str: The server response.
        """
        return self._send_sync_command('INDICES', action, key, value)

    def incr(self, key, value):
        """
        Increments a key's value by a given amount in the MginDB.

        Args:
            key (str): The key to increment.
            value (str): The amount to increment by.

        Returns:
            str: The server response.
        """
        return self._send_sync_command('INCR', key, value)

    def decr(self, key, value):
        """
        Decrements a key's value by a given amount in the MginDB.

        Args:
            key (str): The key to decrement.
            value (str): The amount to decrement by.

        Returns:
            str: The server response.
        """
        return self._send_sync_command('DECR', key, value)

    def delete(self, key):
        """
        Deletes a key from the MginDB.

        Args:
            key (str): The key to delete.

        Returns:
            str: The server response.
        """
        return self._send_sync_command('DEL', key)

    def query(self, key, query_string=None, options=None):
        """
        Queries the MginDB with a given key and query string.

        Args:
            key (str): The key to query.
            query_string (str, optional): The query string.
            options (str, optional): Additional options for the query.

        Returns:
            str: The server response.
        """
        return self._send_sync_command('QUERY', key, query_string, options)

    def count(self, key):
        """
        Counts the number of records for a given key in the MginDB.

        Args:
            key (str): The key to count.

        Returns:
            str: The server response.
        """
        return self._send_sync_command('COUNT', key)

    def schedule(self, action, cron_or_key=None, command=None):
        """
        Schedules a task in the MginDB.

        Args:
            action (str): The action to perform (e.g., ADD, REMOVE).
            cron_or_key (str, optional): The cron expression or key for the action.
            command (str, optional): The command to schedule.

        Returns:
            str: The server response.
        """
        return self._send_sync_command('SCHEDULE', action, cron_or_key, command)

    def sub(self, key):
        """
        Subscribes to a key in the MginDB.

        Args:
            key (str): The key to subscribe to.

        Returns:
            str: The server response.
        """
        return self._send_sync_command('SUB', key)

    def unsub(self, key):
        """
        Unsubscribes from a key in the MginDB.

        Args:
            key (str): The key to unsubscribe from.

        Returns:
            str: The server response.
        """
        return self._send_sync_command('UNSUB', key)