import json
import asyncio
import websockets
import nest_asyncio
nest_asyncio.apply()

class MginDBClient:
    def __init__(self, host='127.0.0.1', port=6446, username='', password=''):
        self.uri = f"ws://{host}:{port}"
        self.username = username
        self.password = password
        self.websocket = None

    async def connect(self):
        self.websocket = await websockets.connect(self.uri)
        auth_data = {'username': self.username, 'password': self.password}
        await self.websocket.send(json.dumps(auth_data))
        response = await self.websocket.recv()
        if response != "MginDB server connected... Welcome!":
            raise Exception("Failed to authenticate: " + response)

    async def send_command(self, command):
        if not self.websocket or self.websocket.closed:
            await self.connect()
        await self.websocket.send(command)
        return await self.websocket.recv()

    def _send_sync_command(self, *args):
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
        if self.websocket:
            await self.websocket.close()

    def set(self, key, value):
        return self._send_sync_command('SET', key, value)

    def indices(self, action, key=None, value=None):
        return self._send_sync_command('INDICES', action, key, value)

    def incr(self, key, value):
        return self._send_sync_command('INCR', key, value)

    def decr(self, key, value):
        return self._send_sync_command('DECR', key, value)

    def delete(self, key):
        return self._send_sync_command('DEL', key)

    def query(self, key, query_string=None, options=None):
        return self._send_sync_command('QUERY', key, query_string, options)

    def count(self, key):
        return self._send_sync_command('COUNT', key)

    def schedule(self, action, cron_or_key=None, command=None):
        return self._send_sync_command('SCHEDULE', action, cron_or_key, command)

    def sub(self, key):
        return self._send_sync_command('SUB', key)

    def unsub(self, key):
        return self._send_sync_command('UNSUB', key)