import uuid
import json
from .app_state import AppState
from .connection_handler import asyncio, websockets, signal, stop_event, signal_stop
from .command_processing import CommandProcessor

class WebSocketManager:
    def __init__(self):
        self.command_processor = CommandProcessor()
    
    async def handle_websocket(self, websocket, path):
        await WebSocketSession(websocket, self.command_processor).start()

class WebSocketSession:
    def __init__(self, websocket, command_processor):
        self.websocket = websocket
        self.command_processor = command_processor
        self.sid = str(uuid.uuid4())
        AppState().sessions[self.sid] = {
            'websocket': websocket,
            'subscribed_keys': set(),
        }
        AppState().websocket = websocket

    async def start(self):
        try:
            await self.authenticate()
            await self.listen_for_messages()
        except websockets.exceptions.ConnectionClosedOK:
            pass
        except asyncio.CancelledError:
            await self.websocket.close(code=1001, reason='Server shutdown')
        except Exception as e:
            print(f"Failed to handle message due to: {e}")
        finally:
            del AppState().sessions[self.sid]

    async def authenticate(self):
        expected_username = AppState().config_store.get('USERNAME', '')
        expected_password = AppState().config_store.get('PASSWORD', '')

        first_message = True
        async for message in self.websocket:
            if first_message:
                first_message = False
                if expected_username or expected_password:
                    if not await self.check_credentials(message, expected_username, expected_password):
                        await self.websocket.send('Authentication failed: Incorrect username or password.')
                        await self.websocket.close(code=1008)
                        return
                await self.websocket.send('MginDB server connected... Welcome!')
                break

    async def check_credentials(self, message, expected_username, expected_password):
        try:
            auth_data = json.loads(message)
            user_provided = auth_data.get('username', '')
            password_provided = auth_data.get('password', '')
            return user_provided == expected_username and password_provided == expected_password
        except json.JSONDecodeError:
            await self.websocket.send('Authentication required but no credentials provided.')
            await self.websocket.close(code=1008)
            return False

    async def listen_for_messages(self):
        async for message in self.websocket:
            response = await self.command_processor.process_command(message, self.sid)
            response = json.dumps(response) if isinstance(response, dict) else str(response)
            await self.websocket.send(response)

# Original function to handle websockets using the new WebSocketManager
async def handle_websocket(websocket, path):
    await WebSocketManager().handle_websocket(websocket, path)