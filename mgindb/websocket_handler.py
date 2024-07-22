import uuid  # Module for generating unique identifiers
import ujson  # Module for JSON operations
from .app_state import AppState  # Importing AppState class from app_state module
from .connection_handler import asyncio, websockets, signal, stop_event, signal_stop  # Importing necessary functions and classes from connection_handler module
from .command_processing import CommandProcessor  # Importing CommandProcessor class from command_processing module

class WebSocketManager:
    def __init__(self, thread_executor, process_executor):
        """Initialize WebSocketManager with a command processor."""
        self.command_processor = CommandProcessor(thread_executor, process_executor)
    
    async def handle_websocket(self, websocket, path):
        """Handle a new WebSocket connection."""
        await WebSocketSession(websocket, self.command_processor).start()

class WebSocketSession:
    def __init__(self, websocket, command_processor):
        """Initialize WebSocketSession with a WebSocket connection and a command processor."""
        self.websocket = websocket
        self.command_processor = command_processor
        self.sid = str(uuid.uuid4())
        self.app_state = AppState()
        self.app_state.sessions[self.sid] = {
            'websocket': websocket,
            'subscribed_keys': set(),
        }

    async def start(self):
        """Start the WebSocket session."""
        try:
            await self.authenticate()
            await self.listen_for_messages()
        except websockets.exceptions.ConnectionClosedOK:
            pass
        except websockets.exceptions.ConnectionClosedError:
            pass
        except asyncio.CancelledError:
            await self.websocket.close(code=1001, reason='Server shutdown')
        except Exception as e:
            print(f"Failed to handle message due to: {e}")
        finally:
            await self.clean_up()

    async def authenticate(self):
        """Authenticate the WebSocket connection."""
        expected_username = self.app_state.config_store.get('USERNAME', '')
        expected_password = self.app_state.config_store.get('PASSWORD', '')

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
        """Check the provided credentials against expected values."""
        try:
            auth_data = ujson.loads(message)
            user_provided = auth_data.get('username', '')
            password_provided = auth_data.get('password', '')
            return user_provided == expected_username and password_provided == expected_password
        except ujson.JSONDecodeError:
            await self.websocket.send('Authentication required but no credentials provided.')
            await self.websocket.close(code=1008)
            return False

    async def listen_for_messages(self):
        """Listen for messages from the WebSocket and process commands."""
        async for message in self.websocket:
            response = await self.command_processor.process_command(message, self.sid, self.websocket)
            response = ujson.dumps(response) if isinstance(response, dict) else str(response)
            await self.websocket.send(response)

    async def clean_up(self):
        """Clean up the session and remove subscriptions."""
        session = self.app_state.sessions.pop(self.sid, None)
        if session:
            subscribed_keys = session.get('subscribed_keys', set())
            for key in subscribed_keys:
                if key == "MONITOR":
                    self.app_state.monitor_subscribers.discard(self.sid)
                elif key == "NODE":
                    self.app_state.node_subscribers.discard(self.sid)
                elif key in self.app_state.sub_pub:
                    self.app_state.sub_pub[key].discard(self.sid)
                    if not self.app_state.sub_pub[key]:
                        del self.app_state.sub_pub[key]

# Original function to handle websockets using the new WebSocketManager
async def handle_websocket(websocket, path, thread_executor, process_executor):
    """Handle WebSocket connections using WebSocketManager."""
    await WebSocketManager(thread_executor, process_executor).handle_websocket(websocket, path)
