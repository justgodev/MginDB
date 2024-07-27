import uuid
import ujson
import asyncio
import websockets
from .app_state import AppState
from .command_processing import CommandProcessor

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
        self.message_queue = asyncio.Queue(maxsize=100)  # Limit the queue size to avoid memory issues
        self.stop_event = asyncio.Event()

    async def start(self):
        """Start the WebSocket session."""
        try:
            print(f"Starting WebSocket session with ID: {self.sid}")
            await self.authenticate()
            await asyncio.gather(
                self.listen_for_messages(),
                self.process_messages()
            )
        except Exception as e:
            print(f"Failed to handle message for session ID {self.sid} due to: {e}")
        finally:
            print(f"Cleaning up session ID: {self.sid}")
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
        """Listen for messages from the WebSocket and add them to the queue."""
        try:
            async for message in self.websocket:
                print(f'Socket Message received: {message}')
                await self.message_queue.put(message)
                print(f'Message put in queue: {message}')
        except websockets.exceptions.ConnectionClosed:
            print(f"WebSocket connection closed for session ID: {self.sid}")
        except Exception as e:
            print(f"Error listening for messages for session ID {self.sid}: {e}")
        finally:
            print(f"Listener is closing for session ID: {self.sid}")
            self.stop_event.set()
            await self.clean_up()

    async def process_messages(self):
        """Process messages from the queue."""
        while not self.stop_event.is_set() or not self.message_queue.empty():
            message = await self.message_queue.get()
            print(f'Message dequeued for processing: {message}')
            await self.process_message(message)

    async def process_message(self, message):
        """Process a single message."""
        try:
            print(f"Processing message: {message}")
            command = asyncio.create_task(self.command_processor.process_command(message, self.sid, self.websocket))
            response = await command
            response = ujson.dumps(response) if isinstance(response, dict) else str(response)
            await self.websocket.send(response)
            print(f'Response sent: {response}')
        except Exception as e:
            print(f"Error processing command for session ID {self.sid}: {e}")
        finally:
            self.message_queue.task_done()
            print(f'Message processing completed: {message}')

    async def clean_up(self):
        """Clean up the session and remove subscriptions."""
        try:
            print(f"Starting clean_up for session ID: {self.sid}")
            session = self.app_state.sessions.pop(self.sid, None)
            if session:
                print(f"Session found: {session}")
                subscribed_keys = session.get('subscribed_keys', set())
                print(f"Subscribed keys: {subscribed_keys}")

                for key in subscribed_keys:
                    print(f"Processing key: {key}")
                    if key == "MONITOR":
                        self.app_state.monitor_subscribers.discard(self.sid)
                        print(f"Removed {self.sid} from monitor_subscribers")
                    elif key == "NODE":
                        self.app_state.node_subscribers.discard(self.sid)
                        print(f"Removed {self.sid} from node_subscribers")
                    elif key == "NODE_LITE":
                        self.app_state.node_lite_subscribers.discard(self.sid)
                        print(f"Removed {self.sid} from node_lite_subscribers")
                    elif key in self.app_state.sub_pub:
                        self.app_state.sub_pub[key].discard(self.sid)
                        print(f"Removed {self.sid} from sub_pub[{key}]")
                        if not self.app_state.sub_pub[key]:
                            del self.app_state.sub_pub[key]
                            print(f"Deleted sub_pub[{key}] as it is now empty")
            else:
                print(f"No session found for session ID: {self.sid}")

            print(f"Clean up completed for session ID: {self.sid}")
        except Exception as e:
            print(f"Error during clean_up for session ID {self.sid}: {e}")

# Original function to handle websockets using the new WebSocketManager
async def handle_websocket(websocket, path, thread_executor, process_executor):
    """Handle WebSocket connections using WebSocketManager."""
    await WebSocketManager(thread_executor, process_executor).handle_websocket(websocket, path)
