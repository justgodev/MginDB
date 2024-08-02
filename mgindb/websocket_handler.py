import uuid
import ujson
import asyncio
import websockets
import zlib
import base64
from .app_state import AppState
from .command_processing import CommandProcessor

class WebSocketManager:
    def __init__(self, thread_executor, process_executor, blockchain_manager):
        self.command_processor = CommandProcessor(thread_executor, process_executor, self)
        self.sessions = {}  # Store sessions to manage multiple connections
        self.blockchain_manager = blockchain_manager
        self.blockchain_websocket = None

    async def handle_websocket(self, websocket, path):
        session = WebSocketSession(websocket, self.command_processor)
        self.sessions[session.sid] = session
        await session.start()
        del self.sessions[session.sid]  # Clean up session after it's done

    async def start_blockchain_websocket(self):
        print("Starting Blockchain WebSocket to master node...")
        self.blockchain_websocket = await websockets.connect(
            "wss://master.mgindb.com",
            ping_interval=None,
            ping_timeout=None,
            max_size=None
        )
        print("Connected to master node at master.mgindb.com")
        await self.authenticate_blockchain_websocket()
        asyncio.create_task(self.handle_blockchain_websocket())

    async def authenticate_blockchain_websocket(self):
        auth_message = ujson.dumps({
            'username': "",
            'password': ""
        })
        
        print("Authenticating to Blockchain WebSocket")
        await self.blockchain_websocket.send(auth_message)
        
        auth_response = await self.blockchain_websocket.recv()
        
        if auth_response == 'MginDB server connected... Welcome!':
            print('Listening to Blockchain WebSocket')
        else:
            raise Exception('Authentication failed for Blockchain WebSocket')

    async def handle_blockchain_websocket(self):
        try:
            async for message in self.blockchain_websocket:
                print(f'Blockchain WebSocket message received: {message}')
                if message:
                    asyncio.create_task(self.process_blockchain_message(message))
        except websockets.exceptions.ConnectionClosed:
            print("Blockchain WebSocket connection closed.")
        except Exception as e:
            print(f"Error in Blockchain WebSocket connection: {e}")

    async def process_blockchain_message(self, message):
        try:
            #print(f"Processing blockchain message: {message}")
            for session in self.sessions.values():
                asyncio.create_task(session.websocket.send(message))
                #print(f"Message forwarded to main WebSocket session: {message}")
        except Exception as e:
            print(f"Error processing blockchain message: {e}")

    async def send_to_blockchain_websocket(self, message):
        if self.blockchain_websocket:
            try:
                asyncio.create_task(self.blockchain_websocket.send(message))
                #print(f"Message sent to Blockchain WebSocket: {message}")
            except Exception as e:
                print(f"Failed to send message to Blockchain WebSocket: {e}")
                return f"Error: {e}"
        else:
            return "ERROR: Blockchain WebSocket is not connected."

    async def close_blockchain_websocket(self):
        if self.blockchain_websocket:
            try:
                await self.blockchain_websocket.close()
                print("Blockchain WebSocket connection closed.")
            except Exception as e:
                print(f"Failed to close Blockchain WebSocket: {e}")

class WebSocketSession:
    def __init__(self, websocket, command_processor):
        self.websocket = websocket
        self.command_processor = command_processor
        self.sid = str(uuid.uuid4())
        self.app_state = AppState()
        self.app_state.sessions[self.sid] = {
            'websocket': websocket,
            'subscribed_keys': set(),
        }
        self.message_queue = asyncio.Queue(maxsize=10000)  # Increase the queue size
        self.stop_event = asyncio.Event()
        self.processing_tasks = set()  # Track ongoing tasks

    async def start(self):
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
        try:
            async for message in self.websocket:
                #print(f'Socket Message received: {message}')
                # Create a task to put the message in the queue to avoid blocking
                asyncio.create_task(self.enqueue_message(message))
        except websockets.exceptions.ConnectionClosed:
            print(f"WebSocket connection closed for session ID: {self.sid}")
        except Exception as e:
            print(f"Error listening for messages for session ID {self.sid}: {e}")
        finally:
            print(f"Listener is closing for session ID: {self.sid}")
            self.stop_event.set()
            await self.clean_up()

    async def enqueue_message(self, message):
        try:
            await self.message_queue.put(message)
            #print(f'Message put in queue: {message}')
        except Exception as e:
            print(f"Error enqueuing message: {e}")

    async def process_messages(self):
        #print(f"Starting to process messages for session ID: {self.sid}")
        while not self.stop_event.is_set() or not self.message_queue.empty():
            if not self.message_queue.empty():
                message = await self.message_queue.get()
                #print(f'Message dequeued for processing: {message}')
                task = asyncio.create_task(self.process_message(message))
                self.processing_tasks.add(task)
                task.add_done_callback(self.processing_tasks.discard)
            else:
                await asyncio.sleep(0.2)
        print(f"Stopping message processing for session ID: {self.sid}")

    async def process_message(self, message):
        try:
            #print(f"Processing message: {message}")

            # Check if the message is compressed (assuming it's base64-encoded and then compressed)
            if self.is_compressed(message):
                message = self.decompress_message(message)
                #print(f"Decompressed message: {message}")

            command = asyncio.create_task(self.command_processor.process_command(message, self.sid, self.websocket))
            response_command = await command
            if response_command:
                response = ujson.dumps(response_command) if isinstance(response_command, dict) else str(response_command)
                asyncio.create_task(self.websocket.send(response))
                #print(f'Response sent: {response}')
        except Exception as e:
            print(f"Error processing command for session ID {self.sid}: {e}")
        finally:
            self.message_queue.task_done()
            #print(f'Message processing completed: {message}')

    def is_compressed(self, message):
        try:
            # Try to decode the message from base64 and decompress it using zlib
            decoded_message = base64.b64decode(message)
            zlib.decompress(decoded_message)
            return True
        except (base64.binascii.Error, zlib.error):
            return False

    def decompress_message(self, message):
        try:
            # Decode from base64 and decompress using zlib
            decoded_message = base64.b64decode(message)
            decompressed_message = zlib.decompress(decoded_message)
            return decompressed_message.decode('utf-8')
        except Exception as e:
            print(f"Error decompressing message: {e}")
            raise

    async def clean_up(self):
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
    await WebSocketManager(thread_executor, process_executor).handle_websocket(websocket, path)
