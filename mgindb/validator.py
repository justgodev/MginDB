import asyncio
import subprocess
import sys
import time  # Module for time-related functions
import hashlib  # Module for cryptographic hash functions
import signal  # Module to handle signals

# List of required packages
required_packages = ['websockets', 'ujson']

def install_package(package):
    """Install a package using pip."""
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

def check_and_install_packages():
    """Check if required packages are installed, and install them if they are not."""
    import importlib
    for package in required_packages:
        try:
            importlib.import_module(package)
        except ImportError:
            print(f"Installing latest version of {package}...")
            install_package(package)

# Check and install packages
check_and_install_packages()

import websockets
import ujson

def color_text(text, color_code):
    """
    Returns text formatted with the given ANSI color code.

    Args:
        text (str): The text to color.
        color_code (int): The ANSI color code.

    Returns:
        str: The colored text.
    """
    return f"\033[{color_code}m{text}\033[0m"

def red(text):
    """
    Returns text formatted in red color.

    Args:
        text (str): The text to color.

    Returns:
        str: The red-colored text.
    """
    return color_text(text, 31)

def green(text):
    """
    Returns text formatted in green color.

    Args:
        text (str): The text to color.

    Returns:
        str: The green-colored text.
    """
    return color_text(text, 32)

def blue(text):
    """
    Returns text formatted in blue color.

    Args:
        text (str): The text to color.

    Returns:
        str: The blue-colored text.
    """
    return color_text(text, 34)

def cyan(text):
    """
    Returns text formatted in cyan color.

    Args:
        text (str): The text to color.

    Returns:
        str: The cyan-colored text.
    """
    return color_text(text, 36)

def yellow(text):
    """
    Returns text formatted in yellow color.

    Args:
        text (str): The text to color.

    Returns:
        str: The yellow-colored text.
    """
    return color_text(text, 33)

def magenta(text):
    """
    Returns text formatted in magenta color.

    Args:
        text (str): The text to color.

    Returns:
        str: The magenta-colored text.
    """
    return color_text(text, 35)

async def authenticate(websocket, username, password):
    auth_command = {
        "username": username,
        "password": password
    }
    await websocket.send(ujson.dumps(auth_command))
    response = await websocket.recv()
    if response:
        if "MginDB server connected... Welcome!" in response:
            print(green("AUTHENTICATION SUCCESSFUL"))
            return {"status": "OK"}
        else:
            try:
                response_data = ujson.loads(response)
                return response_data
            except ujson.JSONDecodeError:
                print(red("Failed to decode JSON response:"), red(response))
                return None
    else:
        print("Received empty response during authentication")
        return None

async def process_transaction(websocket, transaction):
    new_block = {
        'timestamp': int(time.time()),
        'nonce': 0,
        'difficulty': transaction['difficulty'],
        'validation_time': 0,
        'size': len(ujson.dumps(transaction).encode()),
        'hash': '',
        'txid': transaction['txid'],
        'sender': transaction['sender'],
        'receiver': transaction['receiver'],
        'amount': transaction['amount'],
        'data': transaction['data'],
        'fee': transaction['fee']
    }

    mined_block = await mine_block(new_block, new_block['difficulty'])

    await websocket.send(f"BLOCK {ujson.dumps(mined_block)}")

async def calculate_hash(block):
    block_string = ujson.dumps(block, sort_keys=True).encode()
    return hashlib.sha256(block_string).hexdigest()

async def mine_block(block, difficulty):
    start_time = time.time()  # Record the start time
    target = '0' * difficulty
    while block['hash'][:difficulty] != target:
        block['nonce'] += 1
        block['hash'] = await calculate_hash(block)
    block['validation_time'] = time.time() - start_time  # Record the time taken to mine the block
    return block

async def handle_message(websocket, message):
    try:
        message_data = ujson.loads(message)
        if "transaction" in message_data:
            transaction_data = message_data["transaction"]
            await process_transaction(websocket, transaction_data)
        elif "confirmation" in message_data:
            transaction_data = message_data["confirmation"]
            print(cyan("Validated Transaction"), transaction_data)
            print()
    except ujson.JSONDecodeError:
        if message != "OK" and message != "None":
            print(red(message))
            print()

async def handle_websocket(uri, username, password):
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                response_data = await authenticate(websocket, username, password)
                if response_data and response_data.get("status") == "OK":
                    print(yellow("MONITORING FOR PENDING TRANSACTIONS..."))
                    print()
                    await websocket.send("SUB NODE")
                    while True:
                        message = await websocket.recv()
                        if message:
                            response = await handle_message(websocket, message)
                            if response:
                                await websocket.send(ujson.dumps(response))
        except asyncio.CancelledError:
            print(red("WebSocket connection cancelled. Cleaning up..."))
            break
        except Exception as e:
            print(red(f"NODE DISCONNECTED. RECONNECTING IN 5 SECONDS..."))
            await asyncio.sleep(5)

async def main(stop_event):
    uri = "wss://mgindb.digitalwallet.health"  # Replace with your actual WebSocket URI
    username = ""  # Replace with your actual username
    password = ""  # Replace with your actual password

    consumer_task = asyncio.create_task(handle_websocket(uri, username, password))

    # Wait until stop_event is set
    await stop_event.wait()
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        print(red("Main task cancelled"))

def shutdown(loop, stop_event):
    """Shutdown function to close the event loop gracefully."""
    print(red("Shutdown requested. Exiting..."))
    stop_event.set()  # Signal the event to stop the loop

def run():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stop_event = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown, loop, stop_event)

    try:
        loop.run_until_complete(main(stop_event))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

if __name__ == '__main__':
    run()
