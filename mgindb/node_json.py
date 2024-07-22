import asyncio
import subprocess
import sys
import time
import hashlib
import signal
import os
import json
from collections import deque

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
    return f"\033[{color_code}m{text}\033[0m"

def red(text):
    return color_text(text, 31)

def green(text):
    return color_text(text, 32)

def blue(text):
    return color_text(text, 34)

def cyan(text):
    return color_text(text, 36)

def yellow(text):
    return color_text(text, 33)

def magenta(text):
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
            print()
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

async def process_transaction(websocket, transaction, validator_address, cache):
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
        'validator': validator_address,
        'amount': transaction['amount'],
        'data': transaction['data'],
        'fee': transaction['fee']
    }

    mined_block = await mine_block(new_block, new_block['difficulty'])

    try:
        await websocket.send(f"ADD_TO_BLOCK {ujson.dumps(mined_block)}")
    except websockets.ConnectionClosed:
        print(red("Connection closed, caching the transaction"))
        cache.append(f"ADD_TO_BLOCK {ujson.dumps(mined_block)}")

async def calculate_hash(block):
    block_string = ujson.dumps(block, sort_keys=True).encode()
    return hashlib.sha256(block_string).hexdigest()

async def mine_block(block, difficulty):
    start_time = time.time()
    target = '0' * difficulty
    while block['hash'][:difficulty] != target:
        block['nonce'] += 1
        block['hash'] = await calculate_hash(block)
    block['validation_time'] = time.time() - start_time
    return block

async def handle_message(websocket, message, validator_address, cache):
    try:
        message_data = ujson.loads(message)
        if "transaction" in message_data:
            transaction_data = message_data["transaction"]
            await process_transaction(websocket, transaction_data, validator_address, cache)
        elif "confirmation" in message_data:
            transaction_data = message_data["confirmation"]
            display_keys = ["hash", "txid"]
            filtered_data = {key: transaction_data[key] for key in display_keys if key in transaction_data}
            print(yellow("Validated Transaction"))
            print(cyan("Hash"), filtered_data['hash'])
            print(cyan("TXID"), filtered_data['txid'])
            print()
    except ujson.JSONDecodeError:
        if message != "OK" and message != "None":
            print(red(message))
            print()

async def sync_blockchain(websocket, blockchain, tx_index):
    blockchain_file = 'data/blockchain.json'
    tx_index_file = 'data/tx_index.json'
    if os.path.exists(blockchain_file):
        with open(blockchain_file, 'r') as f:
            existing_blocks = json.load(f)
            blockchain.clear()
            blockchain.extend(existing_blocks)
            last_index = existing_blocks[-1]['index'] if existing_blocks else 0
            sync_command = f"BLOCKCHAIN FROM {last_index + 1}"
            await websocket.send(sync_command)
    else:
        sync_command = "BLOCKCHAIN FULL"
        await websocket.send(sync_command)
    
    try:
        has_data_to_sync = False
        while True:
            response = await websocket.recv()
            response_data = ujson.loads(response)
            if isinstance(response_data, dict) and 'end' in response_data:
                if has_data_to_sync:
                    print(green(f"Blockchain sync completed"))
                    print()
                break
            elif isinstance(response_data, dict) and 'data' in response_data:
                new_blocks = response_data["data"]
                if isinstance(new_blocks, list) and new_blocks:
                    blockchain.extend(new_blocks)
                    for block in new_blocks:
                        if 'data' in block and isinstance(block['data'], list):
                            for tx in block['data']:
                                if 'txid' in tx:
                                    tx_index[tx['txid']] = block['index']
                        else:
                            if 'txid' in block:
                                tx_index[block['txid']] = block['index']
                    with open(blockchain_file, 'w') as f:
                        json.dump(blockchain, f, indent=4)
                    with open(tx_index_file, 'w') as f:
                        json.dump(tx_index, f, indent=4)
                    has_data_to_sync = True
                print(green(f"Received chunk {response_data['chunk_index'] + 1} of {response_data['total_chunks']}"))
    except Exception as e:
        print(red(f"Failed to sync blockchain: {e}"))

async def handle_websocket(uri, username, password, validator_address, cache, blockchain, tx_index):
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                response_data = await authenticate(websocket, username, password)
                if response_data and response_data.get("status") == "OK":
                    print(yellow("Performing blockchain sync..."))
                    await sync_blockchain(websocket, blockchain, tx_index)
                    print(yellow("Monitoring for pending transactions..."))
                    print()
                    await websocket.send("SUB NODE")
                    
                    # Send cached transactions
                    while cache:
                        cached_command = cache.popleft()
                        try:
                            await websocket.send(cached_command)
                        except websockets.ConnectionClosed:
                            print(red("Reconnection failed, returning to cache"))
                            cache.appendleft(cached_command)
                            break
                    
                    last_sync_time = time.time()
                    
                    while True:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=1)
                            if message:
                                await handle_message(websocket, message, validator_address, cache)
                        except asyncio.TimeoutError:
                            pass
                        
                        # Periodic sync and save
                        current_time = time.time()
                        if current_time - last_sync_time >= 60:
                            await sync_blockchain(websocket, blockchain, tx_index)
                            last_sync_time = current_time
        except asyncio.CancelledError:
            print(red("WebSocket connection cancelled. Cleaning up..."))
            break
        except Exception as e:
            print(red(f"NODE DISCONNECTED. RECONNECTING IN 5 SECONDS..."))
            await asyncio.sleep(5)

async def main(stop_event, validator_address):
    uri = "wss://mgindb.digitalwallet.health"
    username = ""  # Replace with your actual username
    password = ""  # Replace with your actual password

    # Initialize cache and in-memory blockchain and tx_index
    cache = deque()
    blockchain = []
    tx_index = {}

    # Load tx_index from file if it exists
    tx_index_file = 'data/tx_index.json'
    if os.path.exists(tx_index_file):
        with open(tx_index_file, 'r') as f:
            tx_index.update(json.load(f))

    consumer_task = asyncio.create_task(handle_websocket(uri, username, password, validator_address, cache, blockchain, tx_index))

    # Wait until stop_event is set
    await stop_event.wait()
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        print(red("Main task cancelled"))

def shutdown(loop, stop_event):
    print(red("Shutdown requested. Exiting..."))
    stop_event.set()  # Signal the event to stop the loop

def run():
    validator_address = ""
    while not validator_address.strip():
        validator_address = input("Please enter your validator wallet address: ").strip()
        if not validator_address:
            print("Validator wallet address cannot be empty. Please try again.")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stop_event = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown, loop, stop_event)

    try:
        loop.run_until_complete(main(stop_event, validator_address))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

if __name__ == '__main__':
    run()
