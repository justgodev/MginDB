import asyncio
import subprocess
import sys
import time
import hashlib
import signal
import os
import json
from collections import deque  # For the cache
import aiosqlite

# List of required packages
required_packages = ['websockets', 'ujson', 'aiosqlite']

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

async def initialize_database():
    async with aiosqlite.connect('data/blockchain.db') as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS blockchain (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                idx INTEGER,
                timestamp INTEGER,
                nonce INTEGER,
                difficulty INTEGER,
                validation_time REAL,
                size INTEGER,
                previous_hash TEXT,
                hash TEXT,
                txid TEXT,
                sender TEXT,
                receiver TEXT,
                amount TEXT,
                data TEXT,
                fee TEXT
            )
        ''')
        # Create indices for faster retrieval
        await db.execute('CREATE INDEX IF NOT EXISTS idx_index ON blockchain (idx)')
        await db.execute('CREATE INDEX IF NOT EXISTS hash_index ON blockchain (hash)')
        await db.execute('CREATE INDEX IF NOT EXISTS txid_index ON blockchain (txid)')
        await db.execute('CREATE INDEX IF NOT EXISTS sender_index ON blockchain (sender)')
        await db.execute('CREATE INDEX IF NOT EXISTS receiver_index ON blockchain (receiver)')

        # Create the tx_index table
        await db.execute('''
            CREATE TABLE IF NOT EXISTS tx_index (
                txid TEXT PRIMARY KEY,
                block_index INTEGER
            )
        ''')
        await db.commit()

async def sync_blockchain(websocket):
    async with aiosqlite.connect('data/blockchain.db') as db:
        async with db.execute('SELECT idx FROM blockchain ORDER BY idx DESC LIMIT 1') as cursor:
            last_row = await cursor.fetchone()
            last_index = last_row[0] if last_row else 0

    sync_command = f"BLOCKCHAIN FROM {last_index + 1}" if last_index > 0 else "BLOCKCHAIN FULL"
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
                    async with aiosqlite.connect('data/blockchain.db') as db:
                        for block in new_blocks:
                            # Ensure all expected fields are present, adding default values if necessary
                            block.setdefault('previous_hash', "")
                            block.setdefault('txid', "")
                            block.setdefault('sender', "")
                            block.setdefault('receiver', "")
                            block.setdefault('amount', "")
                            block.setdefault('data', "")
                            block.setdefault('fee', "")
                            
                            # Insert each block
                            await db.execute('''
                                INSERT INTO blockchain (
                                    idx, timestamp, nonce, difficulty, validation_time, size, previous_hash,
                                    hash, txid, sender, receiver, amount, data, fee
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                block['index'], block['timestamp'], block['nonce'], block['difficulty'],
                                block['validation_time'], block['size'], block['previous_hash'], block['hash'],
                                block['txid'], block['sender'], block['receiver'], block['amount'], json.dumps(block['data']), block['fee']
                            ))

                            # Insert transactions into tx_index
                            if isinstance(block['data'], list):
                                for tx in block['data']:
                                    if 'txid' in tx:
                                        await db.execute('''
                                            INSERT OR REPLACE INTO tx_index (txid, block_index) VALUES (?, ?)
                                        ''', (tx['txid'], block['index']))
                            else:
                                if block['txid']:
                                    await db.execute('''
                                        INSERT OR REPLACE INTO tx_index (txid, block_index) VALUES (?, ?)
                                    ''', (block['txid'], block['index']))
                                    
                        await db.commit()
                    has_data_to_sync = True
                    print(green(f"Received chunk {response_data['chunk_index'] + 1} of {response_data['total_chunks']}"))
    except Exception as e:
        print(red(f"Failed to sync blockchain: {e}"))

async def handle_websocket(uri, username, password, validator_address, cache):
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                response_data = await authenticate(websocket, username, password)
                if response_data and response_data.get("status") == "OK":
                    print(yellow("Performing blockchain sync..."))
                    await sync_blockchain(websocket)
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
                            await sync_blockchain(websocket)
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

    # Initialize cache
    cache = deque()

    # Initialize the database
    await initialize_database()

    consumer_task = asyncio.create_task(handle_websocket(uri, username, password, validator_address, cache))

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
