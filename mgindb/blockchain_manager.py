import ujson  # Module for JSON operations
import asyncio
import os  # Module for interacting with the operating system
import time  # Module for time-related functions
import uuid  # Module for uuid
import hashlib  # Module for cryptographic hash functions
import sqlite3  # Module for SQLite operations
from mnemonic import Mnemonic
from ecdsa import SigningKey, SECP256k1
from cryptography.fernet import Fernet
import base64
import base58
from .app_state import AppState  # Import application state management
from .constants import BLOCKCHAIN_DB, PENDING_TRANSACTIONS_FILE, WALLETS_FILE  # Import constant for blockchain and wallets
from .config import save_config  # Import config loading and saving
from .sub_pub_manager import SubPubManager  # Publish/subscribe management

class BlockchainManager:
    def __init__(self):
        """Initialize BlockchainManager with application state and data file path."""
        self.app_state = AppState()
        self.blockchain_db = BLOCKCHAIN_DB
        self.pending_transactions_file = PENDING_TRANSACTIONS_FILE
        self.sub_pub_manager = SubPubManager()
        self.last_block_time = time.time()
        self.tx_per_block = self.app_state.config_store.get('BLOCKCHAIN_TX_PER_BLOCK')
        self.sync_chunks = self.app_state.config_store.get('BLOCKCHAIN_SYNC_CHUNKS')
        self.accumulated_transactions = []  # List to store accumulated transactions
        self._init_blockchain()

    def _init_blockchain(self):
        """Initialize the SQLite database."""
        self.conn = sqlite3.connect(self.blockchain_db)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS blockchain (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                block_index INTEGER,
                timestamp INTEGER,
                nonce INTEGER,
                difficulty INTEGER,
                validation_time REAL,
                size INTEGER,
                previous_hash TEXT,
                hash TEXT,
                checksum TEXT,
                data TEXT,
                fee TEXT,
                validator TEXT
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS wallets (
                address TEXT PRIMARY KEY,
                public_key TEXT,
                private_key TEXT,
                words TEXT,
                balance TEXT,
                tx_count INTEGER,
                tx_data TEXT,
                last_tx_timestamp TEXT,
                created_at INTEGER
            )
        ''')
        self.conn.commit()

    async def has_blockchain(self):
        return self.app_state.config_store.get('BLOCKCHAIN') == '1'

    async def blockchain_routines(self):
            block_auto_creation_interval = self.app_state.config_store.get('BLOCKCHAIN_BLOCK_AUTO_CREATION_INTERVAL')
            current_time = time.time()
            if current_time - self.last_block_time >= int(block_auto_creation_interval):
                if len(self.accumulated_transactions) > 0:
                    await self.create_and_save_block()
                    self.last_block_time = time.time()

    def get_blockchain(self, args=None):
        """Return the entire blockchain or from a specific index."""
        
        # If args are provided and start with 'FROM', filter the blockchain data accordingly
        if args and args.strip().startswith('FROM'):
            _, index = args.split(' ')
            index = int(index)
            blockchain_data = [block for block in self.app_state.blockchain if block['index'] > index]
        else:
            blockchain_data = self.app_state.blockchain

        chunks = list(self.chunk_data(blockchain_data))  # Convert generator to list for debug print
        return chunks

    def chunk_data(self, data):
        """
        Chunk the data into smaller pieces for sending via WebSocket.
        """
        total_chunks = len(data)
        for i, block in enumerate(data):
            yield ujson.dumps({
                "chunk_index": i,
                "total_chunks": total_chunks,
                "data": [block]  # Send one full block per chunk
            })
        # Yield a separate end chunk as a valid JSON object
        yield ujson.dumps({"chunk_index": total_chunks, "end": True})

    def format_block(self, row):
        """
        Convert a database row into a block format.
        """
        block = {
            'index': row[1],
            'timestamp': row[2],
            'nonce': row[3],
            'difficulty': row[4],
            'validation_time': row[5],
            'size': row[6],
            'previous_hash': row[7],
            'hash': row[8],
            'checksum': row[9],
            'data': ujson.loads(row[10]),
            'fee': row[11],
            'validator': row[12]
        }
        return block

    async def load_blockchain(self):
        """
        Load data from the blockchain table into the application state.
        """
        try:
            with sqlite3.connect(self.blockchain_db) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM blockchain")
                loaded_blockchain = cursor.fetchall()
                if not loaded_blockchain:
                    await self.create_genesis_block()
                else:
                    self.app_state.blockchain = [
                        {
                            'index': row[1],
                            'timestamp': row[2],
                            'nonce': row[3],
                            'difficulty': row[4],
                            'validation_time': row[5],
                            'size': row[6],
                            'previous_hash': row[7],
                            'hash': row[8],
                            'checksum': row[9],
                            'data': ujson.loads(row[10]),
                            'fee': row[11],
                            'validator': row[12]
                        }
                        for row in loaded_blockchain
                    ]
        except sqlite3.Error as e:
            print(f"Failed to load blockchain: {e}")
            await self.create_genesis_block()
    
    async def load_blockchain_pending_transactions(self):
        """
        Load data from the pending transactions file into the application state.
        """
        if not os.path.exists(self.pending_transactions_file):
            with open(self.pending_transactions_file, mode='w', encoding='utf-8') as file:
                ujson.dump([], file)
        else:
            try:
                with open(self.pending_transactions_file, mode='r', encoding='utf-8') as file:
                    loaded_pending_transactions = ujson.load(file)
                self.app_state.blockchain_pending_transactions = loaded_pending_transactions
            except ujson.JSONDecodeError as e:
                print(f"Failed to load pending transactions: {e}")

    async def load_blockchain_wallets(self):
        """
        Load data from the wallets table into the application state.
        """
        try:
            with sqlite3.connect(self.blockchain_db) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM wallets")
                loaded_wallets = cursor.fetchall()
                if not loaded_wallets:
                    self.app_state.wallets = {}
                else:
                    self.app_state.wallets = {
                        wallet[0]: {
                            "public_key": wallet[1],
                            "private_key": wallet[2],
                            "words": wallet[3],
                            "balance": wallet[4],
                            "tx_count": wallet[5],
                            "tx_data": ujson.loads(wallet[6]),
                            "last_tx_timestamp": wallet[7],
                            "created_at": wallet[8]
                        } for wallet in loaded_wallets
                    }
        except sqlite3.Error as e:
            print(f"Failed to load wallets: {e}")
            # Optionally, initialize wallets to an empty state
            self.app_state.wallets = {}

    async def save_blockchain(self, block):
        """
        Save the provided block data to the blockchain table.
        """
        try:
            self.cursor.execute('''
                INSERT INTO blockchain (block_index, timestamp, nonce, difficulty, validation_time, size, previous_hash, hash, checksum, data, fee, validator)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                block['index'],
                block['timestamp'],
                block['nonce'],
                block['difficulty'],
                block['validation_time'],
                block['size'],
                block['previous_hash'],
                block['hash'],
                block['checksum'],
                ujson.dumps(block['data']),
                block['fee'],
                block['validator']
            ))
            self.conn.commit()
        except IOError as e:
            print(f"Failed to save blockchain: {e}")
    
    async def save_blockchain_pending_transactions(self):
        """
        Save the current data in the application state to the pending transactions file.
        """
        try:
            if self.app_state.blockchain_pending_transactions_has_changed:
                with open(self.pending_transactions_file, mode='w', encoding='utf-8') as file:
                    ujson.dump(self.app_state.blockchain_pending_transactions, file, indent=4)
                    self.app_state.blockchain_pending_transactions_has_changed = False
        except IOError as e:
            print(f"Failed to save pending transactions: {e}")
    
    async def save_blockchain_wallets(self, address, wallet):
        """
        Save the provided wallet data to the wallets table.
        """
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO wallets (address, public_key, private_key, words, balance, tx_count, tx_data, last_tx_timestamp, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                address,
                wallet['public_key'],
                wallet['private_key'],
                wallet['words'],
                wallet['balance'],
                wallet['tx_count'],
                ujson.dumps(wallet['tx_data']),
                wallet['last_tx_timestamp'],
                wallet['created_at']
            ))
            self.conn.commit()
        except IOError as e:
            print(f"Failed to save wallets: {e}")

    async def create_genesis_block(self):
        """Create the genesis wallet"""
        wallet = await self.new_wallet()
        genesis_address = wallet.get('address')
        """Create the genesis block and initialize blockchain configuration."""
        blockchain_supply = self.app_state.config_store.get('BLOCKCHAIN_SUPPLY', '0')

        genesis_transaction = {
            'timestamp': int(time.time()),
            'nonce': 0,
            'difficulty': 0,
            'validation_time': 0,
            'size': 0,
            'hash': '',
            'checksum': '',
            'txid': '',
            'sender': genesis_address,
            'receiver': genesis_address,
            'validator': '',
            'amount': blockchain_supply,
            'data': '',
            'fee': '0'
        }

        tx_size = len(ujson.dumps(genesis_transaction).encode())
        genesis_transaction['txid'] = await self.hash_data(genesis_transaction)
        genesis_transaction['hash'] = await self.calculate_hash(genesis_transaction)
        genesis_transaction['size'] = tx_size
        genesis_transaction['checksum'] = self.calculate_checksum(genesis_transaction)

        genesis_block = {
            'index': 0,
            'timestamp': int(time.time()),
            'nonce': 0,
            'difficulty': 1,
            'validation_time': 0,
            'size': tx_size,
            'previous_hash': '0',
            'hash': '',
            'checksum': '',
            'data': [genesis_transaction],
            'fee': '0',
            'validator': ''
        }
    
        genesis_block['hash'] = await self.calculate_hash(genesis_block)
        genesis_block['checksum'] = self.calculate_checksum(genesis_block)
        self.app_state.blockchain.append(genesis_block)
        await self.save_blockchain(genesis_block)

        genesis_wallet = self.app_state.wallets.get(wallet.get('address'))
        if genesis_wallet:
            genesis_wallet['balance'] = blockchain_supply
            await self.save_blockchain_wallets(genesis_address, genesis_wallet)

        # Initialize blockchain configuration
        self.app_state.config_store['BLOCKCHAIN_CONF'] = {
            'genesis_address': wallet.get('address'),
            'chain_length': 1,
            'previous_hash': genesis_block['hash'],
            'latest_block': int(genesis_block['timestamp']),
            'validation_time': genesis_block['validation_time'],
            'difficulty': genesis_block['difficulty'],
            'fee': self.app_state.config_store['BLOCKCHAIN_SETUP_FEE']
        }
        save_config()

    async def calculate_hash(self, block):
        block_string = ujson.dumps(block, sort_keys=True).encode()
        return hashlib.sha256(block_string).hexdigest()

    async def mine_block(self, block, difficulty):
        start_time = time.time()  # Record the start time
        target = '0' * difficulty
        while block['hash'][:difficulty] != target:
            block['nonce'] += 1
            block['hash'] = await self.calculate_hash(block)
        block['validation_time'] = time.time() - start_time  # Record the time taken to mine the block
        return block

    async def hash_data(self, data):
        """Hash the data using SHA-256."""
        data_string = ujson.dumps(data, sort_keys=True).encode()
        return hashlib.sha256(data_string).hexdigest()


    async def add_to_block(self, block_data):
        from .scheduler import SchedulerManager  # Scheduler management
        self.scheduler_manager = SchedulerManager()  # Manages the scheduler

        try:
            # Decode the JSON block data
            transaction = ujson.loads(block_data)
            
            # Add the transaction to the accumulated transactions
            self.accumulated_transactions.append(transaction)

            # Remove the transaction from pending transactions by txid
            self.app_state.blockchain_pending_transactions = [
                tx for tx in self.app_state.blockchain_pending_transactions
                if tx['txid'] != transaction['txid']
            ]

            # Save pending transactions
            self.app_state.blockchain_pending_transactions_has_changed = True
            if not self.scheduler_manager.is_scheduler_active():
                self.save_blockchain_pending_transactions()
            
            # Tx per block
            if len(self.accumulated_transactions) >= int(self.tx_per_block):
                asyncio.create_task(self.send_block_for_mining())

            return ujson.dumps({
                "confirmation": transaction
            })
        except Exception as e:
            print(f"Error adding transaction: {e}")
            return "Error adding transaction"

    async def send_block_for_mining(self):
        try:
            # Blockchain details
            chain_length = self.app_state.config_store['BLOCKCHAIN_CONF']['chain_length']
            previous_hash = self.app_state.config_store['BLOCKCHAIN_CONF']['previous_hash']

            block = {
                'index': chain_length,
                'timestamp': int(time.time()),
                'nonce': 0,
                'difficulty': self.app_state.config_store['BLOCKCHAIN_CONF']['difficulty'],
                'validation_time': 0,
                'size': len(ujson.dumps(self.accumulated_transactions).encode()),
                'previous_hash': previous_hash,
                'hash': '',
                'checksum': '',
                'data': self.accumulated_transactions,  # Use accumulated transactions
                'fee': str(sum(int(tx['fee']) for tx in self.accumulated_transactions)),
                'validator': ''
            }

            self.accumulated_transactions = []
            blockchain_data = self.app_state.config_store['BLOCKCHAIN_CONF']
            blockchain_data['chain_length'] = block['index'] + 1

            # Generate a unique request_id
            request_id = str(uuid.uuid4())

            # Store the request_id in app_state.blockchain_blocks_requests with empty data
            self.app_state.blockchain_blocks_requests[request_id] = None

            # Notify nodes passing the request_id
            asyncio.create_task(self.sub_pub_manager.notify_node('mining', {'data': block, 'request_id': request_id}, node_type='FULL'))

            # Wait for resolve_blocks to return the result for the request_id
            mined_block_str = await self.resolve_blocks(request_id)

            if not mined_block_str:
                print('Mining failed or timed out')
                return False  # Return False if mining failed

            # Save the block to the blockchain
            mined_block = ujson.loads(mined_block_str)
            self.app_state.blockchain.append(mined_block)
            await self.save_blockchain(mined_block)

            # Notify nodes
            asyncio.create_task(self.sub_pub_manager.notify_nodes('block', ujson.dumps([mined_block]), node_type='FULL'))

            # Update blockchain configuration
            new_difficulty = self.adjust_difficulty(mined_block['validation_time'])
            await self.update_blockchain_data(mined_block, new_difficulty)

            # Dictionary to accumulate rewards for each validator
            validator_rewards = {}

            # Update wallets data
            for txn in mined_block['data']:
                sender_address = txn['sender']
                sender_wallet = self.app_state.wallets.get(sender_address)
                receiver_address = txn['receiver']
                receiver_wallet = self.app_state.wallets.get(receiver_address)
                validator_address = txn['validator']
                tx_data = f"{txn['txid']}:{block['index']}"

                if sender_wallet:
                    sender_wallet['balance'] = str(int(sender_wallet['balance']) - int(txn['amount']) - int(txn['fee']))
                    sender_wallet['last_tx_timestamp'] = mined_block['timestamp']
                    sender_wallet['tx_count'] += 1
                    sender_wallet['tx_data'].append(tx_data)
                    await self.save_blockchain_wallets(sender_address, sender_wallet)

                if receiver_wallet and receiver_wallet != sender_wallet:
                    receiver_wallet['balance'] = str(int(receiver_wallet['balance']) + int(txn['amount']))
                    receiver_wallet['last_tx_timestamp'] = mined_block['timestamp']
                    receiver_wallet['tx_count'] += 1
                    receiver_wallet['tx_data'].append(tx_data)
                    await self.save_blockchain_wallets(receiver_address, receiver_wallet)
            
                if validator_address:
                    if validator_address not in validator_rewards:
                        validator_rewards[validator_address] = 0
                    validator_rewards[validator_address] += int(self.app_state.config_store['BLOCKCHAIN_VALIDATOR_REWARD'])
        
            # Create a single transaction per validator with the total rewards
            genesis_address = self.app_state.config_store['BLOCKCHAIN_CONF']['genesis_address']
            for validator_address, total_reward in validator_rewards.items():
                validator_data = str({'Validator reward': [txn['hash'] for txn in mined_block['data'] if txn['validator'] == validator_address]})
                await self.add_transaction(genesis_address, validator_address, str(total_reward), validator_data)

            # Remove the request entry from app_state.blockchain_txns_requests
            self.app_state.blockchain_blocks_requests.pop(request_id, None)

            return True  # Return True if everything was successful
        except Exception as e:
            print(f"Error sending block for mining: {e}")
            return False  # Return False if an exception occurred

    async def create_and_save_block(self):
        chain_length = self.app_state.config_store['BLOCKCHAIN_CONF']['chain_length']
        previous_hash = self.app_state.config_store['BLOCKCHAIN_CONF']['previous_hash']

        block = {
            'index': chain_length,
            'timestamp': int(time.time()),
            'nonce': 0,
            'difficulty': self.app_state.config_store['BLOCKCHAIN_CONF']['difficulty'],
            'validation_time': 0,
            'size': len(ujson.dumps(self.accumulated_transactions).encode()),
            'previous_hash': previous_hash,
            'data': self.accumulated_transactions,  # Use accumulated transactions
            'fee': str(sum(int(tx['fee']) for tx in self.accumulated_transactions)),
            'validator': ''
        }

        # Clear the accumulated transactions
        self.accumulated_transactions = []
        mined_block = await self.mine_block(block, block['difficulty'])
        mined_block['checksum'] = self.calculate_checksum(mined_block)
        
        # Save the block to the blockchain
        self.app_state.blockchain.append(mined_block)
        await self.save_blockchain(mined_block)

        # Notify nodes
        asyncio.create_task(self.sub_pub_manager.notify_nodes('block', ujson.dumps([mined_block]), node_type='FULL'))

        # Update blockchain configuration
        new_difficulty = self.adjust_difficulty(mined_block['validation_time'])
        await self.update_blockchain_data(mined_block, new_difficulty)

        # Dictionary to accumulate rewards for each validator
        validator_rewards = {}

        # Update wallets data
        for txn in mined_block['data']:
            sender_address = txn['sender']
            sender_wallet = self.app_state.wallets.get(sender_address)
            receiver_address = txn['receiver']
            receiver_wallet = self.app_state.wallets.get(receiver_address)
            validator_address = txn['validator']
            tx_data = f"{txn['txid']}:{block['index']}"

            if sender_wallet:
                sender_wallet['balance'] = str(int(sender_wallet['balance']) - int(txn['amount']) - int(txn['fee']))
                sender_wallet['last_tx_timestamp'] = mined_block['timestamp']
                sender_wallet['tx_count'] += 1
                sender_wallet['tx_data'].append(tx_data)
                await self.save_blockchain_wallets(sender_address, sender_wallet)

            if receiver_wallet and receiver_wallet != sender_wallet:
                receiver_wallet['balance'] = str(int(receiver_wallet['balance']) + int(txn['amount']))
                receiver_wallet['last_tx_timestamp'] = mined_block['timestamp']
                receiver_wallet['tx_count'] += 1
                receiver_wallet['tx_data'].append(tx_data)
                await self.save_blockchain_wallets(receiver_address, receiver_wallet)
            
            if validator_address:
                if validator_address not in validator_rewards:
                    validator_rewards[validator_address] = 0
                validator_rewards[validator_address] += int(self.app_state.config_store['BLOCKCHAIN_VALIDATOR_REWARD'])
        
        # Create a single transaction per validator with the total rewards
        genesis_address = self.app_state.config_store['BLOCKCHAIN_CONF']['genesis_address']
        for validator_address, total_reward in validator_rewards.items():
            validator_data = str({'Validator reward': [txn['hash'] for txn in mined_block['data'] if txn['validator'] == validator_address]})
            await self.add_transaction(genesis_address, validator_address, str(total_reward), validator_data)

    def adjust_difficulty(self, validation_time):
        target_time = 5  # Target time per block in seconds

        current_difficulty = self.app_state.config_store['BLOCKCHAIN_CONF']['difficulty']

        if validation_time < target_time:
            # If mining time is less than the target, increase difficulty proportionally
            adjustment = max(1, int((target_time - validation_time) * current_difficulty / target_time))
            new_difficulty = current_difficulty + adjustment
        elif validation_time > target_time:
            # If mining time is more than the target, decrease difficulty proportionally
            adjustment = max(1, int((validation_time - target_time) * current_difficulty / target_time))
            new_difficulty = current_difficulty - adjustment
        else:
            new_difficulty = current_difficulty

        # Ensure the difficulty does not drop below 1
        if new_difficulty < 1:
            new_difficulty = 1

        # Ensure the difficulty does not exceed the maximum value
        if new_difficulty > 3:
            new_difficulty = 3

        return new_difficulty

    def validate_block(self, block):
        previous_block = self.app_state.blockchain[block['index'] - 1]
        if block['previous_hash'] != previous_block['hash']:
            return False
        if block['hash'] != self.calculate_hash(block):
            return False
        if block['hash'][:block['difficulty']] != '0' * block['difficulty']:
            return False
        return True

    def validate_chain(self):
        for i in range(1, len(self.app_state.blockchain)):
            if not self.validate_block(self.app_state.blockchain[i]):
                return False
        return True

    async def add_transaction(self, sender, receiver, amount="0", data="", fee="0"):
        from .scheduler import SchedulerManager  # Scheduler management
        self.scheduler_manager = SchedulerManager()  # Manages the scheduler

        # Convert sender address to bytes
        try:
            address_bytes = sender.encode('utf-8')
        except AttributeError as e:
            print(f"Error encoding sender: {e}")
            raise

        # Compute SHA-256 hash of the address bytes
        try:
            sha256_hash = hashlib.sha256(address_bytes).digest()
        except Exception as e:
            print(f"Error hashing address bytes: {e}")
            raise

        # Encode the hash using base64 URL-safe encoding
        try:
            encryption_key = base64.urlsafe_b64encode(sha256_hash)
        except Exception as e:
            print(f"Error encoding hash: {e}")
            raise

        # Create a Fernet encryption object with the encryption key
        try:
            fernet = Fernet(encryption_key)
        except Exception as e:
            print(f"Error creating Fernet object: {e}")
            raise

        # Encrypt the data using the Fernet encryption object
        try:
            encrypted_data = fernet.encrypt(data.encode()).decode()
        except Exception as e:
            print(f"Error encrypting data: {e}")
            raise

        transaction = {
            'sender': sender,
            'receiver': receiver,
            'amount': str(int(float(amount))),
            'data': encrypted_data,
            'fee': fee
        }

        txid = await self.hash_data(transaction)
        transaction['txid'] = str(txid)
        transaction['difficulty'] = self.app_state.config_store['BLOCKCHAIN_CONF']['difficulty']
        transaction['size'] = len(ujson.dumps(transaction).encode())
        transaction['checksum'] = self.calculate_checksum(transaction)
        self.app_state.blockchain_pending_transactions.append(transaction)

        # Save pending transactions
        self.app_state.blockchain_pending_transactions_has_changed = True
        if not self.scheduler_manager.is_scheduler_active():
            self.save_blockchain_pending_transactions()
        
        asyncio.create_task(self.sub_pub_manager.notify_node('transaction', transaction, node_type='ALL'))
        #await self.blockchain_routines()
        return transaction

    async def update_blockchain_data(self, block, difficulty):
        blockchain_data = self.app_state.config_store['BLOCKCHAIN_CONF']
        blockchain_data['previous_hash'] = block['hash']
        blockchain_data['latest_block'] = block['timestamp']
        blockchain_data['validation_time'] = block['validation_time']
        blockchain_data['difficulty'] = difficulty
        
        save_config()
    
    def calculate_checksum(self, data):
        """Calculate SHA-256 checksum of the given data."""
        data_string = ujson.dumps(data, sort_keys=True).encode()
        return hashlib.sha256(data_string).hexdigest()

    def verify_checksum(self, data, checksum):
        """Verify the given checksum matches the calculated checksum of the data."""
        return self.calculate_checksum(data) == checksum
    
    async def generate_mnemonic(self):
        mnemonic = Mnemonic("english")
        words = mnemonic.generate(strength=256)
        seed = Mnemonic.to_seed(words)
        return words, seed

    async def generate_keys_from_seed(self, seed):
        sk = SigningKey.from_string(seed[:32], curve=SECP256k1)
        vk = sk.get_verifying_key()
        private_key = sk.to_string().hex()
        public_key = vk.to_string("uncompressed").hex()
        address = await self.generate_address(bytes.fromhex(public_key))
        return private_key, public_key, address

    async def generate_address(self, public_key):
        # Step 1: Perform SHA-256 hashing on the public key
        sha256_1 = hashlib.sha256(public_key).digest()

        # Step 2: Perform RIPEMD-160 hashing on the SHA-256 result
        ripemd160_hash = hashlib.new('ripemd160', sha256_1).digest()

        # Step 3: Add the custom prefix (e.g., 0x33 for 'M')
        prefix_byte = b'\x33'  # Adjust the prefix byte to ensure the address starts with 'M'
        extended_ripemd160 = prefix_byte + ripemd160_hash

        # Step 4: Perform double SHA-256 to calculate the checksum
        sha256_2 = hashlib.sha256(extended_ripemd160).digest()
        sha256_3 = hashlib.sha256(sha256_2).digest()
        checksum = sha256_3[:4]

        # Step 5: Add the checksum to the extended RIPEMD-160 result
        binary_address = extended_ripemd160 + checksum

        # Step 6: Encode the result using Base58
        base58_address = base58.b58encode(binary_address)

        return base58_address.decode('utf-8')

    async def new_wallet(self, *args, **kwargs):
        from .scheduler import SchedulerManager  # Scheduler management
        self.scheduler_manager = SchedulerManager()  # Manages the scheduler

        if await self.has_blockchain():
            words, seed = await self.generate_mnemonic()
            private_key, public_key, address = await self.generate_keys_from_seed(seed)

            # Encrypt the mnemonic and private key with Fernet using the private key as the encryption key
            encryption_key = base64.urlsafe_b64encode(hashlib.sha256(bytes.fromhex(private_key)).digest())
            fernet = Fernet(encryption_key)
            encrypted_words = fernet.encrypt(words.encode()).decode()
            encrypted_private_key = fernet.encrypt(private_key.encode()).decode()

            wallet_data = {
                "address": address,
                "words": words,
                "private_key": private_key,
                "public_key": public_key
            }

            encrypted_wallet_data = {
                "public_key": str(public_key),
                "private_key": encrypted_private_key,
                "words": encrypted_words,
                "balance": "0",
                "tx_count": 0,
                "tx_data": [],
                "last_tx_timestamp": "",
                "created_at": int(time.time())
            }

            self.app_state.wallets[address] = encrypted_wallet_data

            # Save blockchain wallets
            await self.save_blockchain_wallets(address, encrypted_wallet_data)

            return wallet_data
        else: 
            return 'Blockchain feature is not active. Use CONFIG SET BLOCKCHAIN 1'
    
    async def get_wallet(self, *args, **kwargs):
        # Extract address from args
        address = None
        if args:
            for arg in args:
                if isinstance(arg, str):
                    address = arg
                    break
        
        if not address:
            return "No valid address found in args"

        encrypted_wallet_data = self.app_state.wallets.get(address)
        if not encrypted_wallet_data:
            return "Wallet not found"

        wallet_data = {
            "public_key": encrypted_wallet_data.get("public_key"),
            "balance": encrypted_wallet_data.get("balance"),
            "tx_count": encrypted_wallet_data.get("tx_count"),
            "tx_data": encrypted_wallet_data.get("tx_data"),
            "last_tx_timestamp": encrypted_wallet_data.get("last_tx_timestamp"),
            "created_at": encrypted_wallet_data.get("created_at")
        }

        return wallet_data
    
    async def get_wallet_balance(self, *args, **kwargs):
        # Extract address from args
        address = None
        if args:
            for arg in args:
                if isinstance(arg, str):
                    address = arg
                    break
        
        if not address:
            return "No valid address found in args"

        encrypted_wallet_data = self.app_state.wallets.get(address)
        if not encrypted_wallet_data:
            return "Wallet not found"

        wallet_data = {
            "balance": encrypted_wallet_data.get("balance"),
        }

        return wallet_data

    async def get_blocks(self, options):
        order_by = 'DESC'
        latest = False
        limit_start = 0
        limit_end = None

        if 'order' in options:
            order_by = options['order']
        if 'latest' in options and options['latest']:
            latest = options['latest']
        if 'limit' in options:
            limits = options['limit'].split(',')
            if len(limits) == 2:
                limit_start = int(limits[0])
                limit_end = int(limits[1])

        # Generate a unique request_id
        request_id = str(uuid.uuid4())

        # Store the request_id in app_state.blockchain_txns_requests with empty data
        self.app_state.blockchain_txns_requests[request_id] = None

        # Prepare the data to be sent to nodes
        data = {
            'order_by': order_by,
            'latest': latest,
            'limit_start': limit_start,
            'limit_end': limit_end
        }

        # Notify nodes passing the request_id and data
        asyncio.create_task(self.sub_pub_manager.notify_node('get_blocks', {'data': data, 'request_id': request_id, 'options': options}, node_type='FULL'))

        # Wait for resolve_txns to return the result for the request_id
        result = await self.resolve_txns(request_id)

        # Remove the request entry from app_state.blockchain_txns_requests
        self.app_state.blockchain_txns_requests.pop(request_id, None)

        return result

    async def get_block(self, *args, **kwargs):
        data = None
        if args:
            for arg in args:
                if isinstance(arg, str):
                    data = arg
                    break

        if not data:
            return "Not a valid block hash"

        request_id = str(uuid.uuid4())
        self.app_state.blockchain_txns_requests[request_id] = None

        asyncio.create_task(self.sub_pub_manager.notify_node('get_block', {'data': data, 'request_id': request_id}, node_type='FULL'))
        result = await self.resolve_txns(request_id)
        self.app_state.blockchain_txns_requests.pop(request_id, None)

        return result

    async def get_txns(self, options):
        order_by = 'DESC'
        latest = False
        limit_start = 0
        limit_end = None

        if 'order' in options:
            order_by = options['order']
        if 'latest' in options and options['latest']:
            latest = options['latest']
        if 'limit' in options:
            limits = options['limit'].split(',')
            if len(limits) == 2:
                limit_start = int(limits[0])
                limit_end = int(limits[1])

        # Generate a unique request_id
        request_id = str(uuid.uuid4())

        # Store the request_id in app_state.blockchain_txns_requests with empty data
        self.app_state.blockchain_txns_requests[request_id] = None

        # Prepare the data to be sent to nodes
        data = {
            'order_by': order_by,
            'latest': latest,
            'limit_start': limit_start,
            'limit_end': limit_end
        }

        # Notify nodes passing the request_id and data
        asyncio.create_task(self.sub_pub_manager.notify_node('get_txns', {'data': data, 'request_id': request_id, 'options': options}, node_type='FULL'))

        # Wait for resolve_txns to return the result for the request_id
        result = await self.resolve_txns(request_id)

        # Remove the request entry from app_state.blockchain_txns_requests
        self.app_state.blockchain_txns_requests.pop(request_id, None)

        return result

    async def get_txn(self, *args, **kwargs):
        # Extract data from args
        data = None
        if args:
            for arg in args:
                if isinstance(arg, str):
                    data = arg
                    break
        
        if not data:
            return "Not a valid txid or hash"

        # Generate a unique request_id
        request_id = str(uuid.uuid4())

        # Store the request_id in app_state.blockchain_txns_requests with empty data
        self.app_state.blockchain_txns_requests[request_id] = None

        # Notify nodes passing the request_id
        asyncio.create_task(self.sub_pub_manager.notify_node('get_txn', {'data': data, 'request_id': request_id}, node_type='FULL'))

        # Wait for resolve_txns to return the result for the request_id
        result = await self.resolve_txns(request_id)

        # Remove the request entry from app_state.blockchain_txns_requests
        self.app_state.blockchain_txns_requests.pop(request_id, None)

        return result

    async def resolve_blocks(self, request_id):
        while True:
            if request_id in self.app_state.blockchain_blocks_requests and self.app_state.blockchain_blocks_requests[request_id] is not None:
                return self.app_state.blockchain_blocks_requests[request_id]
            await asyncio.sleep(1)  # Polling interval

    async def submit_block_result(self, request_id, result):
        # Update the request data in app_state.blockchain_blocks_requests
        if request_id in self.app_state.blockchain_blocks_requests:
            self.app_state.blockchain_blocks_requests[request_id] = str(result)

    async def resolve_txns(self, request_id):
        while True:
            if request_id in self.app_state.blockchain_txns_requests and self.app_state.blockchain_txns_requests[request_id] is not None:
                return self.app_state.blockchain_txns_requests[request_id]
            await asyncio.sleep(1)  # Polling interval

    async def submit_txns_result(self, request_id, result):
        # Update the request data in app_state.blockchain_txns_requests
        if request_id in self.app_state.blockchain_txns_requests:
            self.app_state.blockchain_txns_requests[request_id] = str(result)

    async def send_txn(self, private_key, receiver, amount, data, fee="0"):
        try:
            # Generate public key from private key
            sk = SigningKey.from_string(bytes.fromhex(private_key), curve=SECP256k1)
            vk = sk.get_verifying_key()
            public_key = vk.to_string("uncompressed").hex()
            address = await self.generate_address(bytes.fromhex(public_key))

            # Retrieve the sender wallet from the cache
            sender_wallet = self.app_state.wallets.get(address)
            if not sender_wallet:
                return "Sender wallet not found"

            # Check if the sender has sufficient balance
            if int(sender_wallet['balance']) < int(amount) + int(fee):
                return "Insufficient balance"

            # Create and add the transaction
            transaction = await self.add_transaction(address, receiver, amount, data, fee)
            txid = { "txid": transaction['txid'] }
            return txid

        except Exception as e:
            print(f"Error sending transaction: {e}")
            return f"Error sending transaction: {e}"
