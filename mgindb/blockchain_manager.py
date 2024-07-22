import ujson  # Module for JSON operations
import asyncio
import os  # Module for interacting with the operating system
import time  # Module for time-related functions
import hashlib  # Module for cryptographic hash functions
from mnemonic import Mnemonic
from ecdsa import SigningKey, SECP256k1
from cryptography.fernet import Fernet
import base64
import base58
from .app_state import AppState  # Import application state management
from .constants import BLOCKCHAIN_FILE, PENDING_TRANSACTIONS_FILE, WALLETS_FILE  # Import constant for blockchain and wallets
from .config import save_config  # Import config loading and saving
from .sub_pub_manager import SubPubManager  # Publish/subscribe management

class BlockchainManager:
    def __init__(self):
        """Initialize BlockchainManager with application state and data file path."""
        self.app_state = AppState()
        self.blockchain_file = BLOCKCHAIN_FILE
        self.pending_transactions_file = PENDING_TRANSACTIONS_FILE
        self.wallets_file = WALLETS_FILE
        self.sub_pub_manager = SubPubManager()
        self.tx_per_block = self.app_state.config_store.get('BLOCKCHAIN_TX_PER_BLOCK')
        self.sync_chunks = self.app_state.config_store.get('BLOCKCHAIN_SYNC_CHUNKS')
        self.accumulated_transactions = []  # List to store accumulated transactions
    
    async def has_blockchain(self):
        return self.app_state.config_store.get('BLOCKCHAIN') == '1'

    async def get_blockchain(self, args=None):
        """Return the entire blockchain or from a specific index."""
        if args and args.strip().startswith('FROM'):
            _, index = args.split(' ')
            index = int(index)
            blockchain_data = self.app_state.blockchain[index:].copy()
        else:
            blockchain_data = self.app_state.blockchain.copy()
            
        chunk_size = int(self.sync_chunks)
        return self.chunk_data(blockchain_data, chunk_size)

    async def chunk_data(self, data, chunk_size):
        """
        Chunk the data into smaller pieces for sending via WebSocket.
        """
        total_chunks = (len(data) + chunk_size - 1) // chunk_size
        for i in range(total_chunks):
            start_index = i * chunk_size
            end_index = start_index + chunk_size
            chunk = data[start_index:end_index]
            yield ujson.dumps({
                "chunk_index": i,
                "total_chunks": total_chunks,
                "data": chunk
            })
            await asyncio.sleep(1)
        yield ujson.dumps({"end": True})

    async def load_blockchain(self):
        """
        Load data from the blockchain file into the application state.

        If the data file does not exist, create an empty one. Attempt to load
        the data from the file and update the application state. Handle JSON
        decode errors and return an empty dictionary if an error occurs.
        """
        if not os.path.exists(self.blockchain_file):
            with open(self.blockchain_file, mode='w', encoding='utf-8') as file:
                ujson.dump([], file)
            await self.create_genesis_block()
        else:
            try:
                with open(self.blockchain_file, mode='r', encoding='utf-8') as file:
                    loaded_blockchain = ujson.load(file)
                if not loaded_blockchain:
                    await self.create_genesis_block()
                else:
                    self.app_state.blockchain = loaded_blockchain
            except ujson.JSONDecodeError as e:
                print(f"Failed to load blockchain: {e}")
                # Handle the error by initializing the blockchain
                await self.create_genesis_block()
    
    async def load_blockchain_pending_transactions(self):
        """
        Load data from the pending transactions file into the application state.

        If the data file does not exist, create an empty one. Attempt to load
        the data from the file and update the application state. Handle JSON
        decode errors and return an empty dictionary if an error occurs.
        """
        if not os.path.exists(self.pending_transactions_file):
            with open(self.pending_transactions_file, mode='w', encoding='utf-8') as file:
                ujson.dump([], file)
        else:
            try:
                with open(self.pending_transactions_file, mode='r', encoding='utf-8') as file:
                    loaded_pending_transactions = ujson.load(file)
                self.app_state.pending_transactions = loaded_pending_transactions
            except ujson.JSONDecodeError as e:
                print(f"Failed to load pending transactions: {e}")

    async def load_blockchain_wallets(self):
        """
        Load data from the wallets file into the application state.

        If the data file does not exist, create an empty one. Attempt to load
        the data from the file and update the application state. Handle JSON
        decode errors and return an empty dictionary if an error occurs.
        """
        if not os.path.exists(self.wallets_file):
            with open(self.wallets_file, mode='w', encoding='utf-8') as file:
                ujson.dump([], file)
        else:
            try:
                with open(self.wallets_file, mode='r', encoding='utf-8') as file:
                    loaded_wallets = ujson.load(file)
                self.app_state.wallets = loaded_wallets
            except ujson.JSONDecodeError as e:
                print(f"Failed to load wallets: {e}")

    def save_blockchain(self):
        """
        Save the current data in the application state to the blockchain file.

        If there have been changes to the data, write the updated data store
        to the data file and reset the data change flag. Handle I/O errors.
        """
        try:
            if self.app_state.blockchain_has_changed:
                with open(self.blockchain_file, mode='w', encoding='utf-8') as file:
                    ujson.dump(self.app_state.blockchain, file, indent=4)
                    self.app_state.blockchain_has_changed = False
        except IOError as e:
            print(f"Failed to save blockchain: {e}")
    
    def save_blockchain_pending_transactions(self):
        """
        Save the current data in the application state to the pending transactions file.

        If there have been changes to the data, write the updated data store
        to the data file and reset the data change flag. Handle I/O errors.
        """
        try:
            if self.app_state.blockchain_pending_transactions_has_changed:
                with open(self.pending_transactions_file, mode='w', encoding='utf-8') as file:
                    ujson.dump(self.app_state.pending_transactions, file, indent=4)
                    self.app_state.blockchain_pending_transactions_has_changed = False
        except IOError as e:
            print(f"Failed to save pending transactions: {e}")
    
    def save_blockchain_wallets(self):
        """
        Save the current data in the application state to the wallets file.

        If there have been changes to the data, write the updated data store
        to the data file and reset the data change flag. Handle I/O errors.
        """
        try:
            if self.app_state.blockchain_wallets_has_changed:
                with open(self.wallets_file, mode='w', encoding='utf-8') as file:
                    ujson.dump(self.app_state.wallets, file, indent=4)
                    self.app_state.blockchain_wallets_has_changed = False
        except IOError as e:
            print(f"Failed to save wallets: {e}")

    async def create_genesis_block(self):
        """Create the genesis wallet"""
        wallet = await self.new_wallet()
        """Create the genesis block and initialize blockchain configuration."""
        blockchain_supply = self.app_state.config_store.get('BLOCKCHAIN_SUPPLY', '0')
        genesis_block = {
            'index': 0,
            'timestamp': int(time.time()),
            'nonce': 0,
            'difficulty': 1,
            'validation_time': 0,
            'size': 0,
            'previous_hash': '0',
            'hash': '',
            'txid': '',
            'sender': '',
            'receiver': wallet.get('address'),
            'amount': blockchain_supply,
            'data': 'Genesis Block',
            'fee': '0'
        }
        txid = await self.hash_data(genesis_block)
        genesis_block['txid'] = txid
        genesis_block['hash'] = await self.calculate_hash(genesis_block)
        self.app_state.blockchain.append(genesis_block)
        self.app_state.blockchain_has_changed = True
        self.save_blockchain()

        genesis_wallet = self.app_state.wallets.get(wallet.get('address'))
        if genesis_wallet:
            genesis_wallet['balance'] = blockchain_supply
            self.app_state.blockchain_wallets_has_changed = True
            self.save_blockchain_wallets()

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
            self.app_state.pending_transactions = [
                tx for tx in self.app_state.pending_transactions
                if tx['txid'] != transaction['txid']
            ]

            # Save pending transactions
            self.app_state.blockchain_pending_transactions_has_changed = True
            if not self.scheduler_manager.is_scheduler_active():
                self.save_blockchain_pending_transactions()
            
            # Tx per block
            if len(self.accumulated_transactions) >= int(self.tx_per_block):
                await self.create_and_save_block()

            return ujson.dumps({
                "confirmation": transaction
            })
        except Exception as e:
            print(f"Error adding transaction: {e}")
            return "Error adding transaction"

    async def create_and_save_block(self):
        from .scheduler import SchedulerManager  # Scheduler management
        self.scheduler_manager = SchedulerManager()  # Manages the scheduler

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
            'fee': str(sum(int(tx['fee']) for tx in self.accumulated_transactions))
        }

        # Clear the accumulated transactions
        self.accumulated_transactions = []
        
        block['hash'] = await self.calculate_hash(block)
        mined_block = await self.mine_block(block, block['difficulty'])
        
        # Save the block to the blockchain
        self.app_state.blockchain.append(mined_block)
        
        self.app_state.blockchain_has_changed = True
        if not self.scheduler_manager.is_scheduler_active():
            self.save_blockchain()

        # Update blockchain configuration
        new_difficulty = self.adjust_difficulty(mined_block['validation_time'])
        await self.update_blockchain_data(mined_block, new_difficulty)

        # Dictionary to accumulate rewards for each validator
        validator_rewards = {}

        # Update wallets data
        for txn in mined_block['data']:
            sender_wallet = self.app_state.wallets.get(txn['sender'])
            receiver_wallet = self.app_state.wallets.get(txn['receiver'])
            validator_address = txn['validator']
            tx_data = f"{txn['txid']}:{block['index']}"

            if sender_wallet:
                sender_wallet['balance'] = str(int(sender_wallet['balance']) - int(txn['amount']) - int(txn['fee']))
                sender_wallet['last_tx_timestamp'] = mined_block['timestamp']
                sender_wallet['tx_count'] += 1
                sender_wallet['tx_data'].append(tx_data)

            if receiver_wallet and receiver_wallet != sender_wallet:
                receiver_wallet['balance'] = str(int(receiver_wallet['balance']) + int(txn['amount']))
                receiver_wallet['last_tx_timestamp'] = mined_block['timestamp']
                receiver_wallet['tx_count'] += 1
                receiver_wallet['tx_data'].append(tx_data)
            
            if validator_address:
                if validator_address not in validator_rewards:
                    validator_rewards[validator_address] = 0
                validator_rewards[validator_address] += int(self.app_state.config_store['BLOCKCHAIN_VALIDATOR_REWARD'])

        # Save blockchain wallets
        self.app_state.blockchain_wallets_has_changed = True
        if not self.scheduler_manager.is_scheduler_active():
            self.save_blockchain_wallets()
        
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
        self.app_state.pending_transactions.append(transaction)

        # Save pending transactions
        self.app_state.blockchain_pending_transactions_has_changed = True
        if not self.scheduler_manager.is_scheduler_active():
            self.save_blockchain_pending_transactions()
        
        await self.sub_pub_manager.notify_nodes('transaction', transaction)
        return transaction

    async def update_blockchain_data(self, block, difficulty):
        blockchain_data = self.app_state.config_store['BLOCKCHAIN_CONF']
        blockchain_data['previous_hash'] = block['hash']
        blockchain_data['chain_length'] = block['index'] + 1
        blockchain_data['latest_block'] = block['timestamp']
        blockchain_data['validation_time'] = block['validation_time']
        blockchain_data['difficulty'] = difficulty
        
        save_config()
    
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
                "words": encrypted_words,
                "private_key": encrypted_private_key,
                "public_key": public_key,
                "balance": "0",
                "tx_count": 0,
                "tx_data": [],
                "last_tx_timestamp": "",
                "created_at": int(time.time())
            }

            self.app_state.wallets[address] = encrypted_wallet_data

            # Save blockchain wallets
            self.app_state.blockchain_wallets_has_changed = True
            if not self.scheduler_manager.is_scheduler_active():
                self.save_blockchain_wallets()

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
    
    async def get_tx(self, *args, **kwargs):
        pass
