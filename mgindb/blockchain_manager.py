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
from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor(max_workers=10)

class BlockchainManager:
    def __init__(self):
        try:
            """Initialize BlockchainManager with application state and data file path."""
            self.app_state = AppState()
            self.blockchain_db = BLOCKCHAIN_DB
            self.pending_transactions_file = PENDING_TRANSACTIONS_FILE
            self.sub_pub_manager = SubPubManager()
            self.last_block_time = time.time()
            self._init_blockchain()
        except Exception as e:
            print(f"Error initializing BlockchainManager: {e}")

    def _init_blockchain(self):
        try:
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
                    tx_count INTEGER,
                    tx_data TEXT,
                    last_tx_timestamp TEXT,
                    balances TEXT
                )
            ''')
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS contracts (
                    contract_hash TEXT PRIMARY KEY,
                    owner_address TEXT,
                    name TEXT,
                    description TEXT,
                    logo TEXT,
                    symbol TEXT,
                    supply INTEGER,
                    max_supply INTEGER,
                    can_mint INTEGER,
                    can_burn INTEGER,
                    created_at INTEGER,
                    updated_at INTEGER
                )
            ''')
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Error initializing SQLite database: {e}")

    async def has_blockchain(self):
        try:
            return self.app_state.config_store.get("BLOCKCHAIN") == "1"
        except Exception as e:
            print(f"Error checking blockchain status: {e}")
            return False

    async def is_blockchain_master(self):
        try:
            return self.app_state.config_store.get("BLOCKCHAIN_TYPE") == "MASTER"
        except Exception as e:
            print(f"Error checking blockchain master status: {e}")
            return False

    def get_blockchain(self, args=None):
        try:
            """Return the entire blockchain or from a specific index."""
            if args and args.strip().startswith("FROM"):
                _, index = args.split(" ")
                index = int(index)
                blockchain_data = [block for block in self.app_state.blockchain if block["index"] > index]
            else:
                blockchain_data = self.app_state.blockchain

            chunks = list(self.chunk_data(blockchain_data))  # Convert generator to list for debug print
            return chunks
        except Exception as e:
            print(f"Error getting blockchain: {e}")
            return []

    def chunk_data(self, data):
        try:
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
        except Exception as e:
            print(f"Error chunking data: {e}")

    def format_block(self, row):
        try:
            """
            Convert a database row into a block format.
            """
            block = {
                "index": row[1],
                "timestamp": row[2],
                "nonce": row[3],
                "difficulty": row[4],
                "validation_time": row[5],
                "size": row[6],
                "previous_hash": row[7],
                "hash": row[8],
                "checksum": row[9],
                "data": ujson.loads(row[10]),
                "fee": row[11],
                "validator": row[12]
            }
            return block
        except Exception as e:
            print(f"Error formatting block: {e}")
            return {}

    async def load_blockchain(self):
        try:
            """
            Load data from the blockchain table into the application state.
            """
            with sqlite3.connect(self.blockchain_db) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM blockchain")
                loaded_blockchain = cursor.fetchall()
                if not loaded_blockchain:
                    await self.create_genesis_block()
                else:
                    self.app_state.blockchain = [
                        self.format_block(row)
                        for row in loaded_blockchain
                    ]
        except sqlite3.Error as e:
            print(f"Failed to load blockchain: {e}")
            await self.create_genesis_block()

    async def load_blockchain_pending_transactions(self):
        try:
            """
            Load data from the pending transactions file into the application state.
            """
            if not os.path.exists(self.pending_transactions_file):
                with open(self.pending_transactions_file, mode="w", encoding="utf-8") as file:
                    ujson.dump([], file)
            else:
                try:
                    with open(self.pending_transactions_file, mode="r", encoding="utf-8") as file:
                        loaded_pending_transactions = ujson.load(file)
                    self.app_state.blockchain_pending_transactions = loaded_pending_transactions
                except ujson.JSONDecodeError as e:
                    print(f"Failed to load pending transactions: {e}")
        except Exception as e:
            print(f"Error loading blockchain pending transactions: {e}")

    async def load_blockchain_wallets(self):
        try:
            """
            Load data from the wallets table into the application state.
            """
            with sqlite3.connect(self.blockchain_db) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM wallets")
                loaded_wallets = cursor.fetchall()
                if not loaded_wallets:
                    self.app_state.wallets = {}
                else:
                    self.app_state.wallets = {
                        wallet[0]: {
                            "tx_count": wallet[1],
                            "tx_data": ujson.loads(wallet[2]),
                            "last_tx_timestamp": wallet[3],
                            "balances": ujson.loads(wallet[4])
                        } for wallet in loaded_wallets
                    }
        except sqlite3.Error as e:
            print(f"Failed to load wallets: {e}")
            # Optionally, initialize wallets to an empty state
            self.app_state.wallets = {}

    async def save_blockchain(self, block):
        try:
            """
            Save the provided block data to the blockchain table.
            """
            self.cursor.execute('''
                INSERT INTO blockchain (block_index, timestamp, nonce, difficulty, validation_time, size, previous_hash, hash, checksum, data, fee, validator)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                block["index"],
                block["timestamp"],
                block["nonce"],
                block["difficulty"],
                block["validation_time"],
                block["size"],
                block["previous_hash"],
                block["hash"],
                block["checksum"],
                ujson.dumps(block["data"]),
                block["fee"],
                block["validator"]
            ))
            self.conn.commit()
        except IOError as e:
            print(f"Failed to save blockchain: {e}")

    async def save_blockchain_pending_transactions(self):
        try:
            """
            Save the current data in the application state to the pending transactions file.
            """
            if self.app_state.blockchain_pending_transactions_has_changed:
                with open(self.pending_transactions_file, mode="w", encoding="utf-8") as file:
                    ujson.dump(self.app_state.blockchain_pending_transactions, file, indent=4)
                    self.app_state.blockchain_pending_transactions_has_changed = False
        except IOError as e:
            print(f"Failed to save pending transactions: {e}")

    async def save_blockchain_wallets(self, address, wallet):
        try:
            """
            Save the provided wallet data to the wallets table.
            """
            self.cursor.execute('''
                INSERT OR REPLACE INTO wallets (address, tx_count, tx_data, last_tx_timestamp, balances)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                address,
                wallet["tx_count"],
                ujson.dumps(wallet["tx_data"]),
                wallet["last_tx_timestamp"],
                ujson.dumps(wallet["balances"])
            ))
            self.conn.commit()
        except IOError as e:
            print(f"Failed to save wallets: {e}")

    async def save_contract(self, contract):
        try:
            """
            Save the provided contract data to the contracts table.
            """
            self.cursor.execute('''
                INSERT INTO contracts (contract_hash, owner_address, name, description, logo, symbol, supply, max_supply, can_mint, can_burn, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                contract["contract_hash"],
                contract["owner_address"],
                contract["name"],
                contract["description"],
                contract["logo"],
                contract["symbol"],
                contract["supply"],
                contract["max_supply"],
                int(contract["can_mint"]),
                int(contract["can_burn"]),
                contract["created_at"],
                contract["updated_at"]
            ))
            self.conn.commit()
        except IOError as e:
            print(f"Failed to save contract: {e}")

    async def create_genesis_block(self):
        try:
            """Create the genesis wallet"""
            wallet = await self.new_wallet()
            genesis_address = wallet.get("address")
            genesis_private_key = wallet.get("private_key")
            
            """Create the genesis contract"""
            genesis_contract = {
                "owner_address": genesis_address,
                "name": self.app_state.config_store.get("BLOCKCHAIN_NAME"),
                "description": self.app_state.config_store.get("BLOCKCHAIN_DESCRIPTION"),
                "logo": self.app_state.config_store.get("BLOCKCHAIN_LOGO"),
                "symbol": self.app_state.config_store.get("BLOCKCHAIN_SYMBOL"),
                "supply": int(self.app_state.config_store.get("BLOCKCHAIN_SUPPLY", "0")),
                "max_supply": int(self.app_state.config_store.get("BLOCKCHAIN_MAX_SUPPLY", "0")),
                "can_mint": self.app_state.config_store.get("BLOCKCHAIN_CAN_MINT", False),
                "can_burn": self.app_state.config_store.get("BLOCKCHAIN_CAN_BURN", False)
            }
            
            genesis_contract_hash = await self.hash_data(genesis_contract)
            genesis_contract["contract_hash"] = genesis_contract_hash
            genesis_contract["created_at"] = int(time.time())
            genesis_contract["updated_at"] = int(time.time())
            await self.save_contract(genesis_contract)

            """Create the genesis block and initialize blockchain configuration."""
            blockchain_supply = genesis_contract["supply"]

            genesis_transaction = {
                "nonce": 0,
                "difficulty": 0,
                "validation_time": 0,
                "size": 0,
                "hash": "",
                "checksum": "",
                "txid": "",
                "sender": "",
                "receiver": genesis_address,
                "validator": "",
                "amount": blockchain_supply,
                "symbol": genesis_contract["symbol"],
                "data": "",
                "fee": "0",
                "timestamp": int(time.time()),
                "confirmed": True
            }

            tx_size = len(ujson.dumps(genesis_transaction).encode())
            genesis_transaction["txid"] = await self.hash_data(genesis_transaction)
            genesis_transaction["hash"] = await self.calculate_hash(genesis_transaction)
            genesis_transaction["size"] = tx_size
            genesis_transaction["checksum"] = self.calculate_checksum(genesis_transaction)

            genesis_block = {
                "index": 0,
                "timestamp": int(time.time()),
                "nonce": 0,
                "difficulty": 1,
                "validation_time": 0,
                "size": tx_size,
                "previous_hash": "0",
                "hash": "",
                "checksum": "",
                "data": [genesis_transaction],
                "fee": "0",
                "validator": ''
            }

            genesis_block["hash"] = await self.calculate_hash(genesis_block)
            genesis_block["checksum"] = self.calculate_checksum(genesis_block)
            self.app_state.blockchain.append(genesis_block)
            await self.save_blockchain(genesis_block)

            genesis_wallet = self.app_state.wallets.get(wallet.get("address"))
            if genesis_wallet:
                tx_data = {
                    "block_index": genesis_block["index"],
                    "txid": genesis_transaction["txid"],
                    "hash": genesis_transaction["hash"],
                    "amount": blockchain_supply,
                    "symbol": genesis_contract["symbol"],
                    "fee": genesis_transaction["fee"],
                    "sender": genesis_transaction["sender"],
                    "receiver": genesis_transaction["receiver"],
                    "timestamp": genesis_block["timestamp"],
                    "confirmed": True
                }

                genesis_wallet["last_tx_timestamp"] = genesis_transaction["timestamp"]
                genesis_wallet["tx_count"] += 1
                genesis_wallet["tx_data"].append(tx_data)
                genesis_wallet["balances"] = {genesis_contract["symbol"]: {"balance": str(blockchain_supply), "balance_pending": "0"}}
                await self.save_blockchain_wallets(genesis_address, genesis_wallet)

            # Initialize blockchain configuration
            self.app_state.config_store["BLOCKCHAIN_CONF"] = {
                "genesis_address": genesis_address,
                "genesis_private_key": genesis_private_key,
                "genesis_contract_hash": genesis_contract_hash,
                "chain_length": 1,
                "previous_hash": genesis_block["hash"],
                "latest_block": int(genesis_block["timestamp"]),
                "validation_time": genesis_block["validation_time"],
                "difficulty": genesis_block["difficulty"],
                "fee": self.app_state.config_store["BLOCKCHAIN_SETUP_FEE"]
            }
            save_config()
        except Exception as e:
            print(f"Error creating genesis block: {e}")

    async def calculate_hash(self, block):
        try:
            block_string = ujson.dumps(block, sort_keys=True).encode()
            return hashlib.sha256(block_string).hexdigest()
        except Exception as e:
            print(f"Error calculating hash: {e}")
            return ""

    async def hash_data(self, data):
        try:
            """Hash the data using SHA-256."""
            data_string = ujson.dumps(data, sort_keys=True).encode()
            return hashlib.sha256(data_string).hexdigest()
        except Exception as e:
            print(f"Error hashing data: {e}")
            return ""

    async def create_and_save_block(self, mined_block):
        try:
            print("creating and saving block")
            # Blockchain details
            blockchain_data = self.app_state.config_store["BLOCKCHAIN_CONF"]
            chain_length = blockchain_data["chain_length"]
            previous_hash = blockchain_data["previous_hash"]

            mined_block["index"] = chain_length
            mined_block["previous_hash"] = previous_hash
            blockchain_data["chain_length"] = chain_length + 1

            # Save the block to the blockchain
            self.app_state.blockchain.append(mined_block)
            await self.save_blockchain(mined_block)

            # Update blockchain configuration
            new_difficulty = self.adjust_difficulty(mined_block["validation_time"])
            await self.update_blockchain_data(mined_block, new_difficulty)

            # Dictionary to accumulate rewards for each validator
            validator_rewards = {}

            # Update wallets data
            for txn in mined_block["data"]:
                # Process wallets
                await self._process_wallets(mined_block, txn, validator_rewards)

                # Handle mint and burn transactions
                if txn.get("action") == "mint":
                    await self._process_mint(txn)
                elif txn.get("action") == "burn":
                    await self._process_burn(txn)
        
            # Create a single transaction per validator with the total rewards
            await self._create_validator_reward(mined_block, validator_rewards)

            # Notify nodes
            await self.sub_pub_manager.notify_nodes("new_block", [mined_block], None, node_type="FULL")

            return None
        except Exception as e:
            print(f"Error creating and saving block: {e}")
            return False  # Return False if an exception occurred

    async def _process_wallets(self, mined_block, txn, validator_rewards):
        try:
            wallet_template = {
                "tx_count": 0,
                "tx_data": [],
                "last_tx_timestamp": "",
                "balances": {}
            }

            genesis_address = self.app_state.config_store["BLOCKCHAIN_CONF"]["genesis_address"]
            sender_address = txn["sender"]
            receiver_address = txn["receiver"]
            validator_address = txn["validator"]

            # Ensure genesis wallet exists
            if genesis_address not in self.app_state.wallets:
                self.app_state.wallets[genesis_address] = wallet_template.copy()
            genesis_wallet = self.app_state.wallets[genesis_address]

            # Ensure sender wallet exists
            if sender_address not in self.app_state.wallets:
                self.app_state.wallets[sender_address] = wallet_template.copy()
            sender_wallet = self.app_state.wallets[sender_address]

            # Ensure receiver wallet exists
            if receiver_address not in self.app_state.wallets:
                self.app_state.wallets[receiver_address] = wallet_template.copy()
            receiver_wallet = self.app_state.wallets[receiver_address]

            tx_data = {
                "timestamp": txn["timestamp"],
                "block_index": mined_block["index"],
                "txid": txn["txid"],
                "hash": txn["hash"],
                "amount": txn["amount"],
                "symbol": txn["symbol"],
                "fee": txn["fee"],
                "sender": txn["sender"],
                "receiver": txn["receiver"],
                "action": txn["action"],
                "contract_hash": txn["contract_hash"],
                "confirmed": txn["confirmed"]
            }

            blockchain_symbol = self.app_state.config_store["BLOCKCHAIN_SYMBOL"]
            symbol = txn["symbol"]
            amount = int(txn["amount"])
            fee = int(txn["fee"])

            # Initialize balances if not present
            genesis_wallet["balances"].setdefault(blockchain_symbol, {"balance": "0", "balance_pending": "0"})
            sender_wallet["balances"].setdefault(blockchain_symbol, {"balance": "0", "balance_pending": "0"})
            receiver_wallet["balances"].setdefault(blockchain_symbol, {"balance": "0", "balance_pending": "0"})

            sender_wallet["balances"].setdefault(symbol, {"balance": "0", "balance_pending": "0"})
            receiver_wallet["balances"].setdefault(symbol, {"balance": "0", "balance_pending": "0"})

            # Update sender wallet
            if sender_wallet:
                try:
                    if txn["action"] not in ["create_contract", "mint"]:
                        sender_wallet["balances"][symbol]["balance_pending"] = str(int(sender_wallet["balances"][symbol]["balance_pending"]) + amount)

                    sender_wallet["balances"][blockchain_symbol]["balance_pending"] = str(int(sender_wallet["balances"][blockchain_symbol]["balance_pending"]) + fee)
                    sender_wallet["last_tx_timestamp"] = mined_block["timestamp"]
                    existing_tx = next((tx for tx in sender_wallet["tx_data"] if tx["txid"] == tx_data["txid"]), None)
                    if existing_tx:
                        sender_wallet["tx_data"][sender_wallet["tx_data"].index(existing_tx)] = tx_data
                    else:
                        sender_wallet["tx_data"].append(tx_data)

                    self.app_state.data_store["wallets"][sender_address] = sender_wallet
                    await self.save_blockchain_wallets(sender_address, sender_wallet)
                except Exception as e:
                    print(f"Error updating sender's wallet: {e}")

            # Update receiver wallet
            if receiver_wallet:
                try:
                    receiver_wallet["balances"][symbol]["balance"] = str(int(receiver_wallet["balances"][symbol]["balance"]) + amount)
                    receiver_wallet["balances"][symbol]["balance_pending"] = str(int(receiver_wallet["balances"][symbol]["balance_pending"]) - amount)

                    receiver_wallet["last_tx_timestamp"] = mined_block["timestamp"]
                    existing_tx = next((tx for tx in receiver_wallet["tx_data"] if tx["txid"] == tx_data["txid"]), None)
                    if existing_tx:
                        receiver_wallet["tx_data"][receiver_wallet["tx_data"].index(existing_tx)] = tx_data
                    else:
                        receiver_wallet["tx_data"].append(tx_data)

                    self.app_state.data_store["wallets"][receiver_address] = receiver_wallet
                    await self.save_blockchain_wallets(receiver_address, receiver_wallet)
                except Exception as e:
                    print(f"Error updating receiver's wallet: {e}")

            # Update genesis wallet
            if genesis_wallet:
                try:
                    genesis_wallet["balances"][blockchain_symbol]["balance"] = str(int(genesis_wallet["balances"][blockchain_symbol]["balance"]) + fee)
                    genesis_wallet["balances"][blockchain_symbol]["balance_pending"] = str(int(genesis_wallet["balances"][blockchain_symbol]["balance_pending"]) - fee)

                    self.app_state.data_store["wallets"][genesis_address] = genesis_wallet
                    await self.save_blockchain_wallets(genesis_address, genesis_wallet)
                except Exception as e:
                    print(f"Error updating genesis' wallet: {e}")

            # Update validator rewards
            if validator_address:
                if validator_address not in validator_rewards:
                    validator_rewards[validator_address] = 0
                validator_rewards[validator_address] += int(self.app_state.config_store["BLOCKCHAIN_VALIDATOR_REWARD"])

            # Save to cache
            self.app_state.data_has_changed = True

        except KeyError as e:
            print(f"Missing key in transaction: {e}")
            return False
        except ValueError as e:
            print(f"Value error in transaction: {e}")
            return False
        except Exception as e:
            print(f"Error processing wallets: {e}")
            return False  # Error processing wallets
    
    async def _process_mint(self, txn):
        try:
            contract_hash = txn["contract_hash"]
            amount = int(txn["amount"])
            receiver_address = txn["receiver"]

            # Update contract supply
            self.cursor.execute('''
                SELECT * FROM contracts WHERE contract_hash=?
            ''', (contract_hash,))
            contract = self.cursor.fetchone()

            if contract:
                new_supply = contract[6] + amount
                if new_supply > contract[7]:
                    raise ValueError("Amount exceeds max supply")

                self.cursor.execute('''
                    UPDATE contracts SET supply=?, updated_at=? WHERE contract_hash=?
                ''', (new_supply, int(time.time()), contract_hash))
                self.conn.commit()

                # Update receiver's wallet
                receiver_wallet = self.app_state.wallets.get(receiver_address)
                if receiver_wallet:
                    symbol = contract[5]
                    if symbol not in receiver_wallet["balances"]:
                        receiver_wallet["balances"][symbol] = {"balance": "0", "balance_pending": "0"}

                    receiver_wallet["balances"][symbol]["balance"] = str(int(receiver_wallet["balances"][symbol]["balance"]) + amount)
                    receiver_wallet["last_tx_timestamp"] = int(time.time())
                    self.app_state.wallets[receiver_address] = receiver_wallet
                    self.app_state.data_store["wallets"][receiver_address] = receiver_wallet
                    await self.save_blockchain_wallets(receiver_address, receiver_wallet)
        except Exception as e:
            print(f"Error processing mint: {e}")

    async def _process_burn(self, txn):
        try:
            contract_hash = txn["contract_hash"]
            amount = int(txn["amount"])
            sender_address = txn["sender"]

            # Update contract supply
            self.cursor.execute('''
                SELECT * FROM contracts WHERE contract_hash=?
            ''', (contract_hash,))
            contract = self.cursor.fetchone()

            if contract:
                new_supply = contract[6] - amount
                if new_supply < 0:
                    raise ValueError("Burn amount exceeds current supply")

                new_max_supply = contract[7]
                if not contract[8]:  # can_mint
                    new_max_supply -= amount

                self.cursor.execute('''
                    UPDATE contracts SET supply=?, max_supply=?, updated_at=? WHERE contract_hash=?
                ''', (new_supply, new_max_supply, int(time.time()), contract_hash))
                self.conn.commit()

                # Update sender's wallet
                sender_wallet = self.app_state.wallets.get(sender_address)
                if sender_wallet:
                    symbol = contract[5]
                    if symbol not in sender_wallet["balances"]:
                        sender_wallet["balances"][symbol] = {"balance": "0", "balance_pending": "0"}

                    sender_wallet["balances"][symbol]["balance"] = str(int(sender_wallet["balances"][symbol]["balance"]) - amount)
                    sender_wallet["last_tx_timestamp"] = int(time.time())
                    self.app_state.wallets[sender_address] = sender_wallet
                    self.app_state.data_store["wallets"][sender_address] = sender_wallet
                    self.app_state.data_has_changed = True
                    await self.save_blockchain_wallets(sender_address, sender_wallet)
        except Exception as e:
            print(f"Error processing burn: {e}")

    async def _create_validator_reward(self, mined_block, validator_rewards):
        try:
            # Create a single transaction per validator with the total rewards
            genesis_address = self.app_state.config_store["BLOCKCHAIN_CONF"]["genesis_address"]
            for validator_address, total_reward in validator_rewards.items():
                validator_data = {
                    "Validator reward": [txn["hash"] for txn in mined_block["data"] if txn["validator"] == validator_address]
                }
                payload = {
                    "sender": genesis_address,
                    "receiver": validator_address,
                    "amount": str(total_reward),
                    "data": str(validator_data),  # Convert dictionary to string
                    "symbol": self.app_state.config_store["BLOCKCHAIN_SYMBOL"],
                    "action": "validator_reward"
                }
                print("add_validator_reward", payload)
                await self.send_internal_txn(payload)
        except Exception as e:
            print(f"Error creating validator reward: {e}")
            return False  # Error creating validator reward

    def adjust_difficulty(self, validation_time):
        try:
            target_time = 5  # Target time per block in seconds

            current_difficulty = self.app_state.config_store["BLOCKCHAIN_CONF"]["difficulty"]

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
        except Exception as e:
            print(f"Error adjusting difficulty: {e}")
            return current_difficulty

    def validate_block(self, block):
        try:
            previous_block = self.app_state.blockchain[block["index"] - 1]
            if block["previous_hash"] != previous_block["hash"]:
                return False
            if block["hash"] != self.calculate_hash(block):
                return False
            if block["hash"][:block["difficulty"]] != "0" * block["difficulty"]:
                return False
            return True
        except Exception as e:
            print(f"Error validating block: {e}")
            return False

    def validate_chain(self):
        try:
            for i in range(1, len(self.app_state.blockchain)):
                if not self.validate_block(self.app_state.blockchain[i]):
                    return False
            return True
        except Exception as e:
            print(f"Error validating chain: {e}")
            return False

    async def add_txn(self, sender, receiver, amount, symbol, data="", fee="0", action="", contract_hash=""):
        from .scheduler import SchedulerManager  # Scheduler management
        try:
            self.scheduler_manager = SchedulerManager()  # Manages the scheduler

            # Blockchain config
            block_max_size = int(self.app_state.config_store["BLOCKCHAIN_BLOCK_MAX_SIZE"])
            difficulty = self.app_state.config_store["BLOCKCHAIN_CONF"]["difficulty"]

            # Convert sender address to bytes
            address_bytes = sender.encode("utf-8")

            # Compute SHA-256 hash of the address bytes
            sha256_hash = hashlib.sha256(address_bytes).digest()

            # Encode the hash using base64 URL-safe encoding
            encryption_key = base64.urlsafe_b64encode(sha256_hash)

            # Create a Fernet encryption object with the encryption key
            fernet = Fernet(encryption_key)

            # Encrypt the data using the Fernet encryption object
            encrypted_data = fernet.encrypt(data.encode()).decode()

            transaction = {
                "timestamp": int(time.time()),
                "sender": sender,
                "receiver": receiver,
                "amount": str(int(float(amount))),
                "symbol": symbol,
                "data": encrypted_data,
                "fee": fee,
                "action": action,
                "contract_hash": contract_hash,
                "confirmed": False
            }

            txid = await self.hash_data(transaction)
            transaction["txid"] = str(txid)
            transaction["difficulty"] = difficulty
            transaction["size"] = len(ujson.dumps(transaction).encode())
            transaction["checksum"] = self.calculate_checksum(transaction)
            transaction["block_max_size"] = block_max_size
            self.app_state.blockchain_pending_transactions.append(transaction)

            # Save pending transactions
            self.app_state.blockchain_pending_transactions_has_changed = True
            if not self.scheduler_manager.is_scheduler_active():
                self.save_blockchain_pending_transactions()

            await self.sub_pub_manager.notify_node("txn", transaction, None, node_type="ALL")

            return transaction
        except Exception as e:
            print(f"Error adding transaction: {e}")
            return {}

    async def update_blockchain_data(self, block, difficulty):
        try:
            blockchain_data = self.app_state.config_store["BLOCKCHAIN_CONF"]
            blockchain_data["previous_hash"] = block["hash"]
            blockchain_data["latest_block"] = block["timestamp"]
            blockchain_data["validation_time"] = block["validation_time"]
            blockchain_data["difficulty"] = difficulty
            
            save_config()
        except Exception as e:
            print(f"Error updating blockchain data: {e}")

    def calculate_checksum(self, data):
        try:
            """Calculate SHA-256 checksum of the given data."""
            data_string = ujson.dumps(data, sort_keys=True).encode()
            return hashlib.sha256(data_string).hexdigest()
        except Exception as e:
            print(f"Error calculating checksum: {e}")
            return ""

    def verify_checksum(self, data, checksum):
        try:
            """Verify the given checksum matches the calculated checksum of the data."""
            return self.calculate_checksum(data) == checksum
        except Exception as e:
            print(f"Error verifying checksum: {e}")
            return False

    async def generate_mnemonic(self):
        try:
            mnemonic = Mnemonic("english")
            words = mnemonic.generate(strength=128)
            seed = Mnemonic.to_seed(words)
            return words, seed
        except Exception as e:
            print(f"Error generating mnemonic: {e}")
            return "", b""

    async def generate_keys_from_seed(self, seed):
        try:
            sk = SigningKey.from_string(seed[:32], curve=SECP256k1)
            vk = sk.get_verifying_key()
            private_key = sk.to_string().hex()
            public_key = vk.to_string("uncompressed").hex()
            address = await self.generate_address(bytes.fromhex(public_key))
            return private_key, public_key, address
        except Exception as e:
            print(f"Error generating keys from seed: {e}")
            return "", "", ""

    async def generate_address(self, public_key):
        try:
            # Step 1: Perform SHA-256 hashing on the public key
            sha256_1 = hashlib.sha256(public_key).digest()

            # Step 2: Perform RIPEMD-160 hashing on the SHA-256 result
            ripemd160_hash = hashlib.new("ripemd160", sha256_1).digest()

            # Step 3: Add the custom prefix (e.g., 0x33 for "M")
            prefix_byte = b"\x33"  # Adjust the prefix byte to ensure the address starts with "M"
            extended_ripemd160 = prefix_byte + ripemd160_hash

            # Step 4: Perform double SHA-256 to calculate the checksum
            sha256_2 = hashlib.sha256(extended_ripemd160).digest()
            sha256_3 = hashlib.sha256(sha256_2).digest()
            checksum = sha256_3[:4]

            # Step 5: Add the checksum to the extended RIPEMD-160 result
            binary_address = extended_ripemd160 + checksum

            # Step 6: Encode the result using Base58
            base58_address = base58.b58encode(binary_address)

            return base58_address.decode("utf-8")
        except Exception as e:
            print(f"Error generating address: {e}")
            return ""

    async def new_wallet(self, *args, **kwargs):
        from .scheduler import SchedulerManager  # Scheduler management
        try:
            self.scheduler_manager = SchedulerManager()  # Manages the scheduler

            if await self.has_blockchain():
                words, seed = await self.generate_mnemonic()
                private_key, public_key, address = await self.generate_keys_from_seed(seed)

                wallet = {
                    "address": address,
                    "words": words,
                    "private_key": private_key,
                    "public_key": public_key
                }

                wallet_data = {
                    "tx_count": 0,
                    "tx_data": [],
                    "last_tx_timestamp": "",
                    "balances": {self.app_state.config_store["BLOCKCHAIN_SYMBOL"]: {"balance": "0", "balance_pending": "0"}}
                }

                self.app_state.wallets[address] = wallet_data
                
                if "wallets" not in self.app_state.data_store:
                    self.app_state.data_store["wallets"] = {}
                
                if address not in self.app_state.data_store["wallets"]:
                    self.app_state.data_store["wallets"][address] = wallet_data

                # Save blockchain wallets
                self.app_state.data_has_changed = True
                await self.save_blockchain_wallets(address, wallet_data)

                return wallet
            else: 
                return "Blockchain feature is not active. Use CONFIG SET BLOCKCHAIN 1"
        except Exception as e:
            print(f"Error creating new wallet: {e}")
            return {}

    async def get_wallet(self, *args, **kwargs):
        try:
            # Extract address from args
            address = None
            if args:
                for arg in args:
                    if isinstance(arg, str):
                        address = arg
                        break
            
            if not address:
                return "No valid address request"

            wallet_data = {
                "tx_count": "",
                "tx_data": [],
                "last_tx_timestamp": "",
                "balances": {self.app_state.config_store["BLOCKCHAIN_SYMBOL"]: {"balance": "0", "balance_pending": "0"}}
            }

            wallet = self.app_state.wallets.get(address)
            if not wallet:
                return wallet_data

            wallet_data["tx_count"] = wallet.get("tx_count")
            wallet_data["tx_data"] = wallet.get("tx_data")
            wallet_data["last_tx_timestamp"] = wallet.get("last_tx_timestamp")
            wallet_data["balances"] = wallet.get("balances")

            return wallet_data
        except Exception as e:
            print(f"Error getting wallet: {e}")
            return {}

    async def connect_wallet(self, input_value):
        try:
            if len(input_value.split()) > 1:  # Mnemonic
                mnemonic = input_value
                seed = Mnemonic.to_seed(mnemonic)
                private_key, public_key, address = await self.generate_keys_from_seed(seed)
            else:  # Private key
                private_key = input_value
                address = await self.get_address_from_private_key(private_key)

            if address:
                return {"status": "success", "address": address, "private_key": private_key}
            else:
                return {"status": "error", "message": "Wallet not found"}
        except Exception as e:
            print(f"Error connecting wallet: {e}")
            return {"status": "error", "message": str(e)}
    
    async def get_wallet_balance(self, *args, **kwargs):
        try:
            # Extract address from args
            address = None
            if args:
                for arg in args:
                    if isinstance(arg, str):
                        address = arg
                        break
            
            if not address:
                return "No valid address found in args"

            wallet = self.app_state.wallets.get(address)
            if not wallet:
                return {"balances": {}}

            wallet_data = {"balances": wallet.get("balances")}

            return wallet_data
        except Exception as e:
            print(f"Error getting wallet balance: {e}")
            return {}

    async def get_blocks(self, options):
        try:
            order_by = options.get("order", "DESC")
            latest = options.get("latest", False)
            limit_start, limit_end = (0, None)

            if "limit" in options:
                limits = options["limit"].split(",")
                if len(limits) == 2:
                    limit_start = int(limits[0])
                    limit_end = int(limits[1])

            # Generate a unique request_id
            request_id = str(uuid.uuid4())

            # Store the request_id in app_state.blockchain_txns_requests with empty data
            self.app_state.blockchain_txns_requests[request_id] = None

            # Notify nodes passing the request_id and data
            await self.sub_pub_manager.notify_node("get_blocks", options, request_id, node_type="FULL")
            
            # Wait for resolve_txns to return the result for the request_id
            result = await self.resolve_txns(request_id)

            # Remove the request entry from app_state.blockchain_txns_requests
            self.app_state.blockchain_txns_requests.pop(request_id, None)

            return ujson.dumps(result)
        except Exception as e:
            print(f"Error getting blocks: {e}")
            return ""

    async def get_block(self, *args, **kwargs):
        try:
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

            await self.sub_pub_manager.notify_node("get_block", data, request_id, node_type="FULL")

            result = await self.resolve_txns(request_id)
            self.app_state.blockchain_txns_requests.pop(request_id, None)

            return result
        except Exception as e:
            print(f"Error getting block: {e}")
            return {}

    async def get_txns(self, options):
        try:
            order_by = options.get("order", "DESC")
            latest = options.get("latest", False)
            limit_start, limit_end = (0, None)

            if "limit" in options:
                limits = options["limit"].split(",")
                if len(limits) == 2:
                    limit_start = int(limits[0])
                    limit_end = int(limits[1])

            # Generate a unique request_id
            request_id = str(uuid.uuid4())

            # Store the request_id in app_state.blockchain_txns_requests with empty data
            self.app_state.blockchain_txns_requests[request_id] = None

            # Notify nodes passing the request_id and data
            await self.sub_pub_manager.notify_node("get_txns", options, request_id, node_type="FULL")

            # Wait for resolve_txns to return the result for the request_id
            result = await self.resolve_txns(request_id)

            # Remove the request entry from app_state.blockchain_txns_requests
            self.app_state.blockchain_txns_requests.pop(request_id, None)

            return ujson.dumps(result)
        except Exception as e:
            print(f"Error getting transactions: {e}")
            return ""

    async def get_txn(self, *args, **kwargs):
        try:
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
            await self.sub_pub_manager.notify_node("get_txn", data, request_id, node_type="FULL")
            
            # Wait for resolve_txns to return the result for the request_id

            result = await self.resolve_txns(request_id)

            # Remove the request entry from app_state.blockchain_txns_requests
            self.app_state.blockchain_txns_requests.pop(request_id, None)

            return result
        except Exception as e:
            print(f"Error getting transaction: {e}")
            return {}

    async def submit_block(self, request_id, result):
        try:
            # Update the request data in app_state.blockchain_blocks_requests
            parsed_result = ujson.loads(result)
            self.app_state.blockchain_blocks_requests[request_id] = parsed_result
        except Exception as e:
            print(f"Error submitting block: {e}")

    async def resolve_blocks(self):
        try:
            # Check if there are any entries in the requests dictionary
            if self.app_state.blockchain_blocks_requests:
                # Get the first entry from the dictionary
                first_key = next(iter(self.app_state.blockchain_blocks_requests))
                result = self.app_state.blockchain_blocks_requests[first_key]
                # Remove the first entry from the dictionary
                self.app_state.blockchain_blocks_requests.pop(first_key)
                if result is not None:
                    await self.create_and_save_block(result)
            return {}
        except Exception as e:
            print(f"Error resolving blocks: {e}")
            return {}

    async def resolve_txns(self, request_id):
        try:
            while True:
                if request_id in self.app_state.blockchain_txns_requests and self.app_state.blockchain_txns_requests[request_id] is not None:
                    return self.app_state.blockchain_txns_requests[request_id]
                await asyncio.sleep(0.5)  # Polling interval
        except Exception as e:
            print(f"Error resolving transactions: {e}")
            return {}

    async def submit_txns_result(self, request_id, result):
        try:
            # Update the request data in app_state.blockchain_txns_requests
            parsed_result = ujson.loads(result)
            if request_id in self.app_state.blockchain_txns_requests:
                self.app_state.blockchain_txns_requests[request_id] = parsed_result
        except Exception as e:
            print(f"Error submitting transaction result: {e}")

    async def send_internal_txn(self, txn_data):
        try:
            sender = txn_data["sender"]
            receiver = txn_data["receiver"]
            amount = txn_data["amount"]
            symbol = txn_data["symbol"]
            data = txn_data["data"]
            fee = txn_data.get("fee", "0")
            action = txn_data.get("action", "")
            contract_hash = txn_data.get("contract_hash", "")

            # Retrieve the sender wallet from the cache
            sender_wallet = self.app_state.wallets.get(sender)
            if not sender_wallet:
                sender_wallet = {
                    "tx_count": 0,
                    "tx_data": [],
                    "last_tx_timestamp": "",
                    "balances": {}
                }

            # Retrieve or initialize the receiver wallet from the cache
            receiver_wallet = self.app_state.wallets.get(receiver)
            if not receiver_wallet:
                receiver_wallet = {
                    "tx_count": 0,
                    "tx_data": [],
                    "last_tx_timestamp": "",
                    "balances": {}
                }

            if symbol not in sender_wallet["balances"]:
                sender_wallet["balances"][symbol] = {"balance": "0", "balance_pending": "0"}

            if symbol not in receiver_wallet["balances"]:
                receiver_wallet["balances"][symbol] = {"balance": "0", "balance_pending": "0"}

            # Convert the amount and fee with decimal multiplier
            blockchain_decimal = int(self.app_state.config_store["BLOCKCHAIN_DECIMAL"])
            multiplier = 10 ** blockchain_decimal
            amount = int(float(amount) * multiplier)
            fee = int(float(fee) * multiplier)

            # Calculate the available balance by considering the balance_pending
            balance = int(sender_wallet["balances"][symbol]["balance"])
            balance_pending = int(sender_wallet["balances"][symbol]["balance_pending"])
        
            # Adjust the available balance if balance_pending is negative
            if balance_pending < 0:
                available_balance = balance + balance_pending
            else:
                available_balance = balance

            # Check if the sender has sufficient available balance
            if available_balance < int(amount) + int(fee):
                return {"error": "Insufficient balance"}

            # Create and add the transaction
            transaction = await self.add_txn(sender, receiver, amount, symbol, data, fee, action, contract_hash)

            # Update the sender wallet balance and pending balance
            sender_wallet["balances"][symbol]["balance"] = str(balance - int(amount) - int(fee))
            sender_wallet["balances"][symbol]["balance_pending"] = str(balance_pending - int(amount) - int(fee))

            sender_wallet["tx_count"] += 1
            sender_wallet["tx_data"].append(transaction)
            sender_wallet["last_tx_timestamp"] = transaction["timestamp"]
            
            # Save the updated sender back to the cache
            self.app_state.wallets[sender] = sender_wallet
            self.app_state.data_store["wallets"][sender] = sender_wallet
            await self.save_blockchain_wallets(sender, sender_wallet)

            # Update the receiver wallet pending balance
            if sender != receiver:
                receiver_balance_pending = int(receiver_wallet["balances"][symbol]["balance_pending"])
                receiver_wallet["balances"][symbol]["balance_pending"] = str(receiver_balance_pending + int(amount))

                receiver_wallet["tx_count"] += 1
                receiver_wallet["tx_data"].append(transaction)
                receiver_wallet["last_tx_timestamp"] = transaction["timestamp"]

                # Save the updated receiver back to the cache
                self.app_state.wallets[receiver] = receiver_wallet
                self.app_state.data_store["wallets"][receiver] = receiver_wallet

            #Save the updated wallets back to sqlite
            self.app_state.data_has_changed = True
            await self.save_blockchain_wallets(receiver, receiver_wallet)

        except Exception as e:
            print(f"Error sending internal transaction: {e}")
            return {"error": f"Error sending transaction: {e}"}
    
    async def send_txn(self, txn_data, is_contract_action=False):
        try:
            blockchain_symbol = self.app_state.config_store["BLOCKCHAIN_SYMBOL"]
            private_key = txn_data["private_key"]
            receiver = txn_data["receiver"]
            amount = txn_data["amount"]
            symbol = txn_data["symbol"]
            data = txn_data["data"]
            fee = txn_data.get("fee", "0")
            action = txn_data.get("action", "")
            contract_hash = txn_data.get("contract_hash", "")

            # Generate public key from private key
            sender = await self.get_address_from_private_key(private_key)

            # Retrieve the sender wallet from the cache
            sender_wallet = self.app_state.wallets.get(sender, {
                "tx_count": 0,
                "tx_data": [],
                "last_tx_timestamp": "",
                "balances": {}
            })

            # Retrieve or initialize the receiver wallet from the cache
            receiver_wallet = self.app_state.wallets.get(receiver, {
                "tx_count": 0,
                "tx_data": [],
                "last_tx_timestamp": "",
                "balances": {}
            })

            # Initialize balances if not present
            sender_wallet["balances"].setdefault(blockchain_symbol, {"balance": "0", "balance_pending": "0"})
            receiver_wallet["balances"].setdefault(blockchain_symbol, {"balance": "0", "balance_pending": "0"})
            sender_wallet["balances"].setdefault(symbol, {"balance": "0", "balance_pending": "0"})
            receiver_wallet["balances"].setdefault(symbol, {"balance": "0", "balance_pending": "0"})

            # Convert the amount and fee with decimal multiplier
            blockchain_decimal = int(self.app_state.config_store["BLOCKCHAIN_DECIMAL"])
            multiplier = 10 ** blockchain_decimal
            amount = int(float(amount) * multiplier)
            fee = int(float(fee) * multiplier)

            # Calculate the available balance excluding pending
            mgdb_balance = int(sender_wallet["balances"][blockchain_symbol]["balance"])
            mgdb_balance_pending = int(sender_wallet["balances"][blockchain_symbol]["balance_pending"])

            # Check if the sender has sufficient balance for the fee
            if mgdb_balance < fee:
                return {"error": "Insufficient balance for fee"}

            # Create and add the transaction
            transaction = await self.add_txn(sender, receiver, amount, symbol, data, fee, action, contract_hash)

            # Adding txn failed
            if not transaction:
                return {"error": "Transaction failed"}

            # If not contract creation, check the symbol balance and update
            if not is_contract_action:
                # Calculate the available symbol balance excluding pending
                sender_symbol_balance = int(sender_wallet["balances"][symbol]["balance"])
                sender_symbol_balance_pending = int(sender_wallet["balances"][symbol]["balance_pending"])

                # Check if the sender has sufficient available balance for the amount
                if sender_symbol_balance < amount:
                    return {"error": f"Insufficient balance for {symbol}"}

                # Update the sender's symbol balance for the amount
                sender_wallet["balances"][symbol]["balance"] = str(sender_symbol_balance - amount)
                sender_wallet["balances"][symbol]["balance_pending"] = str(sender_symbol_balance_pending - amount)

                # Update the sender's wallet
                sender_wallet["tx_count"] += 1
                sender_wallet["tx_data"].append(transaction)
                sender_wallet["last_tx_timestamp"] = transaction["timestamp"]

            # Handle fee processing
            if fee > 0:
                # Deduct the fee from the sender's balance
                sender_wallet["balances"][blockchain_symbol]["balance"] = str(mgdb_balance - fee)
                sender_wallet["balances"][blockchain_symbol]["balance_pending"] = str(mgdb_balance_pending - fee)
                genesis_address = self.app_state.config_store["BLOCKCHAIN_CONF"]["genesis_address"]

                if self.app_state.config_store["BLOCKCHAIN_CAN_BURN"]:
                    # If burning is allowed, burn the fee tokens as a separate transaction
                    genesis_private_key = self.app_state.config_store["BLOCKCHAIN_CONF"]["genesis_private_key"]
                    burn_data = {
                        "private_key": genesis_private_key,
                        "amount": fee,
                        "action": "burn",
                        "contract_hash": self.app_state.config_store["BLOCKCHAIN_CONF"]["genesis_contract_hash"],
                    }
                    await self.burn(burn_data)
                else:
                    # Credit the fee to the genesis wallet's pending balance
                    genesis_wallet = self.app_state.wallets.get(genesis_address, {
                        "tx_count": 0,
                        "tx_data": [],
                        "last_tx_timestamp": "",
                        "balances": {}
                    })

                    genesis_wallet["balances"].setdefault(blockchain_symbol, {"balance": "0", "balance_pending": "0"})

                    genesis_balance_pending = int(genesis_wallet["balances"][blockchain_symbol]["balance_pending"])
                    genesis_wallet["balances"][blockchain_symbol]["balance_pending"] = str(genesis_balance_pending + fee)
                    await self.save_blockchain_wallets(genesis_address, genesis_wallet)

            # Update the receiver's wallet
            receiver_symbol_balance = int(receiver_wallet["balances"][symbol]["balance"])
            receiver_wallet["balances"][symbol]["balance_pending"] = str(receiver_symbol_balance + amount)

            receiver_wallet["tx_count"] += 1
            receiver_wallet["tx_data"].append(transaction)
            receiver_wallet["last_tx_timestamp"] = transaction["timestamp"]

            # Save the sender's wallet to the cache and database
            self.app_state.wallets[sender] = sender_wallet
            self.app_state.data_store["wallets"][sender] = sender_wallet
            await self.save_blockchain_wallets(sender, sender_wallet)

            # Save the receiver's wallet to the cache and database
            self.app_state.wallets[receiver] = receiver_wallet
            self.app_state.data_store["wallets"][receiver] = receiver_wallet
            await self.save_blockchain_wallets(receiver, receiver_wallet)

            # Save data cache
            self.app_state.data_has_changed = True

            txid = {"txid": transaction["txid"]}
            return txid
        except KeyError as e:
            print(f"Missing key in txn_data: {e}")
            return {"error": f"Missing key in txn_data: {e}"}
        except ValueError as e:
            print(f"Value error: {e}")
            return {"error": f"Value error: {e}"}
        except Exception as e:
            print(f"Error sending transaction: {e}")
            return {"error": f"Error sending transaction: {e}"}

    async def create_contract(self, contract_data):
        try:
            private_key = contract_data.pop("private_key")
            
            # Decode the private key to get the public key and address
            owner_address = await self.get_address_from_private_key(private_key)
            
            # Include the owner address in the contract data
            contract_data["owner_address"] = owner_address
            
            # Prepare the transaction data for contract creation
            contract_txn_data = {
                "private_key": private_key,
                "receiver": owner_address,
                "amount": contract_data["supply"],
                "symbol": contract_data["symbol"],
                "data": ujson.dumps(contract_data),
                "fee": self.app_state.config_store["BLOCKCHAIN_CONTRACT_FEE"],
                "action": "create_contract",
            }
            
            # Send the transaction to broadcast the contract creation
            contract_txid_response = await self.send_txn(contract_txn_data, is_contract_action=True)
            
            if "txid" in contract_txid_response:
                contract_hash = await self.hash_data(contract_data)
                contract_data["contract_hash"] = contract_hash
                contract_data["created_at"] = int(time.time())
                contract_data["updated_at"] = int(time.time())
                
                await self.save_contract(contract_data)
                
                return {"status": "success", "contract_hash": contract_hash}
            else:
                return {"status": "error", "message": f"Failed to broadcast contract creation transaction: {contract_txid_response.get("error", "Unknown error")}"}
        except Exception as e:
            print(f"Error creating contract: {e}")
            return {"status": "error", "message": str(e)}

    async def get_contract(self, contract_data):
        try:
            contract_hash = contract_data["contract_hash"]
            self.cursor.execute('''
                SELECT * FROM contracts WHERE contract_hash=?
            ''', (contract_hash,))
            contract = self.cursor.fetchone()
            if contract:
                contract_dict = {
                    "contract_hash": contract[0],
                    "owner_address": contract[1],
                    "name": contract[2],
                    "description": contract[3],
                    "logo": contract[4],
                    "symbol": contract[5],
                    "supply": contract[6],
                    "max_supply": contract[7],
                    "can_mint": contract[8],
                    "can_burn": contract[9],
                    "created_at": contract[10],
                    "updated_at": contract[11]
                }
                return {"status": "success", "contract": contract_dict}
            else:
                return {"status": "error", "message": "Contract not found"}
        except Exception as e:
            print(f"Error getting contract: {e}")
            return {"status": "error", "message": str(e)}

    async def mint(self, mint_data):
        try:
            contract_hash = mint_data["contract_hash"]
            receiver = mint_data["receiver"]
            amount = mint_data["amount"]
            private_key = mint_data["private_key"]
            
            self.cursor.execute('''
                SELECT * FROM contracts WHERE contract_hash=?
            ''', (contract_hash,))
            contract = self.cursor.fetchone()
            
            if not contract:
                return {"status": "error", "message": "Contract not found"}

            if not contract[7]:  # can_mint
                return {"status": "error", "message": "Minting not allowed for this contract"}

            if contract[5] + amount > contract[6]:  # supply + amount > max_supply
                return {"status": "error", "message": "Amount exceeds max supply"}

            # Decode the private key to get the address
            owner_address = await self.get_address_from_private_key(private_key)

            # Check if the decoded address matches the contract owner address
            if owner_address != contract[1]:
                return {"status": "error", "message": "Unauthorized mint operation"}

            symbol = contract[4]

            # Record the mint transaction
            mint_txn_data = {
                "private_key": private_key,
                "receiver": receiver,
                "amount": amount,
                "symbol": symbol,
                "data": "Mint transaction",
                "fee": "0",  # No fee for mint transactions
                "action": "mint",
                "contract_hash": contract_hash
            }
            mint_txn_response = await self.send_txn(mint_txn_data, is_contract_action=True)

            if "txid" in mint_txn_response:
                return {"status": "success", "mint_txid": mint_txn_response.get("txid")}
            else:
                return {"status": "error", "message": f"Failed to broadcast mint transaction: {mint_txn_response.get("error", "Unknown error")}"}
        except Exception as e:
            print(f"Error minting: {e}")
            return {"status": "error", "message": str(e)}

    async def burn(self, burn_data):
        try:
            contract_hash = burn_data["contract_hash"]
            amount = burn_data["amount"]
            private_key = burn_data["private_key"]
            
            self.cursor.execute('''
                SELECT * FROM contracts WHERE contract_hash=?
            ''', (contract_hash,))
            contract = self.cursor.fetchone()
            
            if not contract:
                return {"status": "error", "message": "Contract not found"}

            if not contract[8]:  # can_burn
                return {"status": "error", "message": "Burning not allowed for this contract"}

            # Decode the private key to get the address
            owner_address = await self.get_address_from_private_key(private_key)

            # Check if the decoded address matches the contract owner address
            if owner_address != contract[1]:
                return {"status": "error", "message": "Unauthorized burn operation"}

            # Check owner's wallet balance
            owner_wallet = self.app_state.wallets.get(owner_address)
            if not owner_wallet:
                return {"status": "error", "message": "Owner wallet not found"}

            symbol = contract[4]
            if symbol not in owner_wallet["balances"]:
                return {"status": "error", "message": f"{symbol} balance not found in owner's wallet"}

            if int(owner_wallet["balances"][symbol]["balance"]) < amount:
                return {"status": "error", "message": "Burn amount exceeds wallet balance"}

            # Record the burn transaction
            burn_txn_data = {
                "private_key": private_key,
                "receiver": "",  # No receiver for burn transactions
                "amount": amount,
                "symbol": symbol,
                "data": "Burn transaction",
                "fee": "0",  # No fee for burn transactions
                "action": "burn",
                "contract_hash": contract_hash
            }
            burn_txn_response = await self.send_txn(burn_txn_data, is_contract_action=False)

            if "txid" in burn_txn_response:
                return {"status": "success", "burn_txid": burn_txn_response.get("txid")}
            else:
                return {"status": "error", "message": f"Failed to broadcast burn transaction: {burn_txn_response.get("error", "Unknown error")}"}
        except Exception as e:
            print(f"Error burning: {e}")
            return {"status": "error", "message": str(e)}
    
    async def get_address_from_private_key(self, private_key):
        try:
            sk = SigningKey.from_string(bytes.fromhex(private_key), curve=SECP256k1)
            vk = sk.get_verifying_key()
            public_key = vk.to_string("uncompressed").hex()
            address = await self.generate_address(bytes.fromhex(public_key))
            return address
        except Exception as e:
            print(f"Error generating address from private key: {e}")
            return None
