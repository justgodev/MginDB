import ujson  # Module for JSON operations
import uuid  # Module for generating unique identifiers

# Print a message indicating the setup process
print("Setting up your MginDB configuration...")

# Define the initial configuration dictionary
config = {
    "INSTANCE_UUID": str(uuid.uuid4()),  # Generate a unique identifier for the instance
    "HOST": "127.0.0.1",  # Default host address
    "PORT": "6446",  # Default port number
    "USERNAME": "",  # Placeholder for username
    "PASSWORD": "",  # Placeholder for password
    "AUTO_UPDATE": "1",  # Enable auto-update by default
    "SAVE_ON_FILE_INTERVAL": "15",  # Interval for saving data to file (in minutes)
    "BACKUP_ON_SHUTDOWN": "0",  # Disable backup on shutdown by default
    "SCHEDULER": "1",  # Enable scheduler by default
    "QUERY_CACHING": "1",  # Enable query caching by default
    "QUERY_CACHING_TTL": "300",  # Query caching time to live
    "REPLICATION": "0",  # Disable replication by default
    "REPLICATION_TYPE": "MASTER",  # Default replication type
    "REPLICATION_MASTER": "",  # Placeholder for replication master address
    "REPLICATION_SLAVES": [],  # List of replication slaves
    "SHARDING_TYPE": "MASTER",  # Default sharding type
    "SHARDING": "0",  # Disable sharding by default
    "SHARDING_BATCH_SIZE": "500",  # Default batch size for sharding
    "SHARDS": [],  # List of shards
    "BLOCKCHAIN": [],  # Blockchain
    "BLOCKCHAIN_NAME": "",  # Blockchain name
    "BLOCKCHAIN_SYMBOL": "",  # Blockchain symbol
    "BLOCKCHAIN_TX_PER_BLOCK": 10,  # Blockchain tx per block
    "BLOCKCHAIN_DATA": {}  # Blockchain conf
}

# Write the configuration to a JSON file
with open('config.json', mode='w', encoding='utf-8') as file:
    ujson.dump(config, file, indent=4)

# Print a message indicating the setup is complete
print("Initial configuration has been set up.")