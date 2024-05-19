import json  # Module for JSON operations
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
    "REPLICATION": "0",  # Disable replication by default
    "REPLICATION_TYPE": "MASTER",  # Default replication type
    "REPLICATION_MASTER": "",  # Placeholder for replication master address
    "REPLICATION_SLAVES": [],  # List of replication slaves
    "SHARDING_TYPE": "MASTER",  # Default sharding type
    "SHARDING": "0",  # Disable sharding by default
    "SHARDING_BATCH_SIZE": "10",  # Default batch size for sharding
    "SHARDS": []  # List of shards
}

# Write the configuration to a JSON file
with open('config.json', mode='w', encoding='utf-8') as file:
    json.dump(config, file, indent=4)

# Print a message indicating the setup is complete
print("Initial configuration has been set up.")