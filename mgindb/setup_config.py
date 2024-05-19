import json
import uuid

print("Setting up your MginDB configuration...")
config = {
    "INSTANCE_UUID": str(uuid.uuid4()),
    "HOST": "127.0.0.1",
    "PORT": "6446",
    "USERNAME": "",
    "PASSWORD": "",
    "AUTO_UPDATE": "1",
    "SAVE_ON_FILE_INTERVAL": "15",
    "BACKUP_ON_SHUTDOWN": "0",
    "SCHEDULER": "1",
    "REPLICATION": "0",
    "REPLICATION_TYPE": "MASTER",
    "REPLICATION_MASTER": "",
    "REPLICATION_SLAVES": [],
    "SHARDING_TYPE": "MASTER",
    "SHARDING": "0",
    "SHARDING_BATCH_SIZE": "10",
    "SHARDS": []
}

with open('config.json', mode='w', encoding='utf-8') as file:
    json.dump(config, file, indent=4)
print("Initial configuration has been set up.")