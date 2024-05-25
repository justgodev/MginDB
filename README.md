# Welcome to MginDB
In-Memory, Schema-less, and Limitless.

## Features
- **Schema Flexibility**
  - Supports schema-less data storage for maximum flexibility through relational indices.
- **Advanced Querying**
  - Offers powerful querying functions to explore and analyze complex relational data efficiently.
- **Scalability & Sharding**
  - Designed for horizontal scalability and sharding to meet growing demands and volumes.
- **Real-time Updates**
  - Supports WebSocket connections for real-time data updates with a robust pub/sub mechanism.

## What is MginDB?
MginDB is an innovative in-memory database that integrates the agility of NoSQL systems with the robustness of traditional databases, ideally suited for applications that demand rapid access to data and flexible management.

Designed for performance, MginDB facilitates extremely fast data operations, leveraging in-memory storage mechanisms to minimize access times and maximize throughput.

## Usage
Integrate MginDB into your development stack to significantly enhance data handling capabilities across web, mobile, and IoT platforms.

MginDB’s comprehensive toolset allows for effective data operations, from simple CRUD to complex transactions and real-time updates.

Ensure data integrity and consistency with MginDB’s support for complex transactions, even in distributed environments.

## CLI / GUI
The simplicity of executing complex queries and accessing real-time data insights with a user-friendly web interface.

Monitor and manage your in-memory databases, analyze and update data, execute deep queries, scale instantly, decentralize and more.

Write, test, and validate your queries before deploying.

Web CLI: https://mgindb.com/client.html

Web GUI DB Admin: https://mgindb.com/gui.html

## Documentation
https://mgindb.com/documentation.html

## Install
```sh
# Download and install via pip
wget https://mgindb.com/downloads/mgindb-0.1.3-py3-none-any.whl
pip install mgindb-0.1.3-py3-none-any.whl

# Or download the tarball and install manually
wget https://mgindb.com/downloads/mgindb-0.1.3.tar.gz
tar -xzf mgindb-0.1.3.tar.gz
cd mgindb-0.1.3
python setup.py install

# Start MginDB
mgindb start

# MginDB CLI
mgindb client