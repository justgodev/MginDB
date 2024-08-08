from .server import ServerManager  # Importing ServerManager from the server module
from .client import MginDBCLI  # Importing MginDBCLI from the client module

async def start_server():
    """Start the MginDB server asynchronously."""
    server_manager = ServerManager()  # Create an instance of ServerManager
    await server_manager.start_server()  # Start the server asynchronously