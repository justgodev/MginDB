from .server import ServerManager
from .client import MginDBCLI

async def start_server():
    server_manager = ServerManager()
    await server_manager.start_server()
