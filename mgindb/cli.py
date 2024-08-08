import click  # Importing Click for command-line interface
import asyncio  # Importing asyncio for asynchronous programming
from .server import ServerManager  # Importing ServerManager from the server module
from .client import MginDBCLI  # Importing MginDBCLI from the client module

@click.group()
def cli():
    """Command-line interface group for MginDB."""
    pass

@click.command()
def start():
    """Start the MginDB server."""
    server_manager = ServerManager()  # Create an instance of ServerManager
    asyncio.run(server_manager.start_server())  # Run the server using asyncio

@click.command()
def client():
    """Start the MginDB client."""
    cli_instance = MginDBCLI()  # Create an instance of MginDBCLI
    cli_instance.run()  # Run the client

# Add the start and client commands to the CLI group
cli.add_command(start)
cli.add_command(client)

if __name__ == '__main__':
    cli()  # Run the CLI