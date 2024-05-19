import click
import asyncio
from .server import ServerManager
from .client import MginDBCLI

@click.group()
def cli():
    pass

@click.command()
def start():
    """Start the MginDB server."""
    server_manager = ServerManager()
    asyncio.run(server_manager.start_server())

@click.command()
def client():
    """Start the MginDB client."""
    cli_instance = MginDBCLI()
    cli_instance.run()

cli.add_command(start)
cli.add_command(client)

if __name__ == '__main__':
    cli()