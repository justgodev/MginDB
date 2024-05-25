import asyncio  # Module for asynchronous programming
import websockets  # WebSocket client and server library for asyncio
import ujson  # Module for JSON operations
import os  # Module for interacting with the operating system
import signal  # Module for signal handling
from getpass import getpass  # Function to securely get a password from the user
from .config import load_config  # Importing function to load configuration

class MginDBCLI:
    def __init__(self):
        # Initialize event loop and websocket attributes
        self.loop = None  # Event loop placeholder
        self.websocket = None  # WebSocket connection placeholder

    def color_text(self, text, color_code):
        """
        Returns text formatted with the given ANSI color code.

        Args:
            text (str): The text to color.
            color_code (int): The ANSI color code.

        Returns:
            str: The colored text.
        """
        return f"\033[{color_code}m{text}\033[0m"

    def red(self, text):
        """
        Returns text formatted in red color.

        Args:
            text (str): The text to color.

        Returns:
            str: The red-colored text.
        """
        return self.color_text(text, 31)

    def green(self, text):
        """
        Returns text formatted in green color.

        Args:
            text (str): The text to color.

        Returns:
            str: The green-colored text.
        """
        return self.color_text(text, 32)

    def blue(self, text):
        """
        Returns text formatted in blue color.

        Args:
            text (str): The text to color.

        Returns:
            str: The blue-colored text.
        """
        return self.color_text(text, 34)

    def cyan(self, text):
        """
        Returns text formatted in cyan color.

        Args:
            text (str): The text to color.

        Returns:
            str: The cyan-colored text.
        """
        return self.color_text(text, 36)

    def yellow(self, text):
        """
        Returns text formatted in yellow color.

        Args:
            text (str): The text to color.

        Returns:
            str: The yellow-colored text.
        """
        return self.color_text(text, 33)

    def magenta(self, text):
        """
        Returns text formatted in magenta color.

        Args:
            text (str): The text to color.

        Returns:
            str: The magenta-colored text.
        """
        return self.color_text(text, 35)

    def print_table(self, data):
        """
        Prints data in a table format.

        This function prints the provided data in a structured table format.
        It supports both list and dictionary data structures.

        Args:
            data (list or dict): The data to print.
        """
        if isinstance(data, list):
            if all(isinstance(item, dict) for item in data):
                self.print_section(data)  # Print section if all items are dictionaries
            else:
                for item in data:
                    self.print_table(item)  # Recursively print table for each item
        elif isinstance(data, dict):
            for group_key, entries in data.items():
                print(f"\nGroup: {group_key}")
                if isinstance(entries, list) and all(isinstance(item, dict) for item in entries):
                    self.print_section(entries)  # Print section if all entries are dictionaries
                else:
                    print("Data format is not as expected.")
        else:
            print(data)
        print('')

    def print_section(self, sections):
        """
        Prints a section of data.

        This function prints a list of dictionaries in a tabular format,
        with headers derived from the dictionary keys.

        Args:
            sections (list of dict): The list of dictionaries to print.
        """
        if not sections:
            print("No data available.")
            return
        
        all_keys = set().union(*(d.keys() for d in sections))
        headers = sorted(all_keys)
        column_widths = self.calculate_column_widths(sections, headers)
        
        self.print_headers(headers, column_widths)
        
        for item in sections:
            values = [self.format_cell(item.get(h, "")).center(column_widths[h]) for h in headers]
            print(" | ".join(values))

    def calculate_column_widths(self, sections, headers):
        """
        Calculates the width of each column.

        This function determines the maximum width required for each column
        based on the headers and the data values.

        Args:
            sections (list of dict): The list of dictionaries containing the data.
            headers (list of str): The list of headers.

        Returns:
            dict: A dictionary with headers as keys and column widths as values.
        """
        return {h: max(len(h), *(len(self.format_cell(item.get(h, ""))) for item in sections)) for h in headers}

    def print_headers(self, headers, column_widths):
        """
        Prints the headers of the table.

        This function prints the headers of the table along with a separator line.

        Args:
            headers (list of str): The list of headers.
            column_widths (dict): A dictionary with headers as keys and column widths as values.
        """
        print('')
        header_line = " | ".join(h.center(column_widths[h]) for h in headers)
        separator_line = "-+-".join("-" * column_widths[h] for h in headers)
        print(header_line)
        print(separator_line)

    def format_cell(self, value):
        """
        Formats a cell value.

        This function converts the cell value to a string.

        Args:
            value: The value to format.

        Returns:
            str: The formatted value.
        """
        return str(value)

    def print_single_entry(self, entry):
        """
        Prints a single entry.

        This function prints a single dictionary entry in a tabular format.

        Args:
            entry (dict): The dictionary to print.
        """
        if isinstance(entry, dict):
            headers = sorted(entry.keys())
            column_widths = self.calculate_column_widths([entry], headers)
            
            self.print_headers(headers, column_widths)
            
            values = [self.format_cell(entry[h]).center(column_widths[h]) for h in headers]
            print(" | ".join(values))
        else:
            print("Invalid entry type")

    def format_response(self, response, use_table_format):
        """
        Formats the server response.

        This function formats the server response as either a JSON string or a table,
        depending on the use_table_format flag.

        Args:
            response (str): The server response.
            use_table_format (bool): Flag indicating whether to use table format.
        """
        try:
            json_response = ujson.loads(response)
            if use_table_format:
                self.print_table(json_response)  # Print response in table format
            else:
                print(ujson.dumps(json_response, indent=2))  # Print formatted JSON response
        except ujson.JSONDecodeError:
            print(response)  # Print raw response if JSON decoding fails

    async def cli_session(self, uri):
        """
        Handles the CLI session.

        This function manages the CLI session, including user authentication,
        sending commands to the server, and handling responses.

        Args:
            uri (str): The WebSocket URI to connect to.
        """
        print('')
        username = input(self.red("Enter username (leave blank if not required): "))
        password = getpass(self.red("Enter password (leave blank if not required): "))

        print('')
        print(self.yellow("Connecting to server..."))
        try:
            self.websocket = await websockets.connect(uri, ping_interval=30, ping_timeout=10)  # Connect to the WebSocket server
            auth_data = {'username': username, 'password': password}
            await self.websocket.send(ujson.dumps(auth_data))  # Send authentication data

            auth_response = await self.websocket.recv()
            print(self.yellow(auth_response))
            print('')

            if auth_response == '!!! Authentication failed: Incorrect username or password. !!!':
                return

            print('-------------------------------------------------------------')
            print('')
            print('MginDB CLI')
            print('')
            print(self.cyan("Documentation --> https://mgindb.com/documentation.html"))
            print(self.green("Add -f after QUERY command for enhanced display."))
            print(self.magenta("Type 'exit' to quit CLI, 'clear' to clear screen."))
            print('')
            print('-------------------------------------------------------------')
            print('')
            while True:
                try:
                    user_input = await asyncio.wait_for(asyncio.to_thread(input, self.yellow('MginDB> ')), timeout=600)  # Get user input with timeout
                    if user_input.lower() == 'exit':
                        print(self.yellow('Exiting CLI.'))
                        break
                    elif user_input.lower() == 'clear':
                        os.system('cls' if os.name == 'nt' else 'clear')  # Clear the screen
                        continue

                    use_table_format = '-f' in user_input
                    if use_table_format:
                        user_input = user_input.replace('-f', '').strip()

                    if user_input.strip():
                        if user_input.strip().startswith('{') and ':' in user_input:
                            command_data = ujson.loads(user_input)
                            await self.websocket.send(ujson.dumps(command_data))  # Send command as JSON
                        else:
                            await self.websocket.send(user_input)  # Send command as raw text
                    else:
                        await self.websocket.ping()  # Send a ping if input is empty

                    response = await self.websocket.recv()
                    self.format_response(response, use_table_format)

                except asyncio.TimeoutError:
                    print("No input received within the timeout period. Exiting CLI.")
                    break
                except websockets.ConnectionClosed:
                    print("Connection to server closed.")
                    break
                except ujson.JSONDecodeError:
                    print("Failed to parse command as ujson. Sending as raw text.")
                    await self.websocket.send(user_input)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    print(f"An error occurred: {e}")
        except KeyboardInterrupt:
            print("Keyboard interrupt, Shutting down client...")
        finally:
            if self.websocket:
                await self.websocket.close()  # Close the WebSocket connection

    async def shutdown(self, signal=None):
        """
        Handles shutdown of the client.

        This function cancels all running tasks and closes the WebSocket connection.

        Args:
            signal (signal.Signals, optional): The signal that triggered the shutdown.
        """
        if signal:
            print(f"Received exit signal {signal.name}...")
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

        for task in tasks:
            task.cancel()  # Cancel all tasks

        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(0.1)

    def run(self):
        """
        Runs the CLI.

        This function initializes the event loop, sets up signal handlers for shutdown,
        and starts the CLI session.
        """
        try:
            config = load_config()  # Load configuration
            uri = f"ws://{config.get('HOST')}:{config.get('PORT')}"  # Construct WebSocket URI
            self.loop = asyncio.new_event_loop()  # Create a new event loop
            asyncio.set_event_loop(self.loop)
            for s in (signal.SIGINT, signal.SIGTERM):
                self.loop.add_signal_handler(
                    s, lambda s=s: asyncio.create_task(self.shutdown(signal=s)))  # Add signal handlers for shutdown
            self.loop.run_until_complete(self.cli_session(uri))  # Run the CLI session
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            self.loop.run_until_complete(self.shutdown())  # Shutdown the client
            self.loop.close()  # Close the event loop
            print("Client closed, you can press enter to fully exit...")

if __name__ == '__main__':
    MginDBCLI().run()  # Run the MginDBCLI if this script is executed directly