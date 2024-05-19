import asyncio
import websockets
import json
import os
import signal
from getpass import getpass
from .config import load_config

class MginDBCLI:
    def __init__(self):
        self.loop = None
        self.websocket = None

    def color_text(self, text, color_code):
        return f"\033[{color_code}m{text}\033[0m"

    def red(self, text):
        return self.color_text(text, 31)

    def green(self, text):
        return self.color_text(text, 32)

    def blue(self, text):
        return self.color_text(text, 34)

    def cyan(self, text):
        return self.color_text(text, 36)

    def yellow(self, text):
        return self.color_text(text, 33)

    def magenta(self, text):
        return f"\033[35m{text}\033[0m"

    def print_table(self, data):
        if isinstance(data, list):
            if all(isinstance(item, dict) for item in data):
                self.print_section(data)
            else:
                for item in data:
                    self.print_table(item)
        elif isinstance(data, dict):
            for group_key, entries in data.items():
                print(f"\nGroup: {group_key}")
                if isinstance(entries, list) and all(isinstance(item, dict) for item in entries):
                    self.print_section(entries)
                else:
                    print("Data format is not as expected.")
        else:
            print(data)
        print('')

    def print_section(self, sections):
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
        return {h: max(len(h), *(len(self.format_cell(item.get(h, ""))) for item in sections)) for h in headers}

    def print_headers(self, headers, column_widths):
        print('')
        header_line = " | ".join(h.center(column_widths[h]) for h in headers)
        separator_line = "-+-".join("-" * column_widths[h] for h in headers)
        print(header_line)
        print(separator_line)

    def format_cell(self, value):
        return str(value)

    def print_single_entry(self, entry):
        if isinstance(entry, dict):
            headers = sorted(entry.keys())
            column_widths = self.calculate_column_widths([entry], headers)
            
            self.print_headers(headers, column_widths)
            
            values = [self.format_cell(entry[h]).center(column_widths[h]) for h in headers]
            print(" | ".join(values))
        else:
            print("Invalid entry type")

    def format_response(self, response, use_table_format):
        try:
            json_response = json.loads(response)
            if use_table_format:
                self.print_table(json_response)
            else:
                print(json.dumps(json_response, indent=2))
        except json.JSONDecodeError:
            print(response)

    async def cli_session(self, uri):
        print('')
        username = input(self.red("Enter username (leave blank if not required): "))
        password = getpass(self.red("Enter password (leave blank if not required): "))

        print('')
        print(self.yellow("Connecting to server..."))
        try:
            self.websocket = await websockets.connect(uri, ping_interval=30, ping_timeout=10)
            auth_data = {'username': username, 'password': password}
            await self.websocket.send(json.dumps(auth_data))

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
                    user_input = await asyncio.wait_for(asyncio.to_thread(input, self.yellow('MginDB> ')), timeout=600)
                    if user_input.lower() == 'exit':
                        print(self.yellow('Exiting CLI.'))
                        break
                    elif user_input.lower() == 'clear':
                        os.system('cls' if os.name == 'nt' else 'clear')
                        continue

                    use_table_format = '-f' in user_input
                    if use_table_format:
                        user_input = user_input.replace('-f', '').strip()

                    if user_input.strip():
                        if user_input.strip().startswith('{') and ':' in user_input:
                            command_data = json.loads(user_input)
                            await self.websocket.send(json.dumps(command_data))
                        else:
                            await self.websocket.send(user_input)
                    else:
                        await self.websocket.ping()

                    response = await self.websocket.recv()
                    self.format_response(response, use_table_format)

                except asyncio.TimeoutError:
                    print("No input received within the timeout period. Exiting CLI.")
                    break
                except websockets.ConnectionClosed:
                    print("Connection to server closed.")
                    break
                except json.JSONDecodeError:
                    print("Failed to parse command as JSON. Sending as raw text.")
                    await self.websocket.send(user_input)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    print(f"An error occurred: {e}")
        except KeyboardInterrupt:
            print("Keyboard interrupt, Shutting down client...")
        finally:
            if self.websocket:
                await self.websocket.close()

    async def shutdown(self, signal=None):
        if signal:
            print(f"Received exit signal {signal.name}...")
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(0.1)

    def run(self):
        try:
            config = load_config()
            uri = f"ws://{config.get('HOST')}:{config.get('PORT')}"
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            for s in (signal.SIGINT, signal.SIGTERM):
                self.loop.add_signal_handler(
                    s, lambda s=s: asyncio.create_task(self.shutdown(signal=s)))
            self.loop.run_until_complete(self.cli_session(uri))
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            self.loop.run_until_complete(self.shutdown())
            self.loop.close()
            print("Client closed, you can press enter to fully exit...")

if __name__ == '__main__':
    MginDBCLI().run()
