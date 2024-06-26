# Import necessary modules and classes
import os  # Module for interacting with the operating system
import requests  # Module for making HTTP requests
import subprocess  # Module for running subprocesses
import sys  # Module for system-specific parameters and functions
import hashlib  # Module for generating hash values
from .app_state import AppState  # Importing AppState class from app_state module

class UpdateManager:
    def __init__(self):
        """Initialize UpdateManager with application state and setup update URL and download directory."""
        self.app_state = AppState()
        self.update_url = f"{self.app_state.mgindb_url}/latest_version.json"
        self.download_directory = "downloads"
    
    def has_auto_update(self):
        """
        Check if auto update is activated.

        Returns:
            bool: True if auto update is activated, False otherwise.
        """
        auto_update = self.app_state.config_store.get('AUTO_UPDATE')
        return auto_update == '1'

    def check_update(self, *args, **kwargs):
        """
        Check if there is a new version available.

        Returns:
            tuple: A tuple containing update data and a message.
        """
        try:
            current_version = self.app_state.version
            response = requests.get(self.update_url)
            data = response.json()
            latest_version = data['version']
            
            if current_version < latest_version:
                return data, f"New version available: {latest_version}"  # Return entire data object which includes the version and checksum
            else:
                return None, f"MginDB is up to date (v.{self.app_state.version})"
        except Exception as e:
            return None, f"Error checking for updates: {e}"

    def verify_checksum(self, file_path, expected_checksum):
        """
        Verify the checksum of the downloaded file.

        Args:
            file_path (str): The path to the file to verify.
            expected_checksum (str): The expected checksum.

        Returns:
            bool: True if the checksum matches, False otherwise.
        """
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        calculated_checksum = "sha256:" + sha256_hash.hexdigest()
        return calculated_checksum == expected_checksum

    def install_update(self, update_data):
        """
        Download and install the new version if available.

        Args:
            update_data (dict): The update data containing version, URL, and checksum.
        """
        try:
            latest_version = update_data.get('version')
            wheel_name = f"mgindb-{latest_version}-py3-none-any.whl"
            wheel_url = update_data.get('url')
            wheel_path = os.path.join(self.download_directory, wheel_name)
            expected_checksum = update_data.get('checksum')

            # Ensure download directory exists
            os.makedirs(self.download_directory, exist_ok=True)

            # Download the wheel file
            print(f"Downloading update from {wheel_url}")
            response = requests.get(wheel_url)
            if response.status_code == 200:
                with open(wheel_path, "wb") as f:
                    f.write(response.content)
                print(f"Successfully downloaded {wheel_name}...")

                # Verify checksum
                if expected_checksum and not self.verify_checksum(wheel_path, expected_checksum):
                    print("Checksum verification failed. Update aborted.")
                    return

                # Install the wheel using pip
                subprocess.call([sys.executable, '-m', 'pip', 'install', '--upgrade', wheel_path])
                print("Update applied successfully. Please restart the server.")

                # Remove the wheel file after installation
                os.remove(wheel_path)
            else:
                print(f"Failed to download the update. Status code: {response.status_code}")
        except Exception as e:
            print(f"Error installing update: {e}")
            if os.path.exists(wheel_path):
                os.remove(wheel_path)  # Remove the wheel file if an exception occurs