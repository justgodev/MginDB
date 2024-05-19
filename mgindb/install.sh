#!/bin/bash

# Checking for Python and pip availability
command -v python >/dev/null 2>&1 || { echo >&2 "Python is not installed or not found in PATH. Please install Python."; exit 1; }
command -v pip >/dev/null 2>&1 || { echo >&2 "pip is not installed or not found in PATH. Please install pip."; exit 1; }

echo "Installing MginDB dependencies..."
if ! pip install -r requirements.txt; then
    echo "Failed to install dependencies."
    exit 1
fi

echo "Dependencies installed successfully."
echo "Setting up initial configuration..."
if ! python setup_config.py; then
    echo "Failed to complete initial configuration."
    exit 1
fi

echo "MginDB is now installed and configured."
echo "You can start the server using: mgindb start"
echo "Documentation: open documentation.html"
echo "Server CLI: mgindb client"
echo "Web CLI: open client.html"
echo "GUI DB Admin: open gui.html"