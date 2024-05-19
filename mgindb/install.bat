@echo off
echo "Checking for Python and pip..."
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo "Python is not installed or not found in PATH."
    echo "Please ensure Python is installed and added to your PATH."
    pause
    exit /b 1
)

where pip >nul 2>&1
if %errorlevel% neq 0 (
    echo "pip is not installed or not found in PATH."
    echo "Please ensure pip is installed and added to your PATH."
    pause
    exit /b 1
)

echo "Installing MginDB dependencies..."
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo "Failed to install dependencies."
    pause
    exit /b 1
)

echo "Dependencies installed successfully."
echo "Setting up initial configuration..."
python setup_config.py
if %errorlevel% neq 0 (
    echo "Failed to complete initial configuration."
    pause
    exit /b 1
)

echo "MginDB is now installed and configured."
echo "You can start the server using: mgindb start"
echo "Documentation: open documentation.html"
echo "Server CLI: mgindb client"
echo "Web CLI: open client.html"
echo "GUI DB Admin: open gui.html"
pause