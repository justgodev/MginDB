import os
from .constants import UPLOAD_DIR  # Import constants
import aiofiles
import base64

class UploadManager:
    def __init__(self):
        self.base_directory = UPLOAD_DIR
        if not os.path.exists(self.base_directory):
            os.makedirs(self.base_directory)

    async def save_file(self, key, data):
        file_path = self._get_file_path(key)
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        # Strip the data URL prefix if it exists
        if isinstance(data, str) and data.startswith('data:image/'):
            data = data.split(',', 1)[1]
        
        # Decode base64 data before saving
        binary_data = base64.b64decode(data)
        
        async with aiofiles.open(file_path, 'wb') as file:
            await file.write(binary_data)
        
        return file_path

    async def read_file(self, key):
        file_path = self._get_file_path(key)
        async with aiofiles.open(file_path, 'rb') as file:
            data = await file.read()
        # Encode the data as base64 and add the prefix back
        base64_data = base64.b64encode(data).decode('utf-8')
        return f"data:image/jpeg;base64,{base64_data}"
    
    def _get_file_path(self, key):
        return os.path.join(self.base_directory, key.replace(':', os.sep))