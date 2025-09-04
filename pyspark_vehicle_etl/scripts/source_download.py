import requests
import os

class SourceDownload:
    def __init__(self, url, target_dir):
        self.url = url
        self.target_dir = target_dir
        
        self.file_name = url.split("/")[-1]
        self._full_path = os.path.join(self.target_dir, self.file_name)
            
    def download_from_url(self):
        try:
            print(f"Downloading file from {self.url}")

            os.makedirs(self.target_dir, exist_ok=True)
            
            response = requests.get(self.url, stream=True)
            response.raise_for_status() 
            
            with open(self._full_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk: 
                        f.write(chunk)
            
            print(f"File {self.file_name} downloaded successfully to {self._full_path}")
            return True
            
        except Exception as e:
            print(f"Error: {e}")
            return None
        
    @property
    def full_path(self):
        return self._full_path
