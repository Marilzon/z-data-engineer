import requests
import os

def download_from_url(url, target_path):
    try:
        file_name = url.split("/")[-1]
        full_path = os.path.join(target_path, file_name)
        
        print(f"Downloading file from {url}")

        os.makedirs(target_path, exist_ok=True)
        
        response = requests.get(url, stream=True)
        response.raise_for_status() 
        
        with open(full_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk: 
                    f.write(chunk)
        
        print(f"File {file_name} downloaded successfully to {full_path}")
        return full_path
        
    except Exception as e:
        print(f"Error: {e}")
        return None
