import requests
from dotenv import load_dotenv
from time import time, sleep
import os

load_dotenv()

class VoluumAccessKeyHandler:
    def __init__(self):
        self.access_key = None
        self.creation_time = None
        self.expiry_time = None

    def get_session_token(self):
        url = "https://api.voluum.com/auth/access/session"

        payload = {
            "accessId": os.getenv("VOLUUM_ACCESS_ID"),
            "accessKey": os.getenv("VOLUUM_ACCESS_KEY")
        }

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json"
        }
        response = None
        try:
            response = requests.post(url, json=payload, headers=headers)
        except Exception as e:
            print(e)
            return False
        
        self.access_key = response.json()["token"]
        self.creation_time = time()
        self.expiry_time = self.creation_time + 14400
        return True
    
    def create_session_token(self):
        while(not self.get_session_token()):
            print('Trying to get Voluum Access Key after failure.')
            sleep(10)

    def get_access_token(self):
        if not self.access_key or self.expiry_time - time() < 0:
            self.create_session_token()
        
        return self.access_key
    
if __name__ == "__main__":
    handler = VoluumAccessKeyHandler()
    print(handler.get_access_token())
    sleep(10)
    print(handler.get_access_token())
    
