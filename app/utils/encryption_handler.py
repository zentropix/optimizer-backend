from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import os
from dotenv import load_dotenv
import base64

load_dotenv()

class EncryptionHandler:
    def __init__(self):
        self.aesgcm = AESGCM(base64.urlsafe_b64decode(os.getenv("AES_KEY")))
    
    def encrypt_number(self, phone_number):
        nonce = os.urandom(12)  # GCM standard
        ciphertext = self.aesgcm.encrypt(
            nonce,
            phone_number.encode(),
            None
        )
        token = nonce + ciphertext
        return base64.urlsafe_b64encode(token).decode().rstrip("=")
    
    def decrypt_number(self, encrypted_key):
        padded = encrypted_key + "=" * (-len(encrypted_key) % 4)
        raw = base64.urlsafe_b64decode(padded.encode())
        nonce = raw[:12]
        ciphertext = raw[12:]
        return self.aesgcm.decrypt(nonce, ciphertext, None).decode()
    
    def encrypt_list(self, phone_numbers):
        return [self.encrypt_number(number) for number in phone_numbers]
    
if __name__ == "__main__":
    encryption_handler = EncryptionHandler()
    encrypted_keys = encryption_handler.encrypt_list(["923244415089", "923241232323235089"])
