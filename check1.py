import base64
import json
from Crypto.Cipher import AES
from Crypto.Protocol.KDF import PBKDF2
from Crypto.Hash import SHA256, SHA1, SHA512

def try_decrypt(ciphertext_b64, password, hash_module):
    blob = base64.b64decode(ciphertext_b64)

    salt = blob[:16]
    iv = blob[16:28]
    enc = blob[28:]

    key = PBKDF2(password, salt, dkLen=32, count=100000, hmac_hash_module=hash_module)

    cipher = AES.new(key, AES.MODE_GCM, iv)
    try:
        data = cipher.decrypt(enc)
        return data.decode("utf-8", errors="ignore")
    except:
        return None


def decrypt_cookie_editor(ciphertext_b64, password):
    """
    Try all known Cookie Editor PBKDF2 hash modules.
    """

    hashes = [SHA256, SHA1, SHA512]

    for h in hashes:
        print(f"[*] Trying {h.__name__} ...")
        out = try_decrypt(ciphertext_b64, password, h)
        if out:
            print(f"[+] Successfully decrypted using {h.__name__}")
            return out

    raise Exception("Decryption failed: No compatible hash algorithm worked.")