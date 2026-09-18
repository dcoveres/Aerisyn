# axiso.py
# Crypto library by s5v3
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.primitives.kdf.hkdf import HKDF as _HKDF
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.fernet import Fernet      
from typing import Union, Optional     
import secrets    
import base64   
import hashlib  
import hmac 

error_decrypt = "Не удалось расшифровать данные. Возможно, ключ неверен или данные повреждены." 

# реализация ChaCha20-Poly1305
class ChaCha20:
    NONCE_SIZE = 12

    @staticmethod
    def generate_key() -> dict:
        raw_key = secrets.token_bytes(32)
        b64_key = base64.urlsafe_b64encode(raw_key).decode('utf-8')
        return {"key": b64_key}

    @staticmethod
    def _normalize_key(key: Union[bytes, str]) -> bytes:
        if isinstance(key, str):
            try:
                raw = base64.urlsafe_b64decode(key)
            except Exception:
                raise ValueError("Key must be a valid URL-safe base64 string")
            if len(raw) != 32:
                raise ValueError("Decoded key must be exactly 32 bytes")
            return raw
        elif isinstance(key, bytes):
            if len(key) == 32:
                return key
            try:
                key_str = key.decode('utf-8')
                raw = base64.urlsafe_b64decode(key_str)
            except Exception:
                raise ValueError("Key bytes must be a valid URL-safe base64 string or 32 raw bytes")
            if len(raw) != 32:
                raise ValueError("Decoded key must be exactly 32 bytes")
            return raw
        else:
            raise TypeError("Key must be of type str or bytes")

    @staticmethod
    def encrypt(data: bytes, key: Union[bytes, str]) -> bytes:
        key_bytes = ChaCha20._normalize_key(key)
        nonce = secrets.token_bytes(ChaCha20.NONCE_SIZE)
        cipher = ChaCha20Poly1305(key_bytes)
        ciphertext = cipher.encrypt(nonce, data, None) 
        return nonce + ciphertext

    @staticmethod
    def decrypt(enc: bytes, key: Union[bytes, str]) -> bytes:
        if len(enc) < ChaCha20.NONCE_SIZE:
            raise ValueError("Encrypted data too short to contain nonce")
        nonce = enc[:ChaCha20.NONCE_SIZE]
        ciphertext = enc[ChaCha20.NONCE_SIZE:]
        key_bytes = ChaCha20._normalize_key(key)
        cipher = ChaCha20Poly1305(key_bytes)
        try:
            return cipher.decrypt(nonce, ciphertext, None)
        except Exception as e:
            raise ValueError(error_decrypt) from e
            
# реализация цифровой подписи
class EdDSA:
    @staticmethod
    def generate_keypair() -> dict:
        private_key = Ed25519PrivateKey.generate()
        public_key = private_key.public_key()

        private_raw = private_key.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption(),
        )
        public_raw = public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )

        return {
            "private_key": base64.urlsafe_b64encode(private_raw).decode("utf-8"),
            "public_key": base64.urlsafe_b64encode(public_raw).decode("utf-8"),
        }

    @staticmethod
    def private_key_from_b64(private_b64: str) -> Ed25519PrivateKey:
        raw = base64.urlsafe_b64decode(private_b64)
        return Ed25519PrivateKey.from_private_bytes(raw)

    @staticmethod
    def public_key_from_b64(public_b64: str) -> Ed25519PublicKey:
        raw = base64.urlsafe_b64decode(public_b64)
        return Ed25519PublicKey.from_public_bytes(raw)

    @staticmethod
    def private_key_to_bytes(private_key: Ed25519PrivateKey) -> bytes:
        return private_key.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption(),
        )

    @staticmethod
    def public_key_to_bytes(public_key: Ed25519PublicKey) -> bytes:
        return public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )

    @staticmethod
    def _ensure_private_key(
        private_key: Union[str, Ed25519PrivateKey],
    ) -> Ed25519PrivateKey:
        if isinstance(private_key, str):
            return EdDSA.private_key_from_b64(private_key)
        return private_key

    @staticmethod
    def _ensure_public_key(public_key: Union[str, Ed25519PublicKey]) -> Ed25519PublicKey:
        if isinstance(public_key, str):
            return EdDSA.public_key_from_b64(public_key)
        return public_key

    @staticmethod
    def sign(data: bytes, private_key: Union[str, Ed25519PrivateKey]) -> bytes:
        priv = EdDSA._ensure_private_key(private_key)
        return priv.sign(data)

    @staticmethod
    def verify(
        data: bytes,
        signature: bytes,
        public_key: Union[str, Ed25519PublicKey],
    ) -> bool:
        pub = EdDSA._ensure_public_key(public_key)
        try:
            pub.verify(signature, data)
            return True
        except Exception:
            return False
            
# реализация HKDF
class HKDF:
    @staticmethod
    def extract_and_expand(salt: Optional[bytes], ikm: bytes, info: bytes, length: int) -> bytes:
        hkdf = _HKDF(
        algorithm=hashes.SHA256(),
        length=length,
        salt=salt,
        info=info,
        backend=default_backend()
        )
        return hkdf.derive(ikm)

#Реализация Diffie Helman
class DH:
    @staticmethod
    def generate_keypair():
        private_key = X25519PrivateKey.generate()
        public_key = private_key.public_key()
        private_raw = private_key.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption()
        )
        public_raw = public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        )
        private_b64 = base64.urlsafe_b64encode(private_raw).decode('utf-8')
        public_b64 = base64.urlsafe_b64encode(public_raw).decode('utf-8')
        return {
            "private_key": private_b64,
            "public_key": public_b64
        }

    @staticmethod
    def private_key_from_b64(private_b64: str) -> X25519PrivateKey:
        raw = base64.urlsafe_b64decode(private_b64)
        return DH.private_key_from_bytes(raw)

    @staticmethod
    def private_key_from_bytes(private_bytes: bytes) -> X25519PrivateKey:
        if len(private_bytes) != 32:
            raise ValueError("Private key must be exactly 32 bytes")
        return X25519PrivateKey.from_private_bytes(private_bytes)

    @staticmethod
    def private_key_to_bytes(private_key: X25519PrivateKey) -> bytes:
        return private_key.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption()
        )

    @staticmethod
    def derive_shared_secret(private_key_b64: str, 
                             peer_public_bytes: bytes,
                             length: int = 32,
                             salt: bytes = None,
                             info: bytes = b'') -> bytes:
        if len(peer_public_bytes) != 32:
            raise ValueError("Peer public key must be exactly 32 bytes")
        private_key = DH.private_key_from_b64(private_key_b64)
        peer_public = X25519PublicKey.from_public_bytes(peer_public_bytes)
        raw_shared = private_key.exchange(peer_public)
        return HKDF.extract_and_expand(salt, raw_shared, info, length)

# реализация генератора ключей
class KeyGen:
    @staticmethod
    def generate_aes(key_size: int = 32) -> dict:
        raw_key = secrets.token_bytes(key_size)
        b64_key = base64.urlsafe_b64encode(raw_key).decode('utf-8')
        return {"key": b64_key}

    @staticmethod
    def generate_rsa(key_size: int = 2048) -> dict:
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=key_size,
            backend=default_backend()
        )
        public_key = private_key.public_key()

        pem_private = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        pem_public = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )

        b64_private = base64.b64encode(pem_private).decode('utf-8')
        b64_public = base64.b64encode(pem_public).decode('utf-8')
        return {
            "private_key": b64_private,
            "public_key": b64_public
        }

# реализация проверки подлинности (HMAC)
class HMAC:    
    @staticmethod
    def _to_bytes(value: Union[bytes, str]) -> bytes:
        if isinstance(value, str):
            return value.encode('utf-8')
        elif isinstance(value, bytes):
            return value
        else:
            raise TypeError("Value must be bytes or str")

    @staticmethod
    def create_sign(message: Union[bytes, str], secret: Union[bytes, str]) -> bytes:
        msg_bytes = HMAC._to_bytes(message)
        secret_bytes = HMAC._to_bytes(secret)
        return hmac.new(secret_bytes, msg_bytes, hashlib.sha256).digest()

    @staticmethod
    def verify(msg: Union[bytes, str], reqv_signature: bytes, secret: Union[bytes, str]) -> bool:
        msg_bytes = HMAC._to_bytes(msg)
        secret_bytes = HMAC._to_bytes(secret)
        try:
            expected = hmac.new(secret_bytes, msg_bytes, hashlib.sha256).digest()
            return hmac.compare_digest(expected, reqv_signature)
        except Exception:
            return False
            
# реализация ассиметричного шифрования RSA
class RSA:
    @staticmethod
    def _max_data_size(public_key: rsa.RSAPublicKey) -> int:
        key_size_bytes = public_key.key_size // 8
        hash_size = 32  # SHA256
        return key_size_bytes - 2 * hash_size - 2

    @staticmethod
    def public_key_from_b64(public_b64: str) -> rsa.RSAPublicKey:
        pem_data = base64.b64decode(public_b64)
        return serialization.load_pem_public_key(pem_data, backend=default_backend())

    @staticmethod
    def private_key_from_b64(private_b64: str) -> rsa.RSAPrivateKey:
        pem_data = base64.b64decode(private_b64)
        return serialization.load_pem_private_key(pem_data, password=None, backend=default_backend())

    @staticmethod
    def _ensure_public_key(public_key: Union[str, rsa.RSAPublicKey]) -> rsa.RSAPublicKey:
        if isinstance(public_key, str):
            return RSA.public_key_from_b64(public_key)
        return public_key

    @staticmethod
    def _ensure_private_key(private_key: Union[str, rsa.RSAPrivateKey]) -> rsa.RSAPrivateKey:
        if isinstance(private_key, str):
            return RSA.private_key_from_b64(private_key)
        return private_key

    @staticmethod
    def encrypt(data: bytes, public_key: Union[str, rsa.RSAPublicKey]) -> bytes:
        pub = RSA._ensure_public_key(public_key)
        max_size = RSA._max_data_size(pub)
        if len(data) > max_size:
            raise ValueError(
                f"Данные слишком большие для RSA шифрования. "
                f"Максимум {max_size} байт для данного ключа."
            )
        return pub.encrypt(
            data,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )

    @staticmethod
    def decrypt(enc: bytes, private_key: Union[str, rsa.RSAPrivateKey]) -> bytes:
        priv = RSA._ensure_private_key(private_key)
        try:
            return priv.decrypt(
                enc,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
        except ValueError as e:
            raise ValueError(error_decrypt) from e
        except Exception as e:
            error_rsa = f"Произошла ошибка при расшифровке: {e}"
            raise RuntimeError(error_rsa) from e
            
#симитричное шифрование Fernet   
# ВНИМАНИЕ ЭТО FERNET, А НЕ ГОЛЫЙ AES.   
class AES:
    @staticmethod
    def _normalize_key(key):
        if isinstance(key, str):
            try:
                raw = base64.urlsafe_b64decode(key)
            except Exception:
                raise ValueError("Key must be a valid URL-safe base64 string")
            if len(raw) != 32:
                raise ValueError("Decoded key must be exactly 32 bytes")
            return key.encode('utf-8')
        elif isinstance(key, bytes):
            try:
                key_str = key.decode('utf-8')
            except UnicodeDecodeError:
                raise ValueError("Key bytes must be a UTF-8 encoded base64 string")
            try:
                raw = base64.urlsafe_b64decode(key_str)
            except Exception:
                raise ValueError("Key bytes must contain a valid URL-safe base64")
            if len(raw) != 32:
                raise ValueError("Decoded key must be exactly 32 bytes")
            return key
        else:
            raise TypeError("Key must be of type str or bytes")

    @staticmethod
    def encrypt(data: bytes, key: Union[bytes, str]) -> bytes:
        normalized = AES._normalize_key(key)
        f = Fernet(normalized)
        return f.encrypt(data)

    @staticmethod
    def decrypt(enc: bytes, key: Union[bytes, str]) -> bytes:
        normalized = AES._normalize_key(key)
        f = Fernet(normalized)
        return f.decrypt(enc)