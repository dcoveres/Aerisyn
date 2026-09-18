import pytest
import base64
from axiso import ChaCha20, EdDSA, DH, HKDF, RSA, HMAC, AES, KeyGen

def test_chacha20_generate_key():
    key = ChaCha20.generate_key()
    assert "key" in key
    assert len(base64.urlsafe_b64decode(key["key"])) == 32

def test_chacha20_encrypt_decrypt():
    key = ChaCha20.generate_key()["key"]
    data = b"Hello, World!"
    encrypted = ChaCha20.encrypt(data, key)
    decrypted = ChaCha20.decrypt(encrypted, key)
    assert decrypted == data

def test_chacha20_invalid_key():
    with pytest.raises(ValueError):
        ChaCha20._normalize_key("invalid")

def test_eddsa_sign_verify():
    keys = EdDSA.generate_keypair()
    data = b"test data"
    signature = EdDSA.sign(data, keys["private_key"])
    assert EdDSA.verify(data, signature, keys["public_key"]) is True
    assert EdDSA.verify(b"wrong", signature, keys["public_key"]) is False

def test_eddsa_key_serialization():
    keys = EdDSA.generate_keypair()
    priv = EdDSA.private_key_from_b64(keys["private_key"])
    pub = EdDSA.public_key_from_b64(keys["public_key"])
    assert EdDSA.private_key_to_bytes(priv) == base64.urlsafe_b64decode(keys["private_key"])
    assert EdDSA.public_key_to_bytes(pub) == base64.urlsafe_b64decode(keys["public_key"])

def test_dh_key_exchange():
    alice = DH.generate_keypair()
    bob = DH.generate_keypair()
    alice_priv = alice["private_key"]
    bob_pub = base64.urlsafe_b64decode(bob["public_key"])
    shared_alice = DH.derive_shared_secret(alice_priv, bob_pub, length=32)
    bob_priv = bob["private_key"]
    alice_pub = base64.urlsafe_b64decode(alice["public_key"])
    shared_bob = DH.derive_shared_secret(bob_priv, alice_pub, length=32)
    assert shared_alice == shared_bob

def test_hkdf():
    ikm = b"secret"
    salt = b"salt"
    info = b"info"
    result = HKDF.extract_and_expand(salt, ikm, info, 32)
    assert len(result) == 32

def test_rsa_encrypt_decrypt():
    keys = KeyGen.generate_rsa(key_size=2048)
    data = b"short data"
    encrypted = RSA.encrypt(data, keys["public_key"])
    decrypted = RSA.decrypt(encrypted, keys["private_key"])
    assert decrypted == data

def test_rsa_max_size():
    keys = KeyGen.generate_rsa(key_size=1024)
    pub = RSA.public_key_from_b64(keys["public_key"])
    max_size = RSA._max_data_size(pub)
    # для 1024 бит, SHA256 -> 1024/8 - 2*32 - 2 = 128 - 64 - 2 = 62
    assert max_size == 62

def test_hmac():
    msg = b"message"
    secret = b"secret"
    sig = HMAC.create_sign(msg, secret)
    assert HMAC.verify(msg, sig, secret) is True
    assert HMAC.verify(b"wrong", sig, secret) is False

def test_aes_fernet():
    # AES использует Fernet, ключ должен быть 32-байтовый base64
    key = base64.urlsafe_b64encode(b"a"*32).decode()
    data = b"secret"
    enc = AES.encrypt(data, key)
    dec = AES.decrypt(enc, key)
    assert dec == data