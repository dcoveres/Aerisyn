import pytest
import base64
from protocol import (
    serialize, deserialize, compress, decompress,
    encrypt_control, decrypt_control,
    encrypt_stream, decrypt_stream,
    derive_stream_key,
    compute_file_checksum,
    validate_email, validate_username,
    serialize_sorted,
)
from axiso import HKDF

def test_serialize_deserialize():
    obj = {"a": 1, "b": [2, 3]}
    data = serialize(obj)
    assert deserialize(data) == obj

def test_compress_decompress():
    original = b"hello" * 100
    compressed = compress(original)
    assert len(compressed) < len(original)
    assert decompress(compressed) == original

def test_encrypt_decrypt_control():
    # Генерируем 32-байтовый ключ в виде base64-строки
    raw_key = b"a" * 32
    key = base64.urlsafe_b64encode(raw_key).decode()  # строка
    data = b"test data"
    encrypted = encrypt_control(data, key)
    decrypted = decrypt_control(encrypted, key)
    assert decrypted == data

def test_derive_stream_key():
    session_key = b"a"*32
    email = "test@example.com"
    stream_id = 123
    key1 = derive_stream_key(session_key, email, stream_id)
    key2 = derive_stream_key(session_key, email, stream_id)
    assert key1 == key2
    assert len(key1) == 32

def test_compute_file_checksum():
    data = b"hello"
    checksum = compute_file_checksum(data)
    assert isinstance(checksum, str)
    assert len(checksum) == 64

def test_validate_email():
    assert validate_email("user@example.com") is True
    assert validate_email("user@.com") is False
    assert validate_email("") is False

def test_validate_username():
    assert validate_username("user_123") is True
    assert validate_username("ab") is False  # меньше 4 символов
    assert validate_username("user!") is False  # недопустимый символ

def test_serialize_sorted():
    obj = {"b": 2, "a": 1, "c": {"z": 0, "y": 9}}
    data = serialize_sorted(obj)
    # Проверяем, что при десериализации ключи отсортированы
    restored = deserialize(data)
    assert list(restored.keys()) == ["a", "b", "c"]
    assert list(restored["c"].keys()) == ["y", "z"]