from cryptography.fernet import Fernet
import os

GOOGLE_CREDENTIALS_KEY = os.environ.get("GOOGLE_ENCRYPT_CREDENTIALS_KEY")


def string_to_byte(string):
    return string.encode('UTF-8')


def byte_to_string(byte):
    return byte.decode('UTF-8')


def secure(string):
    encode_string = string_to_byte(string)
    encode_key = string_to_byte(GOOGLE_CREDENTIALS_KEY)
    cipher_suite = Fernet(encode_key)
    cipher_text = cipher_suite.encrypt(encode_string)
    return byte_to_string(cipher_text)


def decrypt_secure(string):
    encode_string = string_to_byte(string)
    encode_key = string_to_byte(GOOGLE_CREDENTIALS_KEY)
    cipher_suite = Fernet(encode_key)
    plain_text = cipher_suite.decrypt(encode_string)
    return byte_to_string(plain_text)
