import hashlib
import random
import string


def stable_hash(key: str):
    str_bytes = bytes(key, "UTF-8")
    m = hashlib.md5(str_bytes)
    return int(m.hexdigest(), base=16)


def random_str(n: int) -> str:
    m = n // len(string.ascii_uppercase + string.digits) + 1
    return ''.join(random.sample((string.ascii_uppercase + string.digits) * m, n))
