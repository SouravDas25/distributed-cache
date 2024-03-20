import hashlib
import random
import string


def stable_hash(key: str):
    str_bytes = bytes(key, "UTF-8")
    m = hashlib.md5(str_bytes)
    return int(m.hexdigest(), base=16)


def random_str(n: int) -> str:
    return ''.join(random.sample(string.ascii_uppercase + string.digits, n))
