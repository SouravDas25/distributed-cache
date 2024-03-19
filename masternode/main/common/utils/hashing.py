import hashlib
import random
import string


def stableHash(key: str):
    strbytes = bytes(key, "UTF-8")
    m = hashlib.md5(strbytes)
    return int(m.hexdigest(), base=16)


def toCacheIndex(key: str, cacheSize: int):
    return max(stableHash(key) % (cacheSize + 1), 1)


def random_str(n: int) -> str:
    return ''.join(random.sample(string.ascii_uppercase + string.digits, n))
