import hashlib


def stableHash(key: str):
    strbytes = bytes(key, "UTF-8")
    m = hashlib.md5(strbytes)
    return int(m.hexdigest(), base=16)


def toCacheIndex(key: str, cacheSize: int):
    return max(stableHash(key) % (cacheSize + 1), 1)
