import os

from masternode.main.autoscaler.autoscaler_factory import create_autoscaler
from masternode.main.ring import ConsistentHashRing


class CacheConfig:

    def __init__(self):
        self.config = ""
        self.cacheSize = int(os.environ.get("CACHE_SIZE", 2))
        self.scaleFactor = 2
        self.autoscaler_type = os.environ.get("AUTOSCALER_TYPE", "LOCAL")


class DistributedCache:

    def __init__(self):
        self.config = CacheConfig()
        self.autoscaler = create_autoscaler(self.config.autoscaler_type, self)
        self.ring = ConsistentHashRing(self.config.cacheSize, self.autoscaler)

    def put(self, key, value):
        node = self.ring.get_node(key)
        print(f"Server for key {key} : {node.name()}")
        node.put(key, value)

    def get(self, key):
        node = self.ring.get_node(key)
        return node.get(key)

    def has(self, key):
        node = self.ring.get_node(key)
        return node.has(key)

    def remove(self, key):
        node = self.ring.get_node(key)
        return node.remove(key)

    def status(self):
        return self.ring.status()
        pass


if __name__ == "__main__":
    dc = DistributedCache()
    dc.put("apple", "pie")
    print(dc.get("apple"))
    print(dc.has("apple"))
