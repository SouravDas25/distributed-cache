import os

from masternode.main.autoscaler.autoscaler_factory import create_autoscaler
from masternode.main.ring import ConsistentHashRing

from loguru import logger as LOGGER


class CacheConfig:

    def __init__(self):
        self.config = ""
        self.cache_size = int(os.environ.get("CACHE_SIZE", 2))
        self.scale_factor = 2
        self.autoscaler_type = os.environ.get("AUTOSCALER_TYPE", "NA")


class DistributedCache:

    def __init__(self, config):
        self.config = config
        self.autoscaler = create_autoscaler(self.config.autoscaler_type, self)
        self.ring = ConsistentHashRing(self.config.cache_size, self.autoscaler)

    def put(self, key, value):
        node = self.ring.resolve_node(key)
        LOGGER.info(f"Server for key {key} : {node.name()}")
        node.put(key, value)

    def get(self, key):
        node = self.ring.resolve_node(key)
        return node.get(key)

    def has(self, key):
        node = self.ring.resolve_node(key)
        return node.has(key)

    def remove(self, key):
        node = self.ring.resolve_node(key)
        return node.remove(key)

    def load(self) -> float:
        return self.ring.load()

    def status(self):
        return self.ring.status()
        pass

