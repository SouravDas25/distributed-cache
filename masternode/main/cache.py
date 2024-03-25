import os

from masternode.main.autoscaler.autoscaler_factory import create_autoscaler
from masternode.main.datanodes.datanode import DataNode
from masternode.main.load_balancer import RingLoadBalancer

from loguru import logger as LOGGER

from masternode.main.ring import ConsistentHashRing


class CacheConfig:

    def __init__(self):
        self.config = ""
        self.cache_size = int(os.environ.get("CACHE_SIZE", 2))
        self.scale_factor = 2
        self.autoscaler_type = os.environ.get("AUTOSCALER_TYPE", "NA")


class DistributedCache:

    def __init__(self, config):
        self.config = config
        self.ring: ConsistentHashRing[DataNode] = ConsistentHashRing()
        self.autoscaler = create_autoscaler(self.config.autoscaler_type, self)
        self.load_balancer = RingLoadBalancer(self.config.cache_size, self.ring, self.autoscaler)

    def register_new_node(self, node: DataNode):
        self.ring.register_new_node(node)

    def put(self, key, value):
        node = self.load_balancer.resolve_node(key)
        LOGGER.info(f"Server for key {key} : {node.name()}")
        node.put(key, value)

    def get(self, key):
        node = self.load_balancer.resolve_node(key)
        LOGGER.info(f"Server for key {key} : {node.name()}")
        return node.get(key)

    def has(self, key):
        node = self.load_balancer.resolve_node(key)
        LOGGER.info(f"Server for key {key} : {node.name()}")
        return node.has(key)

    def remove(self, key):
        node = self.load_balancer.resolve_node(key)
        LOGGER.info(f"Server for key {key} : {node.name()}")
        return node.remove(key)

    def load(self) -> float:
        return self.load_balancer.load()

    def status(self):
        return self.load_balancer.status()
        pass
