import os

from masternode.main.autoscaler.autoscaler_factory import create_autoscaler
from masternode.main.datanodes.datanode import DataNode
from masternode.main.load_balancer import RingLoadBalancer

from loguru import logger as LOGGER

from masternode.main.ring import ConsistentHashRing


class ClusterConfig:

    def __init__(self):
        self.config = ""
        self.map_size = int(os.environ.get("MAP_SIZE", 2))
        self.scale_factor = 2
        self.autoscaler_type = os.environ.get("AUTOSCALER_TYPE", "NA")


class DistributedMap:

    def __init__(self, config):
        self.config = config
        self.ring: ConsistentHashRing[DataNode] = ConsistentHashRing()
        self.autoscaler = create_autoscaler(self.config.autoscaler_type, self)
        self.load_balancer = RingLoadBalancer(self.config.map_size, self.ring, self.autoscaler)

    def register_new_node(self, node: DataNode):
        self.ring.register_new_node(node)

    def put(self, key, value):
        node = self.ring.resolve_node(key)
        LOGGER.info(f"Server for key {key} : {node.name()}")
        node.put(key, value)

    def get(self, key):
        node = self.ring.resolve_node(key)
        LOGGER.info(f"Server for key {key} : {node.name()}")
        return node.get(key)

    def has(self, key):
        node = self.ring.resolve_node(key)
        LOGGER.info(f"Server for key {key} : {node.name()}")
        return node.has(key)

    def remove(self, key):
        node = self.ring.resolve_node(key)
        LOGGER.info(f"Server for key {key} : {node.name()}")
        return node.remove(key)

    def load(self) -> float:
        return self.load_balancer.load()

    def status(self):
        return self.load_balancer.status()
        pass
