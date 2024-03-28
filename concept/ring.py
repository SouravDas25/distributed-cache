from pytreemap import TreeMap

from concept.node import Node
from concept.node_set import NodeSet

from loguru import logger as LOGGER

from masternode.main.common.hashing import stable_hash


class HashRing:

    def __init__(self, cache_size: int, max_node: int, replication_factor: int) -> None:
        self.replication_factor = replication_factor
        self.max_node = max_node
        self.ring = TreeMap()
        self.ring[0] = NodeSet(replication_factor, 0, cache_size)
        self.cache_size = cache_size
        self.node_counter = 1

    def check_nodes(self):
        overloaded_nodes = []
        underloaded_nodes = []
        for node_hash in self.ring.key_set():
            node = self.ring.get(node_hash)
            metrics = node.metrics()
            load = metrics['load']
            LOGGER.info("{} metrics - {}", node, metrics)
            if load > 0.8:
                overloaded_nodes.append(node_hash)
            if load < 0.15:
                underloaded_nodes.append(node_hash)
        return overloaded_nodes, underloaded_nodes

    def split_node(self, node_hash: int):
        behind_node = self.ring[node_hash]
        LOGGER.info("Splitting {}", behind_node)

        ahead_hash = self.ring.higher_key(node_hash)
        new_node = NodeSet(self.replication_factor, self.node_counter, self.cache_size)
        mid_hash = behind_node.calculate_mid_key()
        LOGGER.info("Moving data {} -> {}", behind_node, new_node)
        behind_node.copy_keys(new_node, mid_hash, ahead_hash)
        self.ring[mid_hash] = new_node
        behind_node.compact_keys()
        self.node_counter += 1
        pass

    def merge_node(self, node_hash: int):
        if node_hash == self.ring.first_key():
            return
        current_node = self.ring[node_hash]
        LOGGER.info("Merging Node {}", current_node)
        behind_hash = self.ring.lower_key(node_hash)
        behind_node = self.ring[behind_hash]

        LOGGER.info("Moving data {} -> {}", current_node, behind_node)
        current_node.copy_keys(behind_node, node_hash, None)
        self.ring.remove(node_hash)
        pass

    def balance(self):
        LOGGER.info("Balance Started")

        overloaded_nodes, underloaded_nodes = self.check_nodes()
        if len(self.ring) < self.max_node:
            for node_hash in overloaded_nodes:
                self.split_node(node_hash)

        for node_hash in underloaded_nodes:
            self.merge_node(node_hash)
        LOGGER.info("Balance Completed")
        pass

    def resolve_node(self, key: str) -> Node:
        key_hash = stable_hash(key)
        key_hash = self.ring.floor_key(key_hash)
        if key_hash is None:
            key_hash = self.ring.last_key()
        return self.ring.get(key_hash)

    def put(self, key, value):
        node = self.resolve_node(key)
        node.put(key, value)
        return True

    def get(self, key):
        node = self.resolve_node(key)
        return node.get(key)

    def has(self, key):
        node = self.resolve_node(key)
        return node.has(key)

    def remove(self, key):
        node = self.resolve_node(key)
        return node.remove(key)
