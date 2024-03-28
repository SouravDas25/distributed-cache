import json
import random

import pylru
from pytreemap import TreeMap

from masternode.main.common.hashing import stable_hash, random_str

import psutil

from loguru import logger as LOGGER


class Node:

    def __init__(self, server_name: str, cache_size: int):
        self.server_name = server_name
        self.cache = {}
        self.cache_size = cache_size
        self.keys_to_remove = set()

    def __str__(self):
        return self.server_name

    def __repr__(self):
        return self.server_name

    def copy_keys(self, target_node, from_key, to_key):
        for key in list(self.cache.keys()):
            key_hash = stable_hash(key)
            if from_key <= key_hash:
                target_node.put(key, self.cache[key])
                self.keys_to_remove.add(key)
        return True

    def compact_keys(self):
        for key in list(self.cache.keys()):
            if key in self.keys_to_remove:
                self.cache.pop(key)
        self.keys_to_remove.clear()

    def metrics(self):
        # used_memory = psutil.virtual_memory().used
        used_memory = 0
        for key in self.cache:
            used_memory += len(json.dumps(self.cache[key]))
        # LOGGER.info("used_memory: {}", used_memory)
        return {
            "used_memory": used_memory,
            "load": used_memory / self.cache_size
        }

    def put(self, key, value):
        self.cache[key] = value

    def get(self, key):
        return self.cache[key]

    def has(self, key: str):
        return key in self.cache

    def remove(self, key):
        return self.cache.pop(key)

    def calculate_mid_key(self):
        return sum(map(lambda x: stable_hash(x), self.cache.keys())) // self.cache.__len__()


class HashRing:

    def __init__(self, cache_size=10):
        self.ring = TreeMap()
        self.free_nodes = []
        self.ring[0] = Node("node-0", cache_size)
        self.cache_size = cache_size

    def add_node(self, node: Node):
        self.free_nodes.append(node)

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
        LOGGER.info("Splitting Node {}", behind_node)
        ahead_hash = self.ring.higher_key(node_hash)
        new_node = Node(f"node-{len(self.ring)}", self.cache_size)
        mid_hash = behind_node.calculate_mid_key()
        behind_node.copy_keys(new_node, mid_hash, ahead_hash)
        self.ring[mid_hash] = new_node
        behind_node.compact_keys()
        pass

    def merge_node(self, node_hash: int):
        if node_hash != self.ring.first_key():
            current_node = self.ring[node_hash]
            LOGGER.info("Merging Node {}", current_node)
            behind_hash = self.ring.lower_key(node_hash)
            behind_node = self.ring[behind_hash]
            current_node.copy_keys(behind_node, node_hash, None)
            self.ring.remove(node_hash)
        pass

    def balance(self):
        LOGGER.info("Balance Started")
        overloaded_nodes, underloaded_nodes = self.check_nodes()
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


if __name__ == '__main__':
    ring = HashRing(cache_size=100)

    keys = []

    for i in range(20):
        key = random_str(3)
        value = random_str(20)
        ring.put(key, value)
        keys.append(key)
        ring.balance()

    for key in keys:
        if ring.has(key):
            ring.remove(key)
            ring.balance()
