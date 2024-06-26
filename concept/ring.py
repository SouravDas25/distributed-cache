from pytreemap import TreeMap

from concept.node import Node
from concept.node_set import NodeSet

from loguru import logger as LOGGER

from masternode.main.common.hashing import stable_hash


# this class contains the bst where all the nodes
# max_node: the maximum no of Node set allowed to create
# replication_factor: no of replica allowed in a node set
class HashRing:

    def __init__(self, cache_size: int, max_node: int, replication_factor: int) -> None:
        self.replication_factor = replication_factor
        self.max_node = max_node
        self.ring = TreeMap()
        self.cache_size = cache_size
        self.node_counter = 1
        # base node at hash 0
        self.ring.put(0, NodeSet(replication_factor, 0, cache_size))

    # find out the overloaded & underloaded nodes
    def check_nodes(self):
        overloaded_nodes = []
        underloaded_nodes = []
        for node_hash in self.ring.key_set():
            node_set = self.ring.get(node_hash)
            metrics = node_set.metrics()
            load = metrics['load']
            LOGGER.info("{} metrics - {}", node_set, metrics)
            # if usage crosses above 80 %, split the data between to node
            if load > 0.8:
                overloaded_nodes.append(node_hash)
            # if usage falls below 15 %, then merge the node with the previous node
            if load < 0.15:
                underloaded_nodes.append(node_hash)
        return overloaded_nodes, underloaded_nodes

    # split a node into two
    def split_node(self, node_hash: int):
        # get the current node
        current_node = self.ring.get(node_hash)
        LOGGER.info("Splitting {}", current_node)
        # get next node hash
        ahead_hash = self.ring.higher_key(node_hash)
        # create a new node set
        new_node = NodeSet(self.replication_factor, self.node_counter, self.cache_size)
        # calculate the hash in-between the two nodes
        mid_hash = current_node.calculate_mid_key()
        LOGGER.info("Moving data {} -> {}", current_node, new_node)
        # move half the data to new node
        current_node.copy_keys(new_node, mid_hash, ahead_hash)
        # place the new node in-between the two nodes
        self.ring.put(mid_hash, new_node)
        # remove copied keys from current node
        current_node.compact_keys()
        self.node_counter += 1
        pass

    # merge the data with previous node, and delete the current node
    def merge_node(self, node_hash: int):
        # should never remove the first node, at 0 hash
        if node_hash == self.ring.first_key():
            return
        # get current node
        current_node = self.ring.get(node_hash)
        LOGGER.info("Merging Node {}", current_node)
        # get the previous node
        behind_hash = self.ring.lower_key(node_hash)
        behind_node = self.ring.get(behind_hash)

        LOGGER.info("Moving data {} -> {}", current_node, behind_node)
        # copy the data from current node to previous node
        current_node.copy_keys(behind_node, node_hash, None)
        # remove node from the ring
        self.ring.remove(node_hash)
        pass

    # call this method after a fixed interval
    def balance(self):
        LOGGER.info("Balance Started")
        # get overloaded & underloaded nodes
        overloaded_nodes, underloaded_nodes = self.check_nodes()
        if len(self.ring) < self.max_node:
            for node_hash in overloaded_nodes:
                # split overloaded nodes
                self.split_node(node_hash)

        for node_hash in underloaded_nodes:
            # merge underloaded nodes
            self.merge_node(node_hash)
        LOGGER.info("Balance Completed")
        pass

    # get nearest node from key hash
    def resolve_node(self, key: str) -> Node:
        # hash the key
        key_hash = stable_hash(key)
        # get nearest lower node from key
        key_hash = self.ring.floor_key(key_hash)
        # if nearest lower node not present get last node
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
