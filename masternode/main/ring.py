from pytreemap import TreeMap

from typing import TypeVar, Generic

from sortedcontainers import SortedDict

from masternode.main.common.hashing import random_str, stable_hash
from masternode.main.datanodes.datanode import DataNode
from loguru import logger as LOGGER

T = TypeVar('T')


class ConsistentHashRing(Generic[T]):

    def __init__(self):
        self.bst = TreeMap()
        self.__nodes_in_ring = {}
        self.__free_nodes = {}
        self.__blocked_nodes = {}

    def all_active_node_hashes(self):
        return list(self.bst.key_set())

    def get_active_node(self, node_hash: int) -> DataNode:
        return self.bst.get(node_hash)

    def put_active_node(self, node_hash: int, node: DataNode) -> None:
        if not self.bst.contains_key(node_hash):

            if node.instance_no() not in self.__blocked_nodes:
                LOGGER.error("new attaching node not in ib-nodes:, {} {}", node, node_hash)
            else:
                self.__blocked_nodes.pop(node.instance_no())

            self.__nodes_in_ring[node.instance_no()] = node_hash
            self.bst.put(node_hash, node)
        else:
            old_node = self.bst.get(node_hash)
            self.__nodes_in_ring.pop(old_node.instance_no())
            self.__nodes_in_ring[node.instance_no()] = node_hash
            self.bst.put(node_hash, node)
        pass

    def remove_active_node(self, node_hash):
        node = self.bst.get(node_hash)
        self.__nodes_in_ring.pop(node.instance_no())
        self.bst.remove(node_hash)
        pass

    def is_active_node_hash(self, node_hash):
        return self.bst.contains_key(node_hash)

    def is_active_node(self, node: DataNode) -> bool:
        return node.instance_no() in self.__nodes_in_ring

    def is_blocked_node(self, node: DataNode) -> bool:
        return node.instance_no() in self.__blocked_nodes

    def is_free_node(self, node: DataNode) -> bool:
        return node.instance_no() in self.__free_nodes

    def get_active_node_hash(self, new_node: DataNode) -> str | None:
        return self.__nodes_in_ring[new_node.instance_no()]

    def update_node(self, new_node: DataNode):
        if self.is_active_node(new_node):
            node_hash = self.get_active_node_hash(new_node)
            node = self.bst.get(node_hash)
            node.update_node(new_node)
        elif self.is_blocked_node(new_node):
            self.__blocked_nodes[new_node.instance_no()].update_node(new_node)
        elif self.is_free_node(new_node):
            self.__free_nodes[new_node.instance_no()].update_node(new_node)
        pass

    def get_active_node_with_max_instance_no(self) -> (DataNode, str):
        max_instance_no = max(self.__nodes_in_ring.keys())
        node_hash = self.__nodes_in_ring[max_instance_no]
        node = self.bst.get(node_hash)
        return node, node_hash

    def register_new_node(self, node: DataNode) -> None:
        if self.is_active_node(node) or self.is_blocked_node(node) or self.is_free_node(node):
            LOGGER.info("Updating node {} ", node)
            self.update_node(node)
        else:
            LOGGER.info("Registering free node {} ", node)
            instance_no = node.instance_no()
            self.__free_nodes[instance_no] = node

    def free_up_node(self, node: DataNode) -> None:
        instance_no = node.instance_no()
        if self.is_blocked_node(node):
            LOGGER.info("Freeing up in-process node {} ", node)
            self.__blocked_nodes.pop(node.instance_no())
        elif self.is_active_node(node):
            LOGGER.info("Freeing up ring node {} ", node)
            node_hash = self.__nodes_in_ring[instance_no]
            self.__nodes_in_ring.pop(instance_no)
            self.bst.remove(node_hash)
        else:
            LOGGER.info("Freeing up node {} ", node)
        self.__free_nodes[instance_no] = node

    def get_free_node_with_max_instance_no(self) -> DataNode:
        instance_no = max(self.__free_nodes.keys())
        return self.__free_nodes.get(instance_no)

    def pop_free_node_with_min_instance_no(self) -> DataNode:
        instance_no = min(self.__free_nodes.keys())
        self.__blocked_nodes[instance_no] = self.__free_nodes.pop(instance_no)
        return self.__blocked_nodes[instance_no]

    def remove_blocked_or_free_node(self, instance_no: int) -> None:
        if instance_no in self.__blocked_nodes:
            self.__blocked_nodes.pop(instance_no)
        if instance_no in self.__free_nodes:
            self.__free_nodes.pop(instance_no)

        pass

    def no_of_active_nodes(self):
        return self.bst.__len__()

    def no_of_free_nodes(self):
        return self.__free_nodes.__len__()

    def last_hash(self):
        return self.bst.last_key()

    def first_hash(self):
        return self.bst.first_key()

    def higher_hash(self, node_key):
        return self.bst.higher_key(node_key)

    def lower_hash(self, node_key):
        return self.bst.lower_key(node_key)

    def floor_hash(self, key_hash):
        return self.bst.floor_key(key_hash)

    def free_nodes(self):
        return self.__free_nodes

    def no_of_blocked_nodes(self) -> int:
        return self.__blocked_nodes.__len__()

    def resolve_node(self, key: str) -> DataNode:
        key_hash = stable_hash(key)
        key_hash = self.floor_hash(key_hash)
        if key_hash is None:
            key_hash = self.last_hash()
        return self.get_active_node(key_hash)
