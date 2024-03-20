from pytreemap import TreeMap

from typing import TypeVar, Generic

from sortedcontainers import SortedDict

from masternode.main.datanodes.datanode import DataNode
from loguru import logger as LOGGER

T = TypeVar('T')


class ConsistentHashRing(Generic[T]):

    def __init__(self):
        self.bst = TreeMap()
        self.attached_nodes = SortedDict()
        self.free_nodes = SortedDict()

    def all_hashes(self):
        return list(self.bst.key_set())

    def get_node(self, node_hash: int) -> DataNode:
        return self.bst.get(node_hash)

    def put_node(self, node_hash: int, node: DataNode) -> None:
        if not self.bst.contains_key(node_hash):
            self.attached_nodes[node.instance_no()] = node_hash
            self.bst.put(node_hash, node)
        else:
            old_node = self.bst.get(node_hash)
            self.attached_nodes.pop(old_node.instance_no())
            self.attached_nodes[node.instance_no()] = node_hash
            self.bst.put(node_hash, node)
        pass

    def remove_node(self, node_hash):
        node = self.bst.get(node_hash)
        self.attached_nodes.pop(node.instance_no())
        self.bst.remove(node_hash)
        pass

    def has_node(self, new_node: DataNode) -> bool:
        return new_node.instance_no() in self.attached_nodes

    def get_node_hash(self, new_node: DataNode) -> str | None:
        return self.attached_nodes[new_node.instance_no()]

    def update_node(self, new_node: DataNode):
        if self.has_node(new_node):
            node_hash = self.get_node_hash(new_node)
            node = self.bst.get(node_hash)
            node.update_node(new_node)
        pass

    def get_node_with_max_instance_no(self) -> (DataNode, str):
        max_instance_no = max(self.attached_nodes.keys())
        node_hash = self.attached_nodes[max_instance_no]
        node = self.bst.get(node_hash)
        return node, node_hash

    def no_of_free_nodes(self):
        return self.free_nodes.__len__()

    def add_free_node(self, node: DataNode) -> None:
        if self.has_node(node):
            self.update_node(node)
        else:
            LOGGER.info("Adding free node {} ", node)
            instance_no = node.instance_no()
            self.free_nodes[instance_no] = node

    def get_free_node_with_max_instance_no(self) -> DataNode:
        instance_no = max(self.free_nodes.keys())
        return self.free_nodes.get(instance_no)

    def pop_free_node_with_min_instance_no(self) -> DataNode:
        instance_no = min(self.free_nodes.keys())
        return self.free_nodes.pop(instance_no)

    def remove_free_node(self, instance_no: int) -> None:
        self.free_nodes.pop(instance_no)
        pass

    def no_of_active_nodes(self):
        return self.bst.__len__()

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


if __name__ == "__main__":
    bst = ConsistentHashRing()

    for i in range(0, 150, 5):
        bst.put(i, f"Value {i}")

    print(bst)
    # {0=Value 0, 5=Value 5, 10=Value 10, 15=Value 15 ... }

    print(bst.get(50))
    # Value 50

    print(bst.lower_hash(10))
    # Value 5

    print(bst.higher_hash(10))
    # Value 15

    print(bst.lower_entry(6))
    # 5=Value 5

    print(bst.higher_entry(6))
    # 10=Value 10
