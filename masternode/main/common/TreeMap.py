from pytreemap import TreeMap

from typing import TypeVar, Generic

from sortedcontainers import SortedDict

from masternode.main.datanodes.datanode import DataNode

T = TypeVar('T')


class TreeDict(Generic[T]):

    def __init__(self):
        self.bst = TreeMap()
        self.attached_nodes = SortedDict()

    def keys(self):
        return list(self.bst.key_set())

    def get_node(self, node_key: int) -> DataNode:
        return self.bst.get(node_key)

    def put_node(self, node_key: int, node: DataNode) -> None:
        if not self.bst.contains_key(node_key):
            self.attached_nodes[node.instance_no()] = node_key
            self.bst.put(node_key, node)
        else:
            old_node = self.bst.get(node_key)
            self.attached_nodes.pop(old_node.instance_no())
            self.attached_nodes[node.instance_no()] = node_key
            self.bst.put(node_key, node)
        pass

    def remove_node(self, node_key):
        node = self.bst.get(node_key)
        self.attached_nodes.pop(node.instance_no())
        self.bst.remove(node_key)
        pass

    def has_node(self, new_node: DataNode) -> bool:
        return new_node.instance_no() in self.attached_nodes

    def get_node_key(self, new_node: DataNode) -> str | None:
        return self.attached_nodes[new_node.instance_no()]

    def update_node(self, new_node: DataNode):
        if self.has_node(new_node):
            node_key = self.get_node_key(new_node)
            node = self.bst.get(node_key)
            node.update_node(new_node)
        pass

    def get_max_node(self) -> (DataNode, str):
        max_node = max(self.attached_nodes.keys())
        node_key = self.attached_nodes[max_node]
        node = self.bst.get(node_key)
        return node, node_key

    def __len__(self):
        return self.bst.__len__()

    def last_key(self):
        return self.bst.last_key()

    def first_key(self):
        return self.bst.first_key()

    def higher_key(self, node_key):
        return self.bst.higher_key(node_key)

    def lower_key(self, node_key):
        return self.bst.lower_key(node_key)

    def floor_key(self, key_hash):
        return self.bst.floor_key(key_hash)


if __name__ == "__main__":
    bst = TreeDict()

    for i in range(0, 150, 5):
        bst.put(i, f"Value {i}")

    print(bst)
    # {0=Value 0, 5=Value 5, 10=Value 10, 15=Value 15 ... }

    print(bst.get(50))
    # Value 50

    print(bst.lower_key(10))
    # Value 5

    print(bst.higher_key(10))
    # Value 15

    print(bst.lower_entry(6))
    # 5=Value 5

    print(bst.higher_entry(6))
    # 10=Value 10
