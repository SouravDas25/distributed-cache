import hashlib

from masternode.main.ring import ConsistentHashRing


class Node:

    def __init__(self, server_name, server_url, authentication):
        self.server_name = server_name
        self.server_url = server_url
        self.authentication = authentication
        pass

    def save(self, key, value):
        # call save api of node
        pass

    def get(self, key):
        # call get api of node
        pass

    def has(self, key):
        # call has api of node
        pass

    def remove(self, key):
        # call remove api of node
        pass

    def metrics(self):
        # get metrics.txt of node
        pass

    def copy_keys(self, other_node, fromKey, toKey):
        # copy from - to keys to other node
        pass

    def calculate_mid_key(self):
        # calculate mid value of all keys
        pass


def stable_hash(key):
    return hashlib.sha256(key.encode()).hexdigest()

class HashRing:

    def __init__(self, replication_factor=1):
        self.replication_factor = replication_factor
        self.ring = ConsistentHashRing()
        self.free_nodes = []
        self.ring[0] = Node("A", "B", "C")

    def add_node(self, node):
        self.free_nodes.append(node)

    def check_nodes(self, ):
        overloaded_nodes = []
        underloaded_nodes = []
        for key, node in self.ring.all_active_node_hashes():
            metrics = node.metrics()
            load = metrics.load
            if load > 0.75:
                overloaded_nodes.append(key)
            if load < 0.1:
                underloaded_nodes.append(key)
        return overloaded_nodes, underloaded_nodes

    def split_node(self, key):
        behind_node = self.ring[key]
        ahead_entry = self.ring.higher_entry(key)
        ahead_node = ahead_entry.value
        free_node = self.free_nodes.pop()
        try:
            current_key = behind_node.calculate_mid_key()
            behind_node.copy_keys(free_node, current_key, ahead_entry.key)
            self.ring[current_key] = free_node
        except:
            self.free_nodes.append(ahead_node)
        pass

    def remove_node(self, key):
        behide_entry = self.ring.lower_entry(key)
        if behide_entry.key != self.ring.first_hash():
            current_node = self.ring[key]
            current_node.move_keys(behide_entry.value, key, None)
            free_node = self.ring.remove(key)
            self.free_nodes.append(free_node)
        pass

    def balance(self):
        overloaded_nodes, underloaded_nodes = self.check_nodes()
        for key in overloaded_nodes:
            self.split_node(key)

        for key in underloaded_nodes:
            self.remove_node(key)
        pass

    def get_node(self, key: str) -> Node:
        keyHash = stable_hash(key)
        return self.ring.floor_entry(keyHash).value

    def save(self, key, value):
        node = self.get_node(key)
        node.save(key, value)
        return True

    def get(self, key):
        node = self.get_node(key)
        return node.get(key)

    def has(self, key):
        node = self.get_node(key)
        return node.get(key)

    def remove(self, key):
        node = self.get_node(key)
        return node.remove(key)

    pass

if __name__ == '__main__':
    node1 = Node("A", "http://A-node", "oauth")
    node2 = Node("B", "http://B-node", "oauth")
    ring = HashRing()
    ring.add_node(node1)
    ring.add_node(node2)

    for i in range(20):
        ring.save("key-1", "some value")

        if ring.has("key-1"):
            print(ring.get("key-1"))
            ring.remove("key-1")

        ring.balance()

    for i in range(20):
        if ring.has("key-1"):
            print(ring.get("key-1"))
            ring.remove("key-1")






