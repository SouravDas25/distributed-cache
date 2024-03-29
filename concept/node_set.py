from concept.node import Node


# create a nodeset,
# replication factor: no of node the data is replicated across
# instance no : unique id of node set
# cache size: maximum size of each node in bytes
class NodeSet:

    def __init__(self, replication_factor: int, instance_no: int, cache_size: int):
        self.replication_factor = replication_factor
        self.instance_no = instance_no
        self.cache_size = cache_size
        self.nodes = []
        for i in range(replication_factor):
            ino = instance_no * replication_factor + i
            self.nodes.append(Node(ino, cache_size))
        pass

    def __str__(self):
        return f"NodeSet-{self.instance_no}: {self.nodes}"

    def __repr__(self):
        return f"NodeSet-{self.instance_no}: {self.nodes}"

    # get specific replicate from the set
    def get_replica(self, index):
        return self.nodes[index]

    # copy data from other node set to other node set
    def copy_keys(self, target_node_set: "NodeSet", from_hash: int, to_hash: int):
        # probably this should run in parallel
        for i in range(self.replication_factor):
            # get other node replica
            target_node = target_node_set.get_replica(i)
            # copy self replica to target replica
            self.nodes[i].copy_keys(target_node, from_hash, to_hash)
        return True

    # reduce memory usage if possible
    def compact_keys(self):
        # can be done in parallel O(1)
        for node in self.nodes:
            node.compact_keys()
        pass

    def load(self):
        return self.nodes[0].load()

    # computes memory usage of current node
    def metrics(self):
        # used_memory = psutil.virtual_memory().used
        return {
            "used_memory": self.nodes[0].data_size,
            "load": self.load()
        }

    # put the same key in all replicas
    def put(self, key: str, value):
        # should be done in parallel O(1)
        for node in self.nodes:
            node.put(key, value)
        pass

    # get the value from any of the replicas
    def get(self, key: str):
        # should be done in parallel O(1)
        for node in self.nodes:
            if node.has(key):
                return node.get(key)
        return None

    def has(self, key: str):
        # should be done in parallel O(1)
        for node in self.nodes:
            if node.has(key):
                return True
        return False

    def remove(self, key: str):
        # should be done in parallel O(1)
        for node in self.nodes:
            return node.remove(key)
        return None

    # calculate median of all keys present
    def calculate_mid_key(self):
        return self.nodes[0].calculate_mid_key()
