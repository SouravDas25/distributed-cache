import json
from collections import OrderedDict

from masternode.main.common.hashing import stable_hash


# Node is a cache instance, were all the key -> value data are present
# instance_no: uniquely identify a node in a node set
# cache_size: total data that can be present in the node in bytes
class Node:

    def __init__(self, instance_no: int, cache_size: int):
        self.instance_no = instance_no
        self.data = OrderedDict()  # LRU cache
        self.cache_size = cache_size  # in bytes
        self.keys_to_remove = set()
        self.data_size = 0

    def __str__(self):
        return f"node-{self.instance_no}"

    def __repr__(self):
        return f"node-{self.instance_no}"

    # moved keys from one node to another
    def copy_keys(self, target_node, from_key, to_key):
        for key in list(self.data.keys()):
            key_hash = stable_hash(key)
            if from_key <= key_hash:
                target_node.put(key, self.data[key])
                self.keys_to_remove.add(key)
        return True

    # reduce memory usage if possible
    def compact_keys(self):
        for key in list(self.data.keys()):
            if key in self.keys_to_remove:
                self.remove(key)
        self.keys_to_remove.clear()

    # calculate amount of used cache size
    def load(self):
        return self.data_size / self.cache_size

    # computes memory usage of current node
    def metrics(self):
        # used_memory = psutil.virtual_memory().used
        # data_size is the current amount of data is present in node in bytes
        # load is present of cache size used
        return {
            "used_memory": self.data_size,
            "load": self.load()
        }

    def put(self, key, value):
        self.data[key] = value
        # move latest used data to end, LRU cache
        self.data.move_to_end(key)
        self.data_size += len(json.dumps(self.data[key])) + len(key)
        if self.load() >= 1.0:
            # remove Least Recently Used key, as total spaced is use up
            key, value = self.data.popitem(last=False)
            self.data_size -= len(json.dumps(value)) + len(key)

    def get(self, key):
        # move latest used data to end, LRU cache
        self.data.move_to_end(key)
        return self.data[key]

    def has(self, key: str):
        return key in self.data

    def remove(self, key: str):
        value = self.data.pop(key)
        self.data_size -= len(json.dumps(value)) + len(key)
        return value

    # calculate median of all keys present
    def calculate_mid_key(self):
        if len(self.data) <= 0:
            return 0
        return sum(map(lambda x: stable_hash(x), self.data.keys())) // self.data.__len__()
