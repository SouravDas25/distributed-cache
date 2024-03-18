from masternode.main.common.utils.hashing import stableHash
from masternode.main.datanodes.datanode import DataNode


class InMemoryDataNode(DataNode):

    def __init__(self, server_name: str, instance_no: int):
        self.server_name = server_name
        self.cache = {}
        self.keysToBeRemoved = set()
        self.ins_no = instance_no

    def update_node(self, node):
        pass

    async def health_check(self) -> bool:
        return True

    def instance_no(self) -> int:
        return self.ins_no

    def size(self, cache=True):
        return len(self.cache)

    def name(self):
        return self.server_name

    def move_keys(self, targetServer, fromHash, toHash):
        for key in list(self.cache.keys()):
            keyHash = stableHash(key)
            if fromHash <= keyHash:
                targetServer.put(key, self.cache[key])
                self.cache.pop(key)
                self.keysToBeRemoved.add(key)
        # print(removeKeys)

    def compact_keys(self):
        for key in list(self.cache.keys()):
            if key not in self.keysToBeRemoved:
                self.cache.pop(key)
        self.keysToBeRemoved.clear()

    def metrics(self):
        return {
            "size": self.cache.__len__()
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
        return sum(map(lambda x: stableHash(x), self.cache.keys())) // self.cache.__len__()

    def copyKeys(self, targetServer, fromKey, toKey):
        for key in list(self.cache.keys()):
            keyHash = stableHash(key)
            if fromKey <= keyHash:
                targetServer.put(key, self.cache[key])
                # self.cache.pop(key)
        pass
