from masternode.main.common.hashing import stable_hash
from masternode.main.datanodes.datanode import DataNode


class InMemoryDataNode(DataNode):

    def __init__(self, server_name: str, instance_no: int):
        self.server_name = server_name
        self.cache = {}
        self.keys_to_remove = set()
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

    async def move_keys(self, target_node, from_key, to_key):
        for key in list(self.cache.keys()):
            key_hash = stable_hash(key)
            if from_key <= key_hash:
                target_node.put(key, self.cache[key])
                # self.cache.pop(key)
                self.keys_to_remove.add(key)
        # print(removeKeys)
        return {
            "success": True
        }

    async def compact_keys(self):
        for key in list(self.cache.keys()):
            if key in self.keys_to_remove:
                self.cache.pop(key)
        self.keys_to_remove.clear()

    async def metrics(self, cache=True):
        return {
            "size": self.cache.__len__()
        }

    def cached_metrics(self):
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

    async def calculate_mid_key(self):
        return sum(map(lambda x: stable_hash(x), self.cache.keys())) // self.cache.__len__()
