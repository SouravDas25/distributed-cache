import pylru

from common.utils.hashing import toCacheIndex, stableHash
from common.interfaces.datanode import DataNode


class InMemoryDataNode(DataNode):

    def __init__(self, serverName: str):
        self.serverName = serverName
        self.cache = {}
        self.keysToBeRemoved = set()

    def name(self):
        return self.serverName

    def moveKeys(self, targetServer, fromHash, toHash):
        for key in list(self.cache.keys()):
            keyHash = stableHash(key)
            if fromHash <= keyHash:
                targetServer.put(key, self.cache[key])
                self.cache.pop(key)
                self.keysToBeRemoved.add(key)
        # print(removeKeys)

    def compact(self):
        newCache = {}
        for key in list(self.cache.keys()):
            if key not in self.keysToBeRemoved:
                newCache[key] = self.cache[key]
        self.cache = newCache
        self.keysToBeRemoved.clear()

    def size(self):
        return self.cache.__len__()

    def put(self, key, value):
        self.cache[key] = value

    def get(self, key):
        return self.cache[key]

    def has(self, key: str):
        return key in self.cache
