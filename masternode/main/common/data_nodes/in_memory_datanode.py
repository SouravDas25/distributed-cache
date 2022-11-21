import pylru

from masternode.main.manager.utils.hashing import toCacheIndex
from masternode.main.common.interfaces.datanode import DataNode


class InMemoryDataNode(DataNode):

    def __init__(self, serverName: str, capacity: int):
        self.serverName = serverName
        self.__capacity = capacity
        cacheSize = int(capacity)
        self.cache = pylru.lrucache(cacheSize)
        self.keysToBeRemoved = set()

    def moveKeys(self, targetServer, low: int, high: int, size: int):
        for key in list(self.cache.keys()):
            index = toCacheIndex(key, size)
            if low < index <= high:
                targetServer.put(key, self.cache[key])
                # self.cache.pop(key)
                self.keysToBeRemoved.add(key)
        # print(removeKeys)

    def compact(self, capacity):
        cacheSize = int(capacity)
        newCache = pylru.lrucache(cacheSize)
        for key in list(self.cache.keys()):
            if key not in self.keysToBeRemoved:
                newCache[key] = self.cache[key]
        self.cache = newCache
        self.__capacity = capacity
        self.keysToBeRemoved.clear()

    def capacity(self):
        return self.__capacity

    def size(self):
        return self.cache.__len__()

    def load(self):
        return self.size() / self.capacity()

    def put(self, key, value):
        self.cache[key] = value

    def get(self, key):
        return self.cache[key]

    def has(self, key: str):
        return key in self.cache
