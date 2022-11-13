import pylru

from hasing import toCacheIndex
from interfaces.datanode import DataNode


class InMemoryServer(DataNode):

    def __init__(self, serverName: str, serverUrl: str, capacity: int):
        self.serverName = serverName
        self.serverUrl = serverUrl
        self.__capacity = capacity
        self.cache = pylru.lrucache(capacity)
        self.keysToBeRemoved = set()

    def moveKeys(self, targetServer, low: int, high: int, size: int):
        for key in list(self.cache.keys()):
            index = toCacheIndex(key, size)
            if low <= index < high:
                targetServer.put(key, self.cache[key])
                # self.cache.pop(key)
                self.keysToBeRemoved.add(key)
        # print(removeKeys)

    def compact(self):
        for key in self.keysToBeRemoved:
            if key in self.cache:
                self.cache.pop(key)

    def capacity(self):
        return self.__capacity

    def size(self):
        return self.cache.__len__()

    def put(self, key, value):
        self.cache[key] = value

    def get(self, key):
        return self.cache[key]

    def has(self, key: str):
        return key in self.cache
