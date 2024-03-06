import hashlib
from time import sleep
from typing import Dict

from common.MedianHeap import MedianHeap
from common.TreeMap import TreeDict
from common.interfaces.datanode import DataNode
from common.utils.hashing import toCacheIndex, stableHash
from common.utils.utils import CommonUtil
from masternode.main.manager.data_nodes.factory import DataNodeFactory

from sortedcontainers import SortedDict

MAX_SERVERS = 16


class ConsistentHashRing:

    def __init__(self, max_size: int) -> None:
        self.max_size = max_size
        self.ring: TreeDict[DataNode] = TreeDict()
        self.medianHeap = MedianHeap()
        for i in range(MAX_SERVERS):
            self.medianHeap.push(stableHash("data-server-" + str(i)))
        self.medianHeap.push(0)
        # self.addServer("data-server-0")
        self.ring[0] = DataNodeFactory.instance().getInMemoryDataNode("data-server-0")
        # self.scheduler = BackgroundScheduler()
        # self.scheduler.add_job(lambda: self.balance(), "interval", minutes=1)
        # self.scheduler.start()
        # self.addServer("data-server-1", "")

    def balance(self):
        print("RUNNING BALANCE")
        balanceReq = []

        for serverKey in list(self.ring.key_set()):
            server = self.ring[serverKey]
            size = server.size()
            # if size <= 0:
            #     continue
            load = size / self.max_size
            print("{0}: load: {1}, datanode-size: {2}".format(server.name(), load, size))
            if load > 0.3:
                balanceReq.append(serverKey)


        for serverKey in balanceReq:
            if serverKey == 0:
                fromHash = serverKey
                toHash = self.ring.last_key()
            else:
                fromHash = self.ring.lower_key(serverKey)
                toHash = serverKey
            self.addServer(fromHash, toHash)

        print("COMPLETING BALANCE")
        pass

    def addServer(self, fromHash, toHash):
        serverName = "data-server-" + str(len(self.ring))
        print("ADDING DATA-NODE ", serverName)

        currentKey = self.medianHeap.getMidKey(fromHash, toHash)
        if currentKey is None:
            print("NO Current Key Found, end of fractal")
            return

        if currentKey in self.ring:
            return

        behideKey = self.ring.floor_key(currentKey)
        aheadKey = self.ring.ceiling_key(currentKey)

        self.ring[currentKey] = DataNodeFactory.instance().getInMemoryDataNode(serverName)

        if behideKey != currentKey:
            behind = self.ring[behideKey]
            current = self.ring[currentKey]
            behind.moveKeys(current, currentKey, aheadKey)
            # behind.compact()

    def removeServer(self, serverName):
        self.ring.pop(serverName)
        serverIndex = CommonUtil.findIndex(self.servePoints, lambda x: x[1] == serverName)
        if serverIndex is not None:
            del self.servePoints[serverIndex]

    def getServer(self, key: str) -> DataNode:
        key = stableHash(key)
        keyHash = self.ring.floor_key(key)
        return self.ring[keyHash]

    def clusterSize(self):
        return len(self.ring)

    def resolveServer(self, key):
        keyIndex = toCacheIndex(key, self.cacheSize)
        for serverIndex, serverId in self.servePoints:
            if keyIndex <= serverIndex:
                return serverId
        return self.servePoints[0][1]

    def printClusterDistribution(self):
        print(self.ring)
        for server in self.ring:
            print("Server :", self.ring[server].name(), list(self.ring[server].cache.keys()))

    def computeTotalLoad(self):
        totalSize = sum([self.ring[server].size() for server in self.ring])
        return totalSize / self.cacheSize

    def put(self, key, value):
        self.getServer(key).put(key, value)
        self.balance()

    def get(self, key):
        return self.getServer(key).get(key)

    def has(self, key: str):
        return self.getServer(key).has(key)


if __name__ == "__main__":
    import random
    import string


    def strGen(N):
        return ''.join(random.sample(string.ascii_uppercase + string.digits, N))


    CACHE_SIZE = 8

    sc = ConsistentHashRing(CACHE_SIZE)

    # sc.balance()
    # print(sc.clusterSize(), sc.servePoints)
    # stream(list(sc.servers.values())) \
    #     .map(lambda s: list(s.cache.keys())) \
    #     .for_each(print)

    while True:

        for i in range(20):
            key = strGen(3)
            print("key ", key, toCacheIndex(key, CACHE_SIZE))
            sc.put(key, i)

        # print(sc.clusterSize(), sc.servePoints)
        sc.printClusterDistribution()
        sleep(10)
