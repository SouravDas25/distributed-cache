from time import sleep
from typing import Dict

from common.interfaces.datanode import DataNode
from common.utils.hashing import toCacheIndex
from common.utils.utils import CommonUtil
from masternode.main.manager.data_nodes.factory import DataNodeFactory



class DataNodeCluster:

    def __init__(self, cacheSize):
        self.cacheSize = cacheSize
        self.servers: Dict[DataNode] = {}
        self.servePoints = []
        self.addServer("data-server-0", None, 0, cacheSize - 1)
        # self.scheduler = BackgroundScheduler()
        # self.scheduler.add_job(lambda: self.balance(), "interval", minutes=1)
        # self.scheduler.start()
        # self.addServer("data-server-1", "")

    def balance(self):
        print("RUNNING BALANCE")
        balanceREQ = []
        low = 0
        for serverIndex, serverId in self.servePoints:
            server = self.servers[serverId]
            capacity = server.capacity()
            load = server.load()
            slotPercentage = capacity / self.cacheSize
            print("{0}: load: {1}, datanode-capacity: {2}, slot-capacity: {3}".format(
                serverId, load, capacity, slotPercentage))
            if load > 0.3 and slotPercentage > 0.3:
                balanceREQ.append((serverId, low, serverIndex))
            low = serverIndex

        for serverId, low, high in balanceREQ:
            serverName = "data-server-" + str(len(self.servers))
            newHigh = low + (high - low) // 2
            # auto-scaling
            self.addServer(serverName, serverId, low, newHigh)
        print("COMPLETING BALANCE")
        pass

    def addServer(self, newServerId, overloadedServerId, low, high):
        print("ADDING DATA-NODE ", newServerId, low, high)
        newServerCapacity = high - low
        if low == 0:
            newServerCapacity += 1
        self.servers[newServerId] = DataNodeFactory.instance().getInMemoryDataNode(newServerId, newServerCapacity)
        if overloadedServerId is not None:
            sourceServer: DataNode = self.servers[overloadedServerId]
            sourceServer.moveKeys(self.servers[newServerId], low, high, self.cacheSize)
        self.servePoints.append((high, newServerId))
        self.servePoints.sort()
        if overloadedServerId is not None:
            sourceServer: DataNode = self.servers[overloadedServerId]
            sourceServer.compact(sourceServer.capacity() // 2)

    def removeServer(self, serverName):
        self.servers.pop(serverName)
        serverIndex = CommonUtil.findIndex(self.servePoints, lambda x: x[1] == serverName)
        if serverIndex is not None:
            del self.servePoints[serverIndex]

    def getServer(self, key: str) -> DataNode:
        serverName = self.resolveServer(key)
        return self.servers[serverName]

    def clusterSize(self):
        return len(self.servers)

    def resolveServer(self, key):
        keyIndex = toCacheIndex(key, self.cacheSize)
        for serverIndex, serverId in self.servePoints:
            if keyIndex <= serverIndex:
                return serverId
        return self.servePoints[0][1]

    def printClusterDistribution(self):
        print(self.servePoints)
        for server in self.servers:
            print("Server :", server, self.servers[server].capacity(), list(self.servers[server].cache.keys()))

    def computeTotalLoad(self):
        totalSize = sum([self.servers[server].size() for server in self.servers])
        return totalSize / self.cacheSize

    def put(self, key, value):
        dataNode = self.getServer(key)
        if dataNode.load() > 0.25:
            self.balance()
        dataNode.put(key, value)

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

    sc = DataNodeCluster(CACHE_SIZE)

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
