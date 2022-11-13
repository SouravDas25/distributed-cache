from time import sleep
from typing import Dict

from apscheduler.schedulers.background import BackgroundScheduler
from lazy_streams import stream

from hasing import toCacheIndex
from utils import CommonUtil

from inmemoryserver import InMemoryServer


class ServerCluster:

    def __init__(self, cacheSize):
        self.cacheSize = cacheSize
        self.servers: Dict[InMemoryServer] = {}
        self.servePoints = []
        self.addServer("data-server-0", "", None, 0, cacheSize)
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(lambda: self.balance(), "interval", minutes=1)
        self.scheduler.start()
        # self.addServer("data-server-1", "")

    def balance(self):
        print("RUNNING BALANCE")
        balanceREQ = []
        low = 0
        for serverIndex, serverId in self.servePoints:
            server = self.servers[serverId]
            capacity = serverIndex - low
            load = server.size() / capacity
            capacityPercent = capacity / self.cacheSize
            print(serverId, " : ", load, capacity, capacityPercent)
            if load > 0.5 and capacityPercent > 0.15:
                balanceREQ.append((serverId, low, serverIndex))
            low = serverIndex

        for serverId, low, high in balanceREQ:
            serverName = "data-server-" + str(len(self.servers))
            serverUrl = ""
            newHigh = low + (high - low) // 2
            # auto-scaling
            self.addServer(serverName, serverUrl, serverId, low, newHigh)
        print("COMPLETING BALANCE")
        pass

    def addServer(self, newServerId, serverUrl, overloadedServerId, low, high):
        self.servers[newServerId] = InMemoryServer(newServerId, serverUrl, high - low + 1)
        if overloadedServerId is not None:
            sourceServer: InMemoryServer = self.servers[overloadedServerId]
            sourceServer.moveKeys(self.servers[newServerId], low, high, self.cacheSize)
        self.servePoints.append((high, newServerId))
        self.servePoints.sort()
        if overloadedServerId is not None:
            sourceServer: InMemoryServer = self.servers[overloadedServerId]
            sourceServer.compact()

    def removeServer(self, serverName):
        self.servers.pop(serverName)
        serverIndex = CommonUtil.findIndex(self.servePoints, lambda x: x[1] == serverName)
        if serverIndex is not None:
            del self.servePoints[serverIndex]

    def getServer(self, key) -> InMemoryServer:
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


if __name__ == "__main__":
    import random
    import string


    def strGen(N):
        return ''.join(random.sample(string.ascii_uppercase + string.digits, N))


    sc = ServerCluster(50)
    for i in range(200):
        key = strGen(3)
        print("key ", key, toCacheIndex(key, 10))
        sc.getServer(key).put(key, i)

    stream(list(sc.servers.values())) \
        .map(lambda s: list(s.cache.keys())) \
        .for_each(print)

    # sc.balance()
    # print(sc.clusterSize(), sc.servePoints)
    # stream(list(sc.servers.values())) \
    #     .map(lambda s: list(s.cache.keys())) \
    #     .for_each(print)

    while True:
        print(sc.clusterSize(), sc.servePoints)
        stream(list(sc.servers.values())) \
            .map(lambda s: list(s.cache.keys())) \
            .for_each(print)
        sleep(30)
