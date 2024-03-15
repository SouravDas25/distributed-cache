import asyncio
from time import sleep

from apscheduler.schedulers.background import BackgroundScheduler

from common.TreeMap import TreeDict
from common.interfaces.datanode import DataNode
from common.utils.hashing import toCacheIndex, stableHash
from datanodes.factory import DataNodeFactory
from autoscaler.autoscaler_factory import createAutoScaler
from datanodes.network_datanode import NetworkDataNode

MAX_SERVERS = 16
MAX_SERVER_LOAD = 0.7
SIZE_PER_NODE = 10


class ConsistentHashRing:

    def __del__(self):
        self.scheduler.shutdown()

    def __init__(self, max_size: int) -> None:
        self.max_size = max_size
        self.ring: TreeDict[DataNode] = TreeDict()
        self.is_job_running = False
        # for i in range(MAX_SERVERS):
        #     self.medianHeap.push(stableHash("data-server-" + str(i)))
        # self.medianHeap.push(0)
        # self.addServer("data-server-0")
        # self.ring[0] = NetworkDataNode("primary-node", 0, "")
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(lambda: self.job(), "interval", seconds=30)
        self.scheduler.start()
        self.autoscaler = createAutoScaler("CF", self)
        self.freeInstances = {}

        # self.addServer("data-server-1", "")

    def job(self):
        if not self.is_job_running:
            print("Job not running")
            self.is_job_running = True
            try:
                asyncio.run(self.balance())
            finally:
                self.is_job_running = False
        else:
            print("Job running")
        pass

    def addFreeInstance(self, instance_id: str, instance: DataNode) -> None:
        currentKey = stableHash(instance_id)
        if currentKey not in self.ring:
            self.freeInstances[instance_id] = instance
        else:
            self.ring[currentKey] = instance

    def getFreeInstance(self) -> DataNode:
        instance_id = list(self.freeInstances.keys())[0]
        return self.freeInstances.pop(instance_id)

    async def balance(self):
        print("RUNNING BALANCE")

        if self.ring.__len__() <= 0:
            if len(self.freeInstances.keys()) > 0:
                self.ring[0] = self.getFreeInstance()
            print("No Datanode found.")
            pass
        else:
            overloadedServers = []
            underloadedServers = []

            for serverKey in list(self.ring.key_set()):
                server = self.ring[serverKey]
                metrics = await server.metrics()
                size = metrics["size"]
                # if size <= 0:
                #     continue
                load = size / self.max_size
                print(f"{server.name()} metrics : {metrics}")
                print(f"{server.name()} load : {load}")
                if load > MAX_SERVER_LOAD:
                    overloadedServers.append(serverKey)
                elif load <= 0.1:
                    underloadedServers.append(serverKey)

            awaitServers = []

            for serverKey in overloadedServers:

                fromKey = serverKey
                if serverKey == self.ring.last_key():
                    toKey = self.ring.first_key()
                else:
                    toKey = self.ring.higher_key(serverKey)
                if self.freeInstances.__len__() > 0:
                    awaitServers.append(self.addServer(fromKey, toKey))
                else:
                    # self.autoscaler.upscale()
                    pass



            for serverKey in underloadedServers:
                if serverKey != self.ring.first_key():
                    awaitServers.append(self.removeServer(serverKey))

            await asyncio.gather(*awaitServers)

        print("COMPLETING BALANCE")
        pass

    async def addServer(self, fromKey, tokey):
        # check if server is active and ready yet

        behideServer = self.ring[fromKey]
        aheadServer = self.ring[tokey]

        if self.freeInstances.__len__() <= 0:
            return

        currentServer = self.getFreeInstance()

        currentKey = behideServer.calculateMidKey()
        serverName = currentServer.name()

        if behideServer == currentKey:
            return

        print("ADDING DATA-NODE ", serverName)

        # self.on_going_scaling_servers[serverName] = {
        #     "assignedKey": currentKey,
        #     "behindKey": fromKey,
        #     "toKey": tokey,
        #     "server": currentServer
        # }

        # currentServer.setName(serverName)

        # self.ring[currentKey] = currentServer
        try:
            response = await behideServer.moveKeys(currentServer, currentKey, tokey)
            if response["success"]:
                self.ring[currentKey] = currentServer
                await behideServer.compactKeys()
            else:
                self.addFreeInstance(currentServer.appId, currentServer)

        except:
            self.addFreeInstance(currentServer.appId, currentServer)

        pass

    async def removeServer(self, currentKey):
        behideKey = self.ring.lower_key(currentKey)
        if behideKey != self.ring.first_key():
            currentServer = self.ring[currentKey]
            response = await currentServer.moveKeys(self.ring[behideKey], currentKey, None)
            self.ring.remove(currentKey)
            await currentServer.compactKeys()
            self.addFreeInstance(currentServer.appId, currentServer)
            print("REMOVING DATA-NODE ", currentServer.serverName)
        pass

    def getServer(self, key: str) -> DataNode:
        key = stableHash(key)
        keyHash = self.ring.floor_key(key)
        return self.ring[keyHash]

    def clusterSize(self):
        return len(self.ring)

    def printClusterDistribution(self):
        print("Total Load: ", self.load())
        print(self.ring)
        for server in self.ring:
            print("Server :", self.ring[server].name(), list(self.ring[server].distributedCache.keys()))

    def load(self):
        return sum([self.ring[server].size() / self.max_size for server in self.ring]) / self.ring.__len__()

    def remove(self, key):
        print("Removing key : ", key)
        self.getServer(key).remove(key)
        # self.balance()

    def put(self, key, value):
        node = self.getServer(key)
        node.put(key, value)
        # self.balance()

    def get(self, key):
        return self.getServer(key).get(key)

    def has(self, key: str):
        return self.getServer(key).has(key)


if __name__ == "__main__":
    import random
    import string


    def strGen(N):
        return ''.join(random.sample(string.ascii_uppercase + string.digits, N))


    sc = ConsistentHashRing(SIZE_PER_NODE)

    # sc.balance()
    # print(sc.clusterSize(), sc.servePoints)
    # stream(list(sc.servers.values())) \
    #     .map(lambda s: list(s.cache.keys())) \
    #     .for_each(print)

    keys = []

    while True:

        for i in range(20):
            key = strGen(3)
            print("key ", key, toCacheIndex(key, SIZE_PER_NODE))
            sc.put(key, i)
            keys.append(key)

        for i in range(20):
            random.shuffle(keys)
            key = keys[0]
            if sc.has(key):
                sc.remove(key)

        # print(sc.clusterSize(), sc.servePoints)
        sc.printClusterDistribution()
        sleep(10)
