import asyncio
from collections import OrderedDict
from time import sleep

from apscheduler.schedulers.background import BackgroundScheduler
from sortedcontainers import SortedDict

from masternode.main.autoscaler.autoscaler import Autoscaler
from masternode.main.common.TreeMap import TreeDict
from masternode.main.datanodes.datanode import DataNode
from masternode.main.common.utils.hashing import toCacheIndex, stableHash
from masternode.main.autoscaler.autoscaler_factory import create_autoscaler
from loguru import logger as LOGGER

MAX_SERVERS = 16
MAX_SERVER_LOAD = 0.7
SIZE_PER_NODE = 2


class FreeNodeManager:

    def __init__(self, ring):
        self.free_nodes = SortedDict()
        self.ring = ring
        pass

    def size(self):
        return self.free_nodes.__len__()

    def add_free_node(self, node: DataNode) -> None:
        instance_no = node.instance_no()
        if self.ring.has_node(node):
            self.ring.update_node(node)
        else:
            self.free_nodes[instance_no] = node

    def max_node(self) -> DataNode:
        instance_no = max(self.free_nodes.keys())
        return self.free_nodes.get(instance_no)

    def pop(self) -> DataNode:
        instance_no = min(self.free_nodes.keys())
        return self.free_nodes.pop(instance_no)

    def remove(self, instance_no: int) -> None:
        self.free_nodes.pop(instance_no)
        pass


class ConsistentHashRing:

    def __del__(self):
        self.scheduler.shutdown()

    def __init__(self, max_size: int, autoscaler: Autoscaler) -> None:
        self.max_size = max_size
        self.bst: TreeDict[DataNode] = TreeDict()
        self.is_job_running = False
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(lambda: self.job(), "interval", seconds=10)
        self.scheduler.start()
        self.autoscaler = autoscaler
        self.fn_manager = FreeNodeManager(self)

    def job(self):
        if not self.is_job_running:
            LOGGER.info("Job not running")
            self.is_job_running = True
            try:
                LOGGER.info("RUNNING BALANCE")
                asyncio.run(self.balance())
                LOGGER.info("COMPLETING BALANCE")
            finally:
                self.is_job_running = False
        else:
            LOGGER.info("Job running")
        pass

    def has_node(self, new_node: DataNode) -> bool:
        return self.get_node_key(new_node) is not None

    def get_node_key(self, new_node: DataNode) -> str | None:
        for node_key in list(self.bst.key_set()):
            node = self.bst.get(node_key)
            if node.name() == new_node.name():
                return node_key
        return None

    def update_node(self, new_node: DataNode):
        node_key = self.get_node_key(new_node)
        if node_key:
            node = self.bst.get(node_key)
            node.update_node(new_node)
        pass

    def add_free_node(self, new_node: DataNode) -> None:
        self.fn_manager.add_free_node(new_node)

    def get_free_node(self) -> DataNode:
        return self.fn_manager.pop()

    def get_max_node(self) -> (DataNode, str):
        instance_no = float("-inf")
        max_node = None
        max_node_key = None
        for node_key in list(self.bst.key_set()):
            node: DataNode = self.bst.get(node_key)
            if instance_no < node.instance_no():
                max_node = node
                max_node_key = node_key
                instance_no = node.instance_no()
        return max_node, max_node_key

    async def down_scale(self):
        if self.fn_manager.size() > 1:
            free_node = self.get_free_node()
            max_node_from_ring, ring_node_key = self.get_max_node()
            max_free_node = self.fn_manager.max_node()

            if max_node_from_ring.instance_no() > max_free_node.instance_no():
                try:
                    response = await max_node_from_ring.move_keys(free_node, ring_node_key, "")
                    if response["success"]:
                        self.bst[ring_node_key] = free_node
                    LOGGER.info("REMOVE from ring {}, {} ", max_node_from_ring.name(), ring_node_key)
                except Exception as e:
                    LOGGER.exception("REMOVE from ring Failed: {}, {} ", max_node_from_ring.name(), ring_node_key, e)
            else:
                if free_node != max_free_node:
                    self.fn_manager.remove(max_free_node.instance_no())
                    LOGGER.info("REMOVE Free Server: {} ", max_free_node.name())
                    self.autoscaler.downscale()
                pass
            pass
        pass

    async def balance(self):

        if self.bst.__len__() <= 0:
            if self.fn_manager.size() > 0:
                node = self.get_free_node()
                from_key = stableHash(node.name())
                self.bst.put(from_key, node)
                LOGGER.info("Adding first node to ring {}, {} ", from_key, node.name())
            LOGGER.info("No Datanode found.")
            return
            pass

        overloaded_servers = SortedDict()
        underloaded_servers = SortedDict()

        for from_key in list(self.bst.key_set()):
            node = self.bst.get(from_key)
            metrics = await node.metrics()
            load = metrics["size"] / self.max_size
            LOGGER.info(f"{node.name()} metrics : {metrics}")
            LOGGER.info(f"{node.name()} load : {load}")
            if load > MAX_SERVER_LOAD:
                overloaded_servers[load] = from_key
            elif load <= 0.1:
                underloaded_servers[load] = from_key

        await_servers = []

        for load in reversed(overloaded_servers.keys()):
            # maximum loaded node first
            from_key = overloaded_servers[load]
            if from_key == self.bst.last_key():
                to_key = self.bst.first_key()
            else:
                to_key = self.bst.higher_key(from_key)
            if self.fn_manager.size() > 0:
                await_servers.append(self.add_server(from_key, to_key))
            else:
                self.autoscaler.upscale()
                pass

        for load in list(underloaded_servers.keys()):
            # least loaded servers first
            from_key = underloaded_servers[load]
            if from_key != self.bst.first_key():
                await_servers.append(self.remove_server(from_key))

        await asyncio.gather(*await_servers)

        await self.down_scale()

        pass

    async def add_server(self, from_key, to_key):
        # check if server is active and ready yet

        behind_server = self.bst.get(from_key)
        ahead_server = self.bst.get(to_key)

        if self.fn_manager.size() <= 0:
            LOGGER.info("Free node available!!")
            return

        free_node = self.get_free_node()
        is_available = await free_node.health_check()
        if not is_available:
            return

        node_key = behind_server.calculate_mid_key()
        server_name = free_node.name()

        if from_key == node_key:
            return

        LOGGER.info("ADDING DATA-NODE for ", behind_server.name())
        try:
            LOGGER.info(f"moving data from: {behind_server.name()} -> {server_name}")
            response = await behind_server.move_keys(free_node, node_key, to_key)
            if response["success"]:
                self.bst[node_key] = free_node
                await behind_server.compact_keys()
            else:
                LOGGER.error(f"moving data failed for: {behind_server.name()} -> {server_name}")
                self.add_free_node(free_node)

        except Exception as e:
            self.add_free_node(free_node)
            LOGGER.exception("Exception while adding node, ", e)

        pass

    async def remove_server(self, currentKey):
        behideKey = self.bst.lower_key(currentKey)
        if currentKey != self.bst.first_key():
            currentServer = self.bst[currentKey]
            response = await currentServer.move_keys(self.bst[behideKey], currentKey, None)
            self.bst.remove(currentKey)
            await currentServer.compact_keys()
            self.add_free_node(currentServer.app_id, currentServer)
            LOGGER.info("REMOVING DATA-NODE ", currentServer.server_name)
        pass

    def get_node(self, key: str) -> DataNode:
        key = stableHash(key)
        keyHash = self.bst.floor_key(key)
        if keyHash is None:
            keyHash = self.bst.last_key()
        return self.bst[keyHash]

    def load(self):
        return sum([self.bst[server].size() / self.max_size for server in self.bst]) / self.bst.__len__()

    def status(self):
        ring = []
        for serverKey in list(self.bst.key_set()):
            server = self.bst[serverKey]
            ring.append({
                "node_key": serverKey,
                "node_name": server.name(),
                "instance_id": server.instance_id,
                "appId": server.app_id,
                "load": server.size() / self.max_size,
                "metric": server.cached_metric
            })
            pass

        free_nodes = []
        for server in self.freeInstances.values():
            free_nodes.append({
                "node_name": server.name(),
                "instance_id": server.instance_id,
                "appId": server.app_id
            })

        return {
            "ring": ring,
            "free_nodes": free_nodes
        }
        pass


if __name__ == "__main__":
    import random
    import string


    def strGen(N):
        return ''.join(random.sample(string.ascii_uppercase + string.digits, N))


    sc = ConsistentHashRing(SIZE_PER_NODE, "LOCAL")

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
