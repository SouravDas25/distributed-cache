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

    def __init__(self, ring_ds: TreeDict):
        self.free_nodes = SortedDict()
        self.ring_ds = ring_ds
        pass

    def size(self):
        return self.free_nodes.__len__()

    def add_free_node(self, node: DataNode) -> None:
        if self.ring_ds.has_node(node):
            self.ring_ds.update_node(node)
        else:
            LOGGER.info("Adding free node {} ", node)
            instance_no = node.instance_no()
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
        self.ring_ds: TreeDict[DataNode] = TreeDict()
        self.is_job_running = False
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(lambda: self.job(), "interval", seconds=10)
        self.scheduler.start()
        self.autoscaler = autoscaler
        self.fn_manager = FreeNodeManager(self.ring_ds)

    def job(self):
        if not self.is_job_running:
            # LOGGER.info("Job not running")
            self.is_job_running = True
            try:
                LOGGER.info("Running balance")
                asyncio.run(self.balance())
                LOGGER.info("Balance completed")
            finally:
                self.is_job_running = False
        else:
            LOGGER.info("Job running")
        pass

    def add_free_node(self, new_node: DataNode) -> None:
        self.fn_manager.add_free_node(new_node)

    def get_free_node(self) -> DataNode:
        return self.fn_manager.pop()

    async def down_scale(self):
        if self.fn_manager.size() > 1:
            LOGGER.info("Downscaling node")

            max_node_from_ring, ring_node_key = self.ring_ds.get_max_node()
            max_free_node = self.fn_manager.max_node()

            LOGGER.info("Max Node from ring {} ", max_node_from_ring)
            LOGGER.info("Max Node free {} ", max_free_node)

            free_node = self.get_free_node()

            if max_node_from_ring.instance_no() > max_free_node.instance_no():
                try:
                    LOGGER.info("Removing from ring {}, {} ", max_node_from_ring.name(), ring_node_key)
                    LOGGER.info(f"moving data from: {max_node_from_ring.name()} -> {free_node.name()}")
                    response = await max_node_from_ring.move_keys(free_node, ring_node_key, "")
                    if response["success"]:
                        self.ring_ds.put_node(ring_node_key, free_node)
                        await max_node_from_ring.compact_keys()
                        self.add_free_node(max_node_from_ring)
                    LOGGER.info("Removed from ring {} ", max_node_from_ring.name())
                except Exception as e:
                    self.add_free_node(free_node)
                    LOGGER.exception("REMOVE from ring Failed: {}, {} ", max_node_from_ring.name(), ring_node_key, e)
            else:
                if free_node != max_free_node:
                    self.fn_manager.remove(max_free_node.instance_no())
                    LOGGER.info("REMOVE Free Server: {} ", max_free_node.name())
                    self.autoscaler.downscale()
                else:
                    self.add_free_node(free_node)
                    LOGGER.info("free_node is max_free_node")
                pass
            pass
        pass

    async def balance(self):

        if self.ring_ds.__len__() <= 0:
            LOGGER.info("No Datanode found in ring.")
            if self.fn_manager.size() > 0:
                node = self.get_free_node()
                self.ring_ds.put_node(0, node)
                LOGGER.info("Adding first node to ring {}, {} ", 0, node.name())
            return

        overloaded_servers = SortedDict()
        underloaded_servers = SortedDict()

        for from_key in self.ring_ds.keys():
            node = self.ring_ds.get_node(from_key)
            metrics = await node.metrics(cache=False)
            load = metrics["size"] / self.max_size
            LOGGER.info(f"{node.name()} metrics : {metrics}")
            LOGGER.info(f"{node.name()} load : {load}")
            if load > MAX_SERVER_LOAD:
                overloaded_servers[load] = from_key
            elif load <= 0.1:
                underloaded_servers[load] = from_key

        new_node_req = len(overloaded_servers) - len(underloaded_servers)
        if new_node_req > 0:
            self.autoscaler.upscale()

        await_servers = []

        LOGGER.info("Overloaded servers : {} ", overloaded_servers)

        for load in reversed(overloaded_servers.keys()):
            # maximum loaded node first
            from_key = overloaded_servers[load]
            if from_key == self.ring_ds.last_key():
                to_key = self.ring_ds.first_key()
            else:
                to_key = self.ring_ds.higher_key(from_key)
            if self.fn_manager.size() > 0:
                await_servers.append(self.add_server(from_key, to_key))

        LOGGER.info("Underloaded servers : {} ", underloaded_servers)

        for load in list(underloaded_servers.keys()):
            # least loaded servers first
            from_key = underloaded_servers[load]
            if from_key != self.ring_ds.first_key():
                await_servers.append(self.remove_server(from_key))

        await_servers.reverse()

        await asyncio.gather(*await_servers)

        await self.down_scale()

        pass

    async def add_server(self, from_key, to_key):
        # check if server is active and ready yet

        behind_server = self.ring_ds.get_node(from_key)
        ahead_server = self.ring_ds.get_node(to_key)

        if self.fn_manager.size() <= 0:
            LOGGER.info("Free node not available!!")
            return

        free_node = self.get_free_node()
        is_available = await free_node.health_check()
        if not is_available:
            return

        node_key = await behind_server.calculate_mid_key()

        if from_key == node_key:
            return

        LOGGER.info("ADDING DATA-NODE for {} ", behind_server.name())
        try:
            LOGGER.info(f"moving data from: {behind_server.name()} -> {free_node.name()}")
            response = await behind_server.move_keys(free_node, node_key, to_key)
            if response["success"]:
                self.ring_ds.put_node(node_key, free_node)
                await behind_server.compact_keys()
            else:
                LOGGER.error(f"moving data failed for: {behind_server.name()} -> {server_name}")
                self.add_free_node(free_node)

            LOGGER.info("ADDING DATA-NODE completed for {} ", behind_server.name())

        except Exception as e:
            self.add_free_node(free_node)
            LOGGER.exception("Exception while adding node, ", e)

        pass

    async def remove_server(self, node_key):
        if node_key != self.ring_ds.first_key():
            behind_key = self.ring_ds.lower_key(node_key)
            node = self.ring_ds.get_node(node_key)
            response = await node.move_keys(self.ring_ds.get_node(behind_key), node_key, None)
            self.ring_ds.remove_node(node_key)
            await node.compact_keys()
            self.add_free_node(node)
            LOGGER.info("Removed DATA-NODE {} ", node.name())
        pass

    def resolve_node(self, key: str) -> DataNode:
        key_hash = stableHash(key)
        key_hash = self.ring_ds.floor_key(key_hash)
        if key_hash is None:
            key_hash = self.ring_ds.last_key()
        return self.ring_ds.get_node(key_hash)

    def load(self):
        return sum([self.ring_ds.get_node(node_key).size() / self.max_size for node_key in
                    self.ring_ds.keys()]) / self.ring_ds.__len__()

    def status(self):
        ring = []
        for node_key in self.ring_ds.keys():
            node: DataNode = self.ring_ds.get_node(node_key)
            ring.append({
                "node_key": node_key,
                "node_name": node.name(),
                "instance_id": node.instance_no(),
                "load": node.size() / self.max_size,
                "metric": node.cached_metrics()
            })
            pass

        free_nodes = []
        for node in self.fn_manager.free_nodes.values():
            free_nodes.append({
                "node_name": node.name(),
                "instance_id": node.instance_no()
            })

        return {
            "ring": ring,
            "free_nodes": free_nodes
        }
        pass
