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
        self.ring_ds.add_free_node(new_node)

    async def down_scale(self):
        if self.ring_ds.no_of_free_nodes() > 1:
            LOGGER.info("Downscaling node")

            max_node_from_ring, ring_node_key = self.ring_ds.get_node_with_max_instance_no()
            max_free_node = self.ring_ds.get_free_node_with_max_instance_no()

            LOGGER.info("Max Node from ring {} ", max_node_from_ring)
            LOGGER.info("Max Node free {} ", max_free_node)

            free_node = self.ring_ds.pop_free_node_with_min_instance_no()

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
                    self.ring_ds.remove_free_node(max_free_node.instance_no())
                    LOGGER.info("REMOVE Free Server: {} ", max_free_node.name())
                    self.autoscaler.downscale()
                else:
                    self.add_free_node(free_node)
                    LOGGER.info("free_node is max_free_node")
                pass
            pass
        pass

    async def get_active_node_status(self):
        overloaded_map = SortedDict()
        underloaded_map = SortedDict()

        for from_key in self.ring_ds.all_hashes():
            node = self.ring_ds.get_node(from_key)
            metrics = await node.metrics(cache=False)
            load = metrics["size"] / self.max_size
            LOGGER.info(f"{node.name()} metrics : {metrics}")
            LOGGER.info(f"{node.name()} load : {load}")

            if load not in overloaded_map:
                overloaded_map[load] = []

            if load not in underloaded_map:
                underloaded_map[load] = []

            if load > MAX_SERVER_LOAD:
                overloaded_map[load].append(from_key)
            if load <= 0.1:
                underloaded_map[load].append(from_key)

        overloaded_nodes = []
        underloaded_nodes = []

        for load in overloaded_map:
            overloaded_nodes.extend(overloaded_map[load])

        for load in underloaded_map:
            underloaded_nodes.extend(underloaded_map[load])

        return overloaded_nodes, underloaded_nodes

    async def balance(self):

        if self.ring_ds.__len__() <= 0:
            LOGGER.info("No Datanode found in ring.")
            if self.ring_ds.no_of_free_nodes() > 0:
                node = self.ring_ds.pop_free_node_with_min_instance_no()
                self.ring_ds.put_node(0, node)
                LOGGER.info("Adding first node to ring {}, {} ", 0, node.name())
            return

        overloaded_nodes, underloaded_nodes = await self.get_active_node_status()

        new_node_req = len(overloaded_nodes) - len(underloaded_nodes)
        if new_node_req > 0:
            self.autoscaler.upscale()

        awaits = []

        LOGGER.info("Underloaded servers : {} ", underloaded_nodes)
        # least loaded servers first
        for node_hash in underloaded_nodes:
            if node_hash != self.ring_ds.first_hash():
                awaits.append(self.remove_server(node_hash))
            pass

        await asyncio.gather(*awaits)
        awaits.clear()

        LOGGER.info("Overloaded servers : {} ", overloaded_nodes)
        # maximum loaded node first
        for node_hash in reversed(overloaded_nodes):
            if node_hash == self.ring_ds.last_hash():
                to_key = self.ring_ds.first_hash()
            else:
                to_key = self.ring_ds.higher_hash(node_hash)
            if self.ring_ds.no_of_free_nodes() > 0:
                awaits.append(self.add_server(node_hash, to_key))

        await asyncio.gather(*awaits)

        await self.down_scale()

        pass

    async def add_server(self, from_key, to_key):
        # check if server is active and ready yet

        behind_server = self.ring_ds.get_node(from_key)
        ahead_server = self.ring_ds.get_node(to_key)

        if self.ring_ds.no_of_free_nodes() <= 0:
            LOGGER.info("Free node not available to scale {}", behind_server)
            return

        free_node = self.ring_ds.pop_free_node_with_min_instance_no()
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
                LOGGER.error(f"moving data failed for: {behind_server.name()} -> {free_node.name()}")
                self.add_free_node(free_node)

            LOGGER.info("ADDING DATA-NODE completed for {} ", behind_server.name())

        except Exception as e:
            self.add_free_node(free_node)
            LOGGER.exception("Exception while adding node, ", e)

        pass

    async def remove_server(self, node_key):
        if node_key != self.ring_ds.first_hash():
            behind_key = self.ring_ds.lower_hash(node_key)
            node = self.ring_ds.get_node(node_key)
            response = await node.move_keys(self.ring_ds.get_node(behind_key), node_key, None)
            self.ring_ds.remove_node(node_key)
            await node.compact_keys()
            self.add_free_node(node)
            LOGGER.info("Removed DATA-NODE {} ", node.name())
        pass

    def resolve_node(self, key: str) -> DataNode:
        key_hash = stableHash(key)
        key_hash = self.ring_ds.floor_hash(key_hash)
        if key_hash is None:
            key_hash = self.ring_ds.last_hash()
        return self.ring_ds.get_node(key_hash)

    def load(self):
        return sum([self.ring_ds.get_node(node_key).size() / self.max_size for node_key in
                    self.ring_ds.all_hashes()]) / self.ring_ds.__len__()

    def status(self):
        ring = []
        for node_key in self.ring_ds.all_hashes():
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
        for node in self.ring_ds.free_nodes.values():
            free_nodes.append({
                "node_name": node.name(),
                "instance_id": node.instance_no()
            })

        return {
            "ring": ring,
            "free_nodes": free_nodes
        }
        pass
