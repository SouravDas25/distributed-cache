import asyncio

from apscheduler.schedulers.background import BackgroundScheduler
from sortedcontainers import SortedDict

from masternode.main.autoscaler.autoscaler import Autoscaler
from masternode.main.ring import ConsistentHashRing
from masternode.main.datanodes.datanode import DataNode
from masternode.main.common.hashing import stable_hash
from loguru import logger as LOGGER

MAX_SERVERS = 16
MAX_SERVER_LOAD = 0.7
SIZE_PER_NODE = 2


class RingLoadBalancer:

    def __del__(self):
        self.scheduler.shutdown()

    def __init__(self, max_size: int, ring: ConsistentHashRing[DataNode], autoscaler: Autoscaler) -> None:
        self.max_size = max_size
        self.ring = ring
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

    async def down_scale(self):
        if self.ring.no_of_free_nodes() > 1:
            LOGGER.info("Downscaling node")

            max_node_from_ring, ring_node_key = self.ring.get_active_node_with_max_instance_no()
            max_free_node = self.ring.get_free_node_with_max_instance_no()

            LOGGER.info("Max Node from ring {} ", max_node_from_ring)
            LOGGER.info("Max Node free {} ", max_free_node)

            if max_node_from_ring.instance_no() > max_free_node.instance_no():
                free_node = self.ring.pop_free_node_with_min_instance_no()
                try:
                    LOGGER.info("Removing from ring {}, {} ", max_node_from_ring.name(), ring_node_key)
                    LOGGER.info(f"moving data from: {max_node_from_ring.name()} -> {free_node.name()}")

                    response = await max_node_from_ring.move_keys(free_node, ring_node_key, "")
                    if response["success"]:
                        self.ring.put_active_node(ring_node_key, free_node)
                        await max_node_from_ring.compact_keys()
                        self.ring.free_up_node(max_node_from_ring)
                    LOGGER.info("Removed from ring {} ", max_node_from_ring.name())
                except Exception as e:
                    self.ring.free_up_node(free_node)
                    LOGGER.exception("REMOVE from ring Failed: {}, {} ", max_node_from_ring.name(), ring_node_key, e)
            else:
                self.ring.remove_blocked_or_free_node(max_free_node.instance_no())
                LOGGER.info("REMOVE Free Server: {} ", max_free_node.name())
                self.autoscaler.downscale()
            pass
        pass

    async def get_active_node_status(self):
        overloaded_map = SortedDict()
        underloaded_map = SortedDict()

        for from_key in self.ring.all_active_node_hashes():
            node = self.ring.get_active_node(from_key)
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

        if self.ring.no_of_active_nodes() <= 0:
            LOGGER.info("No Datanode found in ring.")
            if self.ring.no_of_free_nodes() > 0:
                node = self.ring.pop_free_node_with_min_instance_no()
                self.ring.put_active_node(0, node)
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
            if node_hash != self.ring.first_hash():
                awaits.append(self.merge_node(node_hash))
            pass

        await asyncio.gather(*awaits)
        awaits.clear()

        LOGGER.info("Overloaded servers : {} ", overloaded_nodes)
        # maximum loaded node first
        for node_hash in reversed(overloaded_nodes):
            if node_hash == self.ring.last_hash():
                to_hash = self.ring.first_hash()
            else:
                to_hash = self.ring.higher_hash(node_hash)
            if self.ring.no_of_free_nodes() > 0:
                awaits.append(self.split_node(node_hash, to_hash))

        await asyncio.gather(*awaits)

        await self.down_scale()

        pass

    async def split_node(self, from_hash, to_hash):
        # check if server is active and ready yet

        behind_node = self.ring.get_active_node(from_hash)
        ahead_node = self.ring.get_active_node(to_hash)

        if self.ring.no_of_free_nodes() <= 0:
            LOGGER.info("Free node not available to scale {}", behind_node)
            return

        free_node = self.ring.pop_free_node_with_min_instance_no()

        try:

            is_available = await free_node.health_check()
            if not is_available:
                self.ring.remove_blocked_or_free_node(free_node.instance_no())
                return

            mid_hash = await behind_node.calculate_mid_key()

            if from_hash == mid_hash:
                return

            LOGGER.info("Splitting node for {} ", behind_node.name())

            LOGGER.info(f"moving data from: {behind_node.name()} -> {free_node.name()}")
            response = await behind_node.move_keys(free_node, mid_hash, to_hash)
            if response["success"]:
                self.ring.put_active_node(mid_hash, free_node)
                await behind_node.compact_keys()
            else:
                LOGGER.error(f"moving data failed for: {behind_node.name()} -> {free_node.name()}")
                self.ring.free_up_node(free_node)

            LOGGER.info("Splitting node completed for {} ", behind_node.name())

        except Exception as e:
            self.ring.free_up_node(free_node)
            LOGGER.exception("Exception while adding node, ", e)

        pass

    async def merge_node(self, node_hash):
        if node_hash == self.ring.first_hash():
            return

        behind_hash = self.ring.lower_hash(node_hash)
        behind_node = self.ring.get_active_node(behind_hash)
        node = self.ring.get_active_node(node_hash)
        LOGGER.info("Merging started {} ", node.name())

        # move all data to previous node
        response = await node.move_keys(behind_node, node_hash, None)

        self.ring.free_up_node(node)
        await node.compact_keys()

        LOGGER.info("Merging node {} ", node.name())
        pass

    def resolve_node(self, key: str) -> DataNode:
        key_hash = stable_hash(key)
        key_hash = self.ring.floor_hash(key_hash)
        if key_hash is None:
            key_hash = self.ring.last_hash()
        return self.ring.get_active_node(key_hash)

    def load(self):
        return sum([self.ring.get_active_node(node_hash).size() / self.max_size for node_hash in
                    self.ring.all_active_node_hashes()]) / self.ring.no_of_active_nodes()

    def status(self):
        ring = []
        for node_hash in self.ring.all_active_node_hashes():
            node: DataNode = self.ring.get_active_node(node_hash)
            ring.append({
                "node_hash": node_hash,
                "node_name": node.name(),
                "instance_id": node.instance_no(),
                "load": node.size() / self.max_size,
                "metric": node.cached_metrics()
            })
            pass

        free_nodes = []
        for node in self.ring.free_nodes().values():
            free_nodes.append({
                "node_name": node.name(),
                "instance_id": node.instance_no()
            })

        return {
            "ring": ring,
            "free_nodes": free_nodes
        }
        pass
