import asyncio
import unittest

from masternode.main.common.hashing import stable_hash, random_str
from masternode.main.datanodes.in_memory_datanode import InMemoryDataNode
from masternode.main.map import DistributedMap, ClusterConfig
from loguru import logger as LOGGER

from masternode.main.ring import ConsistentHashRing

from loguru import logger as LOGGER


class HashRingTest(unittest.TestCase):

    def setUp(self):
        self.ring = ConsistentHashRing()

    def test_put(self):
        for i in range(5):
            random_hash = stable_hash(random_str(5))
            node = InMemoryDataNode(f"datanode-{i}", i)
            self.ring.put_active_node(random_hash, node)

        for i in range(5):
            key = random_str(3)
            node = self.ring.resolve_node(key)
            LOGGER.info("key {} is resolved to node {}", key, node.name())
        # self.ring.floor_hash()
        pass
