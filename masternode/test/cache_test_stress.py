import asyncio
import string
import unittest
import random
from time import sleep

from masternode.main.cache import DistributedCache, CacheConfig
from masternode.main.common.utils.hashing import random_str
from loguru import logger as LOGGER


class DistributedCacheStressTest(unittest.TestCase):

    def setUp(self):
        config = CacheConfig()
        config.cache_size = 10
        config.autoscaler_type = "LOCAL"
        self.cache = DistributedCache(config)
        self.cache.autoscaler.upscale()
        self.run_balance()

    def run_balance(self):
        LOGGER.info("Running Balance")
        asyncio.run(self.cache.ring.balance())
        LOGGER.info("Balance completed ")

    def test_scale_up_balance(self):
        for i in range(100):
            key = "apple-" + str(i)
            self.cache.put(key, i)
        load = self.cache.load()
        while load >= 1:
            LOGGER.info("Current load: {}", load)
            self.run_balance()
            new_load = self.cache.load()
            self.assertLess(new_load, load)
            load = new_load
            LOGGER.info("New load: {}", load)
            sleep(1)

    def test_scale_down_balance(self):
        self.test_scale_up_balance()
        for i in range(99):
            key = "apple-" + str(i)
            self.cache.remove(key)
        no_of_nodes = self.cache.ring.ring_ds.__len__()
        while no_of_nodes > 1:
            LOGGER.info("Current nodes: {}", no_of_nodes)
            self.run_balance()
            new_nodes = self.cache.ring.ring_ds.__len__()
            self.assertGreater(no_of_nodes, new_nodes)
            no_of_nodes = new_nodes
            LOGGER.info("New nodes: {}", new_nodes)
            sleep(1)

        pass
