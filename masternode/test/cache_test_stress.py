import asyncio
import unittest
import random

from masternode.main.cache import DistributedCache, CacheConfig
from masternode.main.common.hashing import random_str
from loguru import logger as LOGGER


class DistributedCacheStressTest(unittest.TestCase):

    def setUp(self):
        config = CacheConfig()
        config.cache_size = 10
        config.autoscaler_type = "LOCAL"
        self.cache = DistributedCache(config)
        self.cache.autoscaler.upscale()
        self.run_balance()
        self.saved_keys = set()

    def run_balance(self):
        LOGGER.info("Running Balance")
        asyncio.run(self.cache.load_balancer.balance())
        LOGGER.info("Balance completed ")

    def test_scale_up_balance(self):
        for i in range(100):
            key = random_str(3)
            self.cache.put(key, i)
            self.saved_keys.add(key)

        load = self.cache.load()
        while load >= 1:
            LOGGER.info("Current load: {}", load)
            self.run_balance()
            new_load = self.cache.load()
            self.assertLess(new_load, load)
            load = new_load
            LOGGER.info("New load: {}", load)
            # sleep(1)

    def test_scale_down_balance(self):
        self.saved_keys.clear()

        self.test_scale_up_balance()

        LOGGER.info("Saved keys: {}", self.saved_keys)

        saved_keys = list(self.saved_keys)
        random.shuffle(saved_keys)

        no_of_nodes = self.cache.load_balancer.ring.no_of_active_nodes()
        while no_of_nodes > 1:
            LOGGER.info("Current nodes: {}", no_of_nodes)

            for i in range(min(25, len(saved_keys))):
                key = saved_keys.pop()
                self.cache.remove(key)

            self.run_balance()

            new_nodes = self.cache.load_balancer.ring.no_of_active_nodes()

            diff = new_nodes - no_of_nodes
            self.assertTrue(diff <= 1)

            no_of_nodes = new_nodes

            LOGGER.info("New nodes: {}", new_nodes)
            # sleep(1)

        pass
