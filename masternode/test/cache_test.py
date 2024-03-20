import asyncio
import unittest

from masternode.main.cache import DistributedCache, CacheConfig
from loguru import logger as LOGGER


class DistributedCacheTest(unittest.TestCase):

    def setUp(self):
        config = CacheConfig()
        config.autoscaler_type = "LOCAL"
        self.cache = DistributedCache(config)
        self.cache.autoscaler.upscale()
        self.run_balance()

    def run_balance(self):
        LOGGER.info("Running Balance")
        asyncio.run(self.cache.load_balancer.balance())
        LOGGER.info("Balance completed ")

    def test_put(self):
        self.cache.put("apple", "pie")
        self.assertTrue(self.cache.has("apple"))
        if self.cache.has("apple"):
            value = self.cache.get("apple")
            self.assertEqual(value, "pie")
        pass

    def test_scale_up(self):
        for i in range(5):
            key = "apple-" + str(i)
            self.cache.put(key, i)
        self.run_balance()
        self.run_balance()
        status = self.cache.status()
        self.assertGreaterEqual(status["ring"].no_of_active_nodes(), 2)

    def test_scale_down(self):
        self.test_scale_up()
        for i in range(5):
            key = "apple-" + str(i)
            self.cache.remove(key)
        self.run_balance()
        self.run_balance()
        status = self.cache.status()
        self.assertLessEqual(status["ring"].no_of_active_nodes(), 1)
        pass

    def test_contains(self):
        pass

    # def test_get(self):
    #
    #     keys = []
    #
    #     while True:
    #
    #         for i in range(5):
    #             key = random_str(3)
    #             self.cache.put(key, i)
    #             keys.append(key)
    #             sleep(1)
    #
    #         for i in range(5):
    #             random.shuffle(keys)
    #             key = keys[0]
    #             if self.cache.has(key):
    #                 self.cache.remove(key)
    #             sleep(1)
    #     pass
