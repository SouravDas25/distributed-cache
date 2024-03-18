import asyncio
import string
import unittest
import random
from time import sleep

from masternode.main.cache import DistributedCache


class DistributedCacheTest(unittest.TestCase):
    def setUp(self):
        self.cache = DistributedCache()
        self.cache.autoscaler.upscale()
        asyncio.run(self.cache.ring.balance())

    def test_put(self):
        self.cache.put("apple", "pie")
        value = self.cache.get("apple")
        self.assertEqual(value, "pie")
        pass

    def test_get(self):

        def strGen(N):
            return ''.join(random.sample(string.ascii_uppercase + string.digits, N))

        keys = []

        while True:

            for i in range(5):
                key = strGen(3)
                self.cache.put(key, i)
                keys.append(key)
                sleep(1)

            for i in range(5):
                random.shuffle(keys)
                key = keys[0]
                if self.cache.has(key):
                    self.cache.remove(key)
                sleep(1)
        pass
