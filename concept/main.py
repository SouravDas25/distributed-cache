import random

from loguru import logger as LOGGER

from concept.ring import HashRing
from masternode.main.common.hashing import random_str

if __name__ == '__main__':
    ring = HashRing(128, 5, 2)

    keys = []

    for i in range(20):
        key = random_str(3)
        value = random_str(random.randint(1, 100))
        ring.put(key, value)
        keys.append(key)
        LOGGER.info("Inserted Key : {} -> {}", key, value)
        ring.balance()

    for key in keys:
        if ring.has(key):
            ring.remove(key)
            LOGGER.info("Removed Key : {}", key)
            ring.balance()
