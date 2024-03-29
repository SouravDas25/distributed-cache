import random

from loguru import logger as LOGGER

from concept.ring import HashRing
from masternode.main.common.hashing import random_str

if __name__ == '__main__':
    # cache with 128 bytes size
    # maximum having 5 node sets
    # with 2 replica per node set
    ring = HashRing(128, 5, 2)

    keys = []

    for i in range(20):
        # generate random string
        key = random_str(3)
        # generate random string of random length
        value = random_str(random.randint(1, 100))
        # save in ring
        ring.put(key, value)
        # store key from removal
        keys.append(key)
        LOGGER.info("Inserted Key : {} -> {}", key, value)
        # distribute the data across the nodes
        ring.balance()

    for key in keys:
        if ring.has(key):
            # remove key from the ring
            ring.remove(key)
            LOGGER.info("Removed Key : {}", key)
            # distribute the data & remove unused nodes
            ring.balance()
