from masternode.main.autoscaler.autoscaler import Autoscaler

from masternode.main.datanodes.in_memory_datanode import InMemoryDataNode
from loguru import logger as LOGGER


class LocalAutoscaler(Autoscaler):
    def __init__(self, cache):
        super().__init__()
        self.cache = cache
        self.count = 0

    def upscale(self, instance_no: int):
        if instance_no > self.count:
            node = InMemoryDataNode("datanode-" + str(self.count), self.count)
            self.cache.register_new_node(node)
            self.count += 1
            LOGGER.info("Up scaling: no of nodes: {}", self.count)
        else:
            LOGGER.info("No Up scaling as instance_no already satisfied: {}, count : {}", instance_no, self.count)
        pass

    def downscale(self, instance_no: int):
        pass
