from masternode.main.autoscaler.autoscaler import Autoscaler

from masternode.main.datanodes.in_memory_datanode import InMemoryDataNode


class LocalAutoscaler(Autoscaler):
    def __init__(self, cache):
        super().__init__()
        self.cache = cache
        self.count = 0

    def upscale(self):
        self.cache.ring.add_free_node(InMemoryDataNode("datanode-" + str(self.count), self.count))
        self.count += 1
        pass

    def downscale(self):
        pass
