import threading

from masternode.main.common.data_nodes.in_memory_datanode import InMemoryDataNode


class DataNodeFactory:
    __singleton_lock = threading.Lock()
    __singleton_instance = None

    # define the classmethod
    @classmethod
    def instance(cls) -> "DataNodeFactory":

        # check for the singleton instance
        if not cls.__singleton_instance:
            with cls.__singleton_lock:
                if not cls.__singleton_instance:
                    cls.__singleton_instance = cls()
        # return the singleton instance
        return cls.__singleton_instance

    def getInMemoryDataNode(self, name, capacity) -> InMemoryDataNode:
        return InMemoryDataNode(name, capacity)
