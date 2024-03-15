import threading

from common.interfaces.datanode import DataNode
from .in_memory_datanode import InMemoryDataNode
from .network_datanode import NetworkDataNode


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

    def getInMemoryDataNode(self, name) -> InMemoryDataNode:
        return InMemoryDataNode(name)

    def createDataNode(self, name: str) -> DataNode:
        return InMemoryDataNode(name)
