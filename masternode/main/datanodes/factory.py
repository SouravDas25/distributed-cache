import threading

from masternode.main.datanodes.datanode import DataNode
from masternode.main.datanodes.in_memory_datanode import InMemoryDataNode
from .network_datanode import NetworkDataNode


def create_datanode(type: str):
    return InMemoryDataNode("datanode")
    pass
