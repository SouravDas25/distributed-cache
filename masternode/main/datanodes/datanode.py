import abc
from enum import Enum


class DataNode(abc.ABC):

    @abc.abstractmethod
    def instance_no(self) -> int:
        pass

    @abc.abstractmethod
    def name(self) -> str:
        pass

    @abc.abstractmethod
    def size(self, cache=True):
        pass

    @abc.abstractmethod
    def copy_keys(self, target_node: "DataNode", from_key: int, to_key: int):
        pass

    @abc.abstractmethod
    async def compact_keys(self):
        pass

    @abc.abstractmethod
    async def health_check(self) -> bool:
        pass

    @abc.abstractmethod
    def cached_metrics(self):
        pass

    @abc.abstractmethod
    async def metrics(self, cache=True):
        pass

    @abc.abstractmethod
    def put(self, key, value):
        pass

    @abc.abstractmethod
    def get(self, key):
        pass

    @abc.abstractmethod
    def has(self, key):
        pass

    @abc.abstractmethod
    async def calculate_mid_key(self):
        pass

    def remove(self, key):
        pass

    @abc.abstractmethod
    def update_node(self, new_node) -> None:
        pass

    def __str__(self):
        return f"{self.name()}:{self.instance_no()}"

    def __repr__(self):
        return f"{self.name()}:{self.instance_no()}"
