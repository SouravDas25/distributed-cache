import abc


class DataNode(abc.ABC):

    @abc.abstractmethod
    def moveKeys(self, targetServer, low: int, high: int, size: int):
        pass

    @abc.abstractmethod
    def compact(self):
        pass

    @abc.abstractmethod
    def capacity(self):
        pass

    @abc.abstractmethod
    def size(self):
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
