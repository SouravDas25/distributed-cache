import abc


class DataNode(abc.ABC):

    @abc.abstractmethod
    def moveKeys(self, targetServer, fromHash, toHash):
        pass

    @abc.abstractmethod
    def compactKeys(self, capacity):
        pass

    @abc.abstractmethod
    def metrics(self):
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
    def calculateMidKey(self):
        pass

    @abc.abstractmethod
    def copyKeys(self, targetServer, fromKey, toKey):
        pass

    def remove(self, key):
        pass
