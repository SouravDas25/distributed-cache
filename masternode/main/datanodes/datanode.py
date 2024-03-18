import abc


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
    def move_keys(self, targetServer, fromHash, toHash):
        pass

    @abc.abstractmethod
    def compact_keys(self, capacity):
        pass

    @abc.abstractmethod
    def health_check(self) -> bool:
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
    def calculate_mid_key(self):
        pass

    def remove(self, key):
        pass

    @abc.abstractmethod
    def update_node(self, new_node) -> None:
        pass
