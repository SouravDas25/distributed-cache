from ring import ConsistentHashRing


class CacheConfig:

    def __init__(self):
        self.config = ""
        self.cacheSize = 10
        self.scaleFactor = 2


class DistributedCache:

    def __init__(self):
        self.config = CacheConfig()
        self.serverCluster = ConsistentHashRing(self.config.cacheSize)

    def put(self, key, value):
        server = self.serverCluster.getServer(key)
        print(f"Server for key {key} : {server.name()}")
        server.put(key, value)

    def get(self, key):
        server = self.serverCluster.getServer(key)
        return server.get(key)

    def has(self, key):
        server = self.serverCluster.getServer(key)
        return server.has(key)

    def remove(self, key):
        server = self.serverCluster.getServer(key)
        return server.remove(key)


if __name__ == "__main__":
    dc = DistributedCache()
    dc.put("apple", "pie")
    print(dc.get("apple"))
    print(dc.has("apple"))
