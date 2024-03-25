import os
import psutil
import requests

from hashing import stableHash


class LRUCache:

    def __init__(self):
        self.cache = {}
        self.keysToBeRemoved = set()

    def compact(self):
        for key in self.keysToBeRemoved:
            if key in self.cache:
                self.cache.pop(key)
        self.keysToBeRemoved.clear()

    def size(self):
        return self.cache.__len__()

    def put(self, key, value):
        self.cache[key] = value

    def get(self, key):
        return self.cache[key]

    def has(self, key: str):
        return key in self.cache

    def remove(self, key: str):
        return self.cache.pop(key)

    def calculateMidKey(self):
        return sum(map(lambda x: stableHash(x), self.cache.keys())) // self.cache.__len__()

    def copySingleKey(self, targetServer, key):
        url = f"{targetServer['url']}/save/{key}"
        if key not in self.cache:
            return
        value = self.cache[key]
        headers = {
            "Content-Type": "application/json",
            # "X-Cf-App-Instance": str(self.instance_id)
        }

        print("moving key : ", key, value, url)
        # Make the POST request
        try:
            response = requests.post(url, json=value, headers=headers, verify=False)

            print("POST request successful")
            print("Response:", response.text)
        except Exception as e:
            print("POST request failed with status code:", e)
        pass

    def copy_keys(self, targetServer, fromKey, toKey):

        for key in list(self.cache.keys()):
            keyHash = stableHash(key)
            if fromKey <= keyHash:
                self.copySingleKey(targetServer, key)
                # self.cache.pop(key)
                self.keysToBeRemoved.add(key)
        pass
