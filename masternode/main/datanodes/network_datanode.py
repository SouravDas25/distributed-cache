import asyncio
import json

import requests
from common.utils.hashing import stableHash
from common.interfaces.datanode import DataNode
import aiohttp


class NetworkDataNode(DataNode):

    def __init__(self, serverName: str, instance_id: str, appUrl: str, appId: str):
        self.serverName = serverName
        self.instance_id = instance_id
        self.appId = appId
        self.appUrl = appUrl
        self.url = f"{appUrl}"
        self.cachedMetric = {}

    def getUrl(self):
        return self.url

    def name(self):
        return self.serverName

    def put(self, key, value):
        url = f"{self.url}/save/{key}"
        payload = {"value": value}
        headers = {
            "Content-Type": "application/json",
            # "X-Cf-App-Instance": str(self.instance_id)
        }

        print("saving key : ", key, value, url)
        # Make the POST request
        try:
            response = requests.post(url, json=value, headers=headers, verify=False)

            # print("POST request successful")
            print("Response:", response.json())
        except Exception as e:
            print("POST request failed with status code:", e)

    def get(self, key):
        url = f"{self.url}/retrieve/{key}"
        headers = {"Accept": "application/json"}

        # Make the GET request
        response = requests.get(url, headers=headers, verify=False)

        # Check the response status code
        if response.status_code == 200:
            print("GET request successful")
            # print("Response:", response.json())
            return response.json()
        else:
            print("GET request failed with status code:", response.status_code)

    def has(self, key: str):
        url = f"{self.url}/contains/{key}"
        headers = {"Accept": "application/json"}

        # Make the GET request
        response = requests.get(url, headers=headers, verify=False)

        # Check the response status code
        return response.status_code == 200

    def remove(self, key):
        url = f"{self.url}/remove/{key}"
        headers = {"Accept": "application/json"}

        # Make the GET request
        try:
            response = requests.get(url, headers=headers, verify=False)
        except Exception as e:
            print("GET request failed with status code:", e)

    def calculateMidKey(self):
        url = f"{self.url}/calculate-mid-key/"
        headers = {"Accept": "application/json"}

        # Make the GET request
        response = requests.get(url, headers=headers, verify=False)

        # Check the response status code
        return response.json()['midKey']

    def copyKeys(self, targetServer, fromKey, toKey):
        for key in list(self.cache.keys()):
            keyHash = stableHash(key)
            if fromKey <= keyHash:
                targetServer.put(key, self.cache[key])
                # self.cache.pop(key)

    async def metrics(self):
        url = f"{self.url}/metrics/"
        headers = {
            # "Content-Type": "application/json",
            # "X-Cf-App-Instance": str(self.instance_id)
        }

        print("getting metrics ", self.serverName)
        # Make the POST request
        try:
            response = requests.get(url, headers=headers, verify=False)

            self.cachedMetric = response.json()
            print("POST request successful")
            return self.cachedMetric
        except Exception as e:
            print("POST request failed with status code:", e)

    async def moveKeys(self, targetServer, fromKey, toKey):

        url = f"{self.url}/copy-keys/"
        payload = {
            "targetServer": {
                "url": targetServer.getUrl(),
                "name": targetServer.name()
            },
            "fromKey": fromKey,
            "toKey": toKey
        }
        headers = {
            "Content-Type": "application/json",
            # "X-Cf-App-Instance": str(self.instance_id)
        }

        print("move keys : ", payload, url)
        # Make the POST request
        try:
            response = await asyncio.to_thread(requests.post, url=url, json=payload, headers=headers, verify=False)
            data = response.json()
            print("copy-keys response : ", data)
            return data
        except Exception as e:
            print("POST request failed with status code:", e)
        pass

    async def compactKeys(self):
        url = f"{self.url}/compact-keys/"
        headers = {"Accept": "application/json"}

        # Make the GET request
        response = await asyncio.to_thread(requests.get, url=url, headers=headers, verify=False)

        # Check the response status code
        return response.json()
