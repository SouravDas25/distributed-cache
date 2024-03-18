import asyncio
import json

import requests
from masternode.main.common.utils.hashing import stableHash
from masternode.main.datanodes.datanode import DataNode
import aiohttp


class NetworkDataNode(DataNode):

    def __init__(self, server_name: str, instance_id: int, app_url: str, app_id: str):
        self.server_name = server_name
        self.instance_id = int(instance_id)
        self.app_id = app_id
        self.app_url = app_url
        self.url = f"{app_url}"
        self.cached_metric = {}

    def update_node(self, new_node: DataNode):
        self.app_url = new_node.app_url
        self.app_id = new_node.app_id
        self.url = new_node.url

    def instance_no(self) -> int:
        return self.instance_id

    def size(self, cache=True):
        return self.cached_metric["size"] if "size" in self.cached_metric else 0
        pass

    def get_url(self):
        return self.url

    def name(self):
        return self.server_name

    async def health_check(self):
        url = f"{self.url}/health/"
        headers = {"Accept": "application/json"}
        try:
            # Make the GET request
            response = await asyncio.to_thread(requests.get, url=url, headers=headers, verify=False)

            # Check the response status code
            return response.status_code == 200
        except:
            return False

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

    def calculate_mid_key(self):
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

        print("getting metrics ", self.server_name)
        # Make the POST request
        try:
            response = requests.get(url, headers=headers, verify=False)

            self.cached_metric = response.json()
            print("POST request successful")
            return self.cached_metric
        except Exception as e:
            print("POST request failed with status code:", e)

    async def move_keys(self, targetServer, fromKey, toKey):

        url = f"{self.url}/copy-keys/"
        payload = {
            "targetServer": {
                "url": targetServer.get_url(),
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

    async def compact_keys(self):
        url = f"{self.url}/compact-keys/"
        headers = {"Accept": "application/json"}

        # Make the GET request
        response = await asyncio.to_thread(requests.get, url=url, headers=headers, verify=False)

        # Check the response status code
        return response.json()
