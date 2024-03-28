from time import sleep

import requests
import json


class Server:

    def __init__(self, url):
        self.url = url
        pass

    def size(self):
        pass

    def put(self, key, value):
        url = f"{self.url}/save/{key}"
        payload = {"value": value}
        headers = {"Content-Type": "application/json"}

        # Make the POST request
        response = requests.post(url, data=json.dumps(payload), headers=headers, verify=False)

        # Check the response status code
        if response.status_code == 200:
            print("POST request successful")
            print("Response:", response.text)
        else:
            print("POST request failed with status code:", response.status_code)

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
        response = requests.post(url, headers=headers, verify=False)

        # Check the response status code
        return response.status_code == 200
        pass


if __name__ == "__main__":
    import random
    import string

    URL = "http://127.0.0.1:57840"


    def strGen(N):
        return ''.join(random.sample(string.ascii_uppercase + string.digits, N))


    sc = Server(URL)

    # sc.balance()
    # print(sc.clusterSize(), sc.servePoints)
    # stream(list(sc.servers.values())) \
    #     .map(lambda s: list(s.cache.keys())) \
    #     .for_each(print)

    keys = []

    while True:

        action = input("Enter action(I/C/G/R): ")

        if action == "I":
            # keys.clear()
            for i in range(10):
                key = strGen(3)
                print("Inserting key : ", key)
                sc.put(key, i)
                keys.append(key)
            pass
        elif action == "C":
            for key in keys:
                print("has key : ", key, sc.has(key))

            pass
        elif action == "G":
            for key in keys:
                print("has key : ", key, sc.get(key))
            pass
        elif action == "R":
            for key in keys:
                print("has key : ", key, sc.remove(key))
            pass

            # sleep(1)

        # for i in range(500):
        #     random.shuffle(keys)
        #     if keys.__len__() <= 0:
        #         break
        #     key = keys[0]
        #     if sc.has(key):
        #         sc.get(key)
        #         sc.remove(key)
        #         if random.random() > 0.3:
        #             keys.pop(0)
        #             pass
        #     else:
        #         keys.pop(0)
        # sleep(1)

        # print(sc.clusterSize(), sc.servePoints)
        # sc.printClusterDistribution()
        # sleep(5)


