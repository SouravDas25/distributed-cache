import json
from time import sleep

import requests

URL = "http://localhost:8091"
def get_status():
    url = f"{URL}/status/"
    headers = {"Accept": "application/json"}

    # Make the GET request
    response = requests.get(url, headers=headers, verify=False)

    # Check the response status code
    if response.status_code == 200:
        # print("Response:", response.json())
        print(json.dumps(response.json(), indent=4))
    else:
        print("GET request failed with status code:", response.status_code, response.text)


def main():

    while True:

        get_status()
        sleep(1)
        pass
    pass


if __name__ == '__main__':
    main()