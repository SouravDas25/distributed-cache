import requests
from requests.auth import HTTPBasicAuth

from masternode.main.autoscaler.autoscaler import Autoscaler

credentials = {
    "username": "sbss_dljpwkw6briclznscyg256pa0za7rc5a/93llnhr3r68z55g96adulgam+ofkaaldsq=",
    "password": "aa_i1B9qdIrsz1istrknoS9CYpyC20=",
    "url": "https://autoscaler-metrics.cf.us10-001.hana.ondemand.com",
}


class CfAutoscaler(Autoscaler):

    def __init__(self):
        super().__init__()
        self.url = f"{credentials["url"]}/v1/apps/cb12c3c6-be5c-4b33-88ea-fbc148499059/metrics.txt"

    def upscale(self):
        body = {
            "instance_index": 0,
            "metrics.txt": [
                {
                    "name": "scallingmetric",
                    "value": 100,
                    "unit": "%"
                }
            ]
        }

        response = requests.post(self.url, json=body, verify=False,
                                 auth=HTTPBasicAuth(credentials["username"], credentials["password"]))
        # Check the response
        if response.status_code == 200:
            print("Request successful!")
            print(response.json())  # Process the response data as needed
        else:
            print(f"Error: {response.status_code}")
        pass

    def downscale(self, instance_no: int):
        body = {
            "instance_index": 0,
            "metrics.txt": [
                {
                    "name": "scallingmetric",
                    "value": 0,
                    "unit": "%"
                }
            ]
        }

        response = requests.post(self.url, json=body, verify=False,
                                 auth=HTTPBasicAuth(credentials["username"], credentials["password"]))
        # Check the response
        if response.status_code == 200:
            print("Request successful!")
            print(response.json())  # Process the response data as needed
        else:
            print(f"Error: {response.status_code}")
        pass
