from datetime import datetime

import requests
from requests.auth import HTTPBasicAuth

from masternode.main.autoscaler.autoscaler import Autoscaler

from kubernetes import client, config

TIMEOUT_SEC = 10
from loguru import logger as LOGGER


class K8sAutoscaler(Autoscaler):
    def __init__(self, ch):
        super().__init__()
        config.load_incluster_config()
        self.ch = ch
        self.api = client.AppsV1Api()
        self.LAST_CALLED_AT = datetime.now()

    def upscale(self, instance_no: int):
        time_diff = datetime.now() - self.LAST_CALLED_AT
        if time_diff.seconds > TIMEOUT_SEC:
            self.LAST_CALLED_AT = datetime.now()
            data = self.api.read_namespaced_stateful_set_status("ihs-datanode", "default")

            LOGGER.info("k8s status : {}", data.status)

            body = {'spec': {'replicas': data.status.current_replicas + 1}}
            self.api.patch_namespaced_stateful_set_scale("ihs-datanode", "default", body)
            LOGGER.info("Up scaling: no of nodes: {}", data.status.current_replicas + 1)
        pass

    def downscale(self, instance_no: int):
        time_diff = datetime.now() - self.LAST_CALLED_AT
        if time_diff.seconds > TIMEOUT_SEC:
            self.LAST_CALLED_AT = datetime.now()

            data = self.api.read_namespaced_stateful_set_status("ihs-datanode", "default")
            print(data.status)
            if data.status.current_replicas > 1 and (data.status.current_replicas - 1) >= instance_no:
                body = {'spec': {'replicas': data.status.current_replicas - 1}}
                self.api.patch_namespaced_stateful_set_scale("ihs-datanode", "default", body)

        pass


if __name__ == '__main__':
    config.load_kube_config()

    autoscaler = K8sAutoscaler(None)
    autoscaler.upscale()
