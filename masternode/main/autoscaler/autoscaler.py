import requests
from requests.auth import HTTPBasicAuth

from datanodes.factory import DataNodeFactory



class Autoscaler(object):
    def __init__(self):
        pass

    def upscale(self):
        pass

    def downscale(self):
        pass


class LocalAutoscaler(Autoscaler):
    def __init__(self, ch):
        super().__init__()
        self.ch = ch

    def upscale(self):
        self.ch.addFreeInstance(DataNodeFactory.instance().createDataNode("data-node-server"))
        pass

    def downscale(self):
        pass

