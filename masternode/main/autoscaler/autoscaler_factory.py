from .autoscaler import LocalAutoscaler
from .cloud_autoscaler import CfAutoscaler


def createAutoScaler(autoscalerType, ch):
    if autoscalerType == "LOCAL":
        return LocalAutoscaler(ch)
    else:
        return CfAutoscaler()
