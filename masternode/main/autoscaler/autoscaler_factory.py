from masternode.main.autoscaler.autoscaler import Autoscaler
from masternode.main.autoscaler.in_mem_autoscaler import LocalAutoscaler
from masternode.main.autoscaler.cloud_autoscaler import CfAutoscaler
from masternode.main.autoscaler.k8s_autoscaler import K8sAutoscaler


def create_autoscaler(autoscaler_type, ch):
    if autoscaler_type == "LOCAL":
        return LocalAutoscaler(ch)
    elif autoscaler_type == "K8S":
        return K8sAutoscaler(ch)
    elif autoscaler_type == "CF":
        return CfAutoscaler()
    else:
        return Autoscaler()
