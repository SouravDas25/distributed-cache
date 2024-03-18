



kubectl apply -f .\deployment.yaml

minikube image rm 3658ea2ab9739


minikube image ls --format table


docker context use default

minikube docker-env


kubectl rollout restart deployment ihs-master-deploy

 minikube service ihs-master-svc --url

kubectl get --raw /apis/custom.metrics.k8s.io/v2/namespace/default/pods/*/cpu | jq


helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

helm install prometheus prometheus-community/prometheus --namespace=prometheus-ns --create-namespace

minikube service prometheus-server --url -n=prometheus-ns



helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

helm install grafana grafana/grafana --namespace=grafana-ns --create-namespace

minikube service grafana --url -n=grafana-ns