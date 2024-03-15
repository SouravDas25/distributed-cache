



kubectl apply -f .\deployment.yaml

minikube image rm 3658ea2ab9739


minikube image ls --format table


docker context use default

minikube docker-env


 minikube service ihs-master-svc --url