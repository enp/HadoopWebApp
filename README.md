# How to run Hadoop Web Application

```
gradle jar
docker build -t evnp/hadoop .
docker login
docker push evnp/hadoop
kubectl run -it --rm hadoop-evnp --image=evnp/hadoop
kubectl exec hadoop-evnp -- curl -s localhost:8080
```
