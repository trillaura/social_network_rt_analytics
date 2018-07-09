#!/usr/bin/env bash
kubectl delete -f storm-nimbus.yml

kubectl delete -f storm-nimbus-service.yml

kubectl delete -f storm-ui.yml

kubectl delete -f storm-ui-service.yml

kubectl delete -f storm-worker-controller.yml

kubectl delete -f storm-worker-service.yml