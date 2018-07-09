#!/usr/bin/env bash
kubectl create -f storm-nimbus.yml

sleep 2m

kubectl create -f storm-nimbus-service.yml

kubectl create -f storm-ui.yml

sleep 30

kubectl create -f storm-ui-service.yml

kubectl create -f storm-worker-controller.yml

sleep 30

kubectl create -f storm-worker-service.yml