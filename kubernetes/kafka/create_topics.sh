#!/usr/bin/env bash

topics=( comments-stream-input posts-stream-input friendships-stream-input)

for topic in "${topics[@]}"
do
  echo Creating topic "${topic}"
  kubectl run  --image=gcr.io/google\_containers/kubernetes-kafka:1.0-10.2.1 "${topic}" --restart=Never  -- kafka-topics.sh --create \
   --topic "${topic}" --zookeeper zk-cs.default.svc.cluster.local:2181 --partitions 1 --replication-factor 1
done