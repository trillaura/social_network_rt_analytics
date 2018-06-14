#!/usr/bin/env bash

CONFIG_FILE=/kafka/config/server.properties
DS_FILE=/kafka/config/ds.properties

HOST_IP=$(awk 'END{print $1}' /etc/hosts)
CONFIG_LINE=listeners="PLAINTEXT://$HOST_IP:9092"
BROKER_ID_LINE="broker.id=$BROKER_ID"

cp $CONFIG_FILE $DS_FILE
echo $CONFIG_LINE >> $DS_FILE
echo $BROKER_ID_LINE >> $DS_FILE