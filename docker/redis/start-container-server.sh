#!/bin/bash
docker run --rm --network=kafka-network --name=redis-server sickp/alpine-redis
