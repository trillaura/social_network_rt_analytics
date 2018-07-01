#!/bin/bash
docker run --rm --network=kafka-network -it sickp/alpine-redis redis-cli -h redis-server
