#!/bin/bash
docker run -d -p 6379:6379 --rm --network=kafkastreams_default --name=redis-server sickp/alpine-redis
