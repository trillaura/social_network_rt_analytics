#!/bin/bash
docker run -it --rm --network=kafkastreams_default sickp/alpine-redis redis-cli -h redis-server
