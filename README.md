# Social Network Real-Time Analytics

Table of Contents
=================

* [Application usage](#application-usage)
* [Local Deployment](#local-deployment)
   * [Data Ingestion: Apache Kafka](#data-ingestion-apache-kafka)
   * [Data Storage: Redis](#redis)
   * [Data Processing: Apache Flink](#data-processing-apache-flink)
   * [Data Processing: Apache Storm](#data-processing-apache-storm)
   * [Data Processing: Apache Kafka Streams](#data-processing-apache-kafka-streams)

#### Application usage

cd docker/kafka-streams

bash launchEnvironment.sh

cd docker/redis

bash start-container-server.sh

execute ProducerLauncher Main

execute Kafka Query1 Main

execute RedisConsumerLauncher Main

(execute RedisManager Main) 
